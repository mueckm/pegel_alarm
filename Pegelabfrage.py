#!/usr/bin/env python3
# Multi_Pegel.py (multi-station + 3/4 Warnstufen pro Messstelle, v2.3)
#
# - Abfrage aller Stationen aus der config.ini im Turnus
# - Aktuellster Messwert pro Station via HLNUG WISKI-Web: layers/10/index.json
# - 3 oder 4 Thresholds pro Station (Warnstufe 1..N) in cm
# - Ausgabe pro Station: Pegel, Warnstufe, Zeit (HH:MM TT:MM:JJJJ)
# - Optional: E-Mail bei Warnstufe >=1 (mit Rate-Limit) und/oder bei Stufenanstieg

import argparse
import configparser
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
import smtplib

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None  # type: ignore


HLNUG_LASTVALUES_INDEX = "https://www.hlnug.de/static/pegel/wiskiweb3/data/internet/layers/10/index.json"


def get_app_dir() -> Path:
    """EXE-tauglich: Verzeichnis der EXE (PyInstaller) oder des Scripts."""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


@dataclass(frozen=True)
class StationConfig:
    name: str
    station_id_public: str
    station_no: str
    parameter: str  # z.B. W
    thresholds_cm: Tuple[float, ...]  # Warnstufe 1..N (aufsteigend), z.B. 3 oder 4 Stufen
    level_names: Tuple[str, ...]               # Namen für Warnstufe 1..N


@dataclass(frozen=True)
class Settings:
    stations: List[StationConfig]

    db_path: Path

    mode: str  # once | daemon
    poll_interval_seconds: int
    min_alert_interval_minutes: int
    request_timeout_seconds: int

    email_enabled: bool
    mail_to: str
    mail_from: str

    smtp_host: str
    smtp_port: int
    smtp_user: str
    smtp_password: str
    smtp_use_ssl: bool
    smtp_use_starttls: bool

    # Mail-Verhalten
    alert_on_start: bool           # beim ersten Lauf (kein last_level) mailen, wenn Warnstufe>=1
    alert_on_level_increase: bool  # mailen bei Stufenanstieg

    debug: bool


def _read_bool(cp: configparser.ConfigParser, section: str, key: str, default: bool) -> bool:
    if not cp.has_section(section) or not cp.has_option(section, key):
        return default
    return cp.getboolean(section, key, fallback=default)


def _parse_csv_floats(s: str) -> List[float]:
    vals: List[float] = []
    for part in (s or "").split(","):
        p = part.strip()
        if not p:
            continue
        p = p.replace(",", ".")
        vals.append(float(p))
    return vals


def _parse_thresholds_for_section(
    cp: configparser.ConfigParser,
    section: str,
    fallback_thresholds: Tuple[float, ...],
) -> Tuple[float, ...]:
    """
    Unterstützte Keys (pro Station oder global):
      - thresholds_cm = a,b,c      (3 Stufen)
      - thresholds_cm = a,b,c,d    (4 Stufen)
      - threshold1_cm / threshold2_cm / threshold3_cm [/ threshold4_cm]
      - legacy: threshold_cm  -> eine Stufe (Warnstufe 1), wird als (x,) interpretiert

    Rückgabe: Tuple[float] mit 3 oder 4 (oder 1 bei legacy) Thresholds, strikt aufsteigend.
    """
    if cp.has_option(section, "thresholds_cm"):
        vals = _parse_csv_floats(cp.get(section, "thresholds_cm", fallback=""))
        if len(vals) not in (3, 4):
            raise ValueError(
                f"[{section}] thresholds_cm muss 3 oder 4 Werte haben (z.B. 150,180,200 oder 150,180,200,230)"
            )
        vals = sorted(vals)
        if any(v <= 0 for v in vals):
            raise ValueError(f"[{section}] thresholds_cm: alle Werte müssen > 0 sein")
        for i in range(1, len(vals)):
            if not (vals[i - 1] < vals[i]):
                raise ValueError(f"[{section}] thresholds_cm muss strikt aufsteigend sein (z.B. 150<180<200<230)")
        return tuple(vals)

    # Einzelwerte: 3 oder 4 Werte > 0
    has_any = any(cp.has_option(section, k) for k in ("threshold1_cm", "threshold2_cm", "threshold3_cm", "threshold4_cm"))
    if has_any:
        t1 = cp.getfloat(section, "threshold1_cm", fallback=0.0)
        t2 = cp.getfloat(section, "threshold2_cm", fallback=0.0)
        t3 = cp.getfloat(section, "threshold3_cm", fallback=0.0)
        t4 = cp.getfloat(section, "threshold4_cm", fallback=0.0)

        vals = [t for t in (t1, t2, t3, t4) if t and t > 0]
        if len(vals) not in (3, 4):
            raise ValueError(f"[{section}] threshold1_cm..threshold4_cm: es müssen 3 oder 4 Werte > 0 gesetzt sein")
        vals = sorted(vals)
        for i in range(1, len(vals)):
            if not (vals[i - 1] < vals[i]):
                raise ValueError(f"[{section}] Thresholds müssen strikt aufsteigend sein")
        return tuple(vals)

    # legacy single threshold
    if cp.has_option(section, "threshold_cm"):
        x = cp.getfloat(section, "threshold_cm", fallback=0.0)
        if x <= 0:
            raise ValueError(f"[{section}] threshold_cm muss > 0 sein")
        return (x,)

    return fallback_thresholds

def _parse_level_names_for_section(
    cp: configparser.ConfigParser,
    section: str,
    fallback: Tuple[str, ...],
    n_levels: int,
) -> Tuple[str, ...]:
    """
    Optional: level_names = Name1,Name2,Name3[,Name4]
    Muss zur Anzahl der Thresholds passen.
    """
    if cp.has_option(section, "level_names"):
        parts = [p.strip() for p in cp.get(section, "level_names", fallback="").split(",") if p.strip()]
        if len(parts) != n_levels:
            raise ValueError(f"[{section}] level_names muss genau {n_levels} Werte haben (passend zu thresholds)")
        return tuple(parts)

    if len(fallback) == n_levels:
        return fallback

    return tuple(f"Warnstufe {i}" for i in range(1, n_levels + 1))

def load_settings(config_path: Path) -> Settings:
    cp = configparser.ConfigParser()
    if not config_path.exists():
        raise FileNotFoundError(f"Config-Datei nicht gefunden: {config_path}")

    cp.read(config_path, encoding="utf-8")

    
    # Global thresholds
    fallback_thresholds: Tuple[float, ...]
    if cp.has_section("threshold") and (
        cp.has_option("threshold", "thresholds_cm")
        or any(cp.has_option("threshold", k) for k in ("threshold1_cm", "threshold2_cm", "threshold3_cm", "threshold4_cm"))
        or cp.has_option("threshold", "threshold_cm")
    ):
        fallback_thresholds = _parse_thresholds_for_section(cp, "threshold", (0.0, 0.0, 0.0))
    else:
        # legacy global value_cm (eine Stufe)
        global_value = cp.getfloat("threshold", "value_cm", fallback=0.0)
        if global_value > 0:
            fallback_thresholds = (global_value,)
        else:
            # hard-fail if nothing configured
            fallback_thresholds = (0.0, 0.0, 0.0)

    if min(fallback_thresholds) <= 0:
        raise ValueError(
            "Keine gültigen Thresholds gefunden. Setze entweder [threshold].thresholds_cm=... (3 oder 4 Werte) "
            "oder pro Station thresholds_cm=..."
        )
    
    # Global level names (optional)
    global_level_names: Tuple[str, ...] = tuple(f"Warnstufe {i}" for i in range(1, len(fallback_thresholds) + 1))
    if cp.has_section("threshold"):
        global_level_names = _parse_level_names_for_section(
            cp, "threshold", fallback=global_level_names, n_levels=len(fallback_thresholds)
        )
    # Stationen: bevorzugt Sections [station:<Name>]
    stations: List[StationConfig] = []
    station_sections = [s for s in cp.sections() if s.lower().startswith("station:")]

    if station_sections:
        for sec in station_sections:
            sec_name = sec.split(":", 1)[1].strip() or sec
            name = cp.get(sec, "name", fallback=sec_name).strip() or sec_name
            station_id_public = cp.get(sec, "station_id_public", fallback=cp.get(sec, "station_id", fallback="")).strip()
            station_no = cp.get(sec, "station_no", fallback="").strip()
            parameter = cp.get(sec, "parameter", fallback="W").strip()

            if not station_no:
                raise ValueError(f"{sec}: station_no fehlt")

            thresholds = _parse_thresholds_for_section(cp, sec, fallback_thresholds)
            for i in range(1, len(thresholds)):
                if not (thresholds[i-1] < thresholds[i]):
                    raise ValueError(f"{sec}: thresholds müssen strikt aufsteigend sein")
            if min(thresholds) <= 0:
                raise ValueError(f"{sec}: thresholds müssen > 0 sein")

            level_names = _parse_level_names_for_section(cp, sec, fallback=global_level_names, n_levels=len(thresholds))

            stations.append(
                StationConfig(
                    name=name,
                    station_id_public=station_id_public,
                    station_no=station_no,
                    parameter=parameter,
                    thresholds_cm=thresholds,
                    level_names=level_names,
                )
            )
    else:
        # Backward compatible: single [station]
        if not cp.has_section("station"):
            raise ValueError("Keine Stationen gefunden. Nutze [station:<Name>] oder [station].")

        name = cp.get("station", "name", fallback="(unknown)").strip()
        station_id_public = cp.get("station", "station_id_public", fallback="").strip()
        station_no = cp.get("station", "station_no", fallback="").strip()
        parameter = cp.get("station", "parameter", fallback="W").strip()

        if not station_no:
            raise ValueError("station.station_no fehlt in config.ini")

        thresholds = _parse_thresholds_for_section(cp, "station", fallback_thresholds)
        level_names = _parse_level_names_for_section(cp, "station", fallback=global_level_names, n_levels=len(thresholds))

        stations.append(
            StationConfig(
                name=name,
                station_id_public=station_id_public,
                station_no=station_no,
                parameter=parameter,
                thresholds_cm=thresholds,
                level_names=level_names,
            )
        )

    # Storage
    db_path = Path(cp.get("storage", "db_path", fallback="pegel.db").strip() or "pegel.db")

    # Runtime
    mode = cp.get("runtime", "mode", fallback="once").strip().lower()

    poll_interval_minutes = cp.getint("runtime", "poll_interval_minutes", fallback=15)
    poll_interval_seconds_raw = cp.getint("runtime", "poll_interval_seconds", fallback=0)
    poll_interval_seconds = int(poll_interval_seconds_raw) if poll_interval_seconds_raw and poll_interval_seconds_raw > 0 else int(poll_interval_minutes) * 60

    min_alert_interval_minutes = cp.getint("runtime", "min_alert_interval_minutes", fallback=180)
    request_timeout_seconds = cp.getint("runtime", "request_timeout_seconds", fallback=20)

    # Mail behavior (optional)
    alert_on_start = _read_bool(cp, "runtime", "alert_on_start", True)
    alert_on_level_increase = _read_bool(cp, "runtime", "alert_on_level_increase", True)

    # Email + SMTP
    email_enabled = _read_bool(cp, "email", "enabled", True)
    mail_to = cp.get("email", "to", fallback="").strip()
    mail_from = cp.get("email", "from", fallback="").strip()

    smtp_host = cp.get("smtp", "host", fallback="").strip()
    smtp_port = cp.getint("smtp", "port", fallback=465)
    smtp_user = cp.get("smtp", "user", fallback="").strip()
    smtp_password = cp.get("smtp", "password", fallback="").strip()
    smtp_use_ssl = _read_bool(cp, "smtp", "use_ssl", True)
    smtp_use_starttls = _read_bool(cp, "smtp", "use_starttls", False)

    debug = _read_bool(cp, "debug", "enabled", False)

    # DB-Pfad relativ zur App/EXE
    app_dir = get_app_dir()
    if not db_path.is_absolute():
        db_path = (app_dir / db_path).resolve()

    # Validierung Runtime
    if mode not in ("once", "daemon"):
        raise ValueError("runtime.mode muss 'once' oder 'daemon' sein")
    if poll_interval_seconds < 10:
        raise ValueError("Intervall zu klein (mindestens 10 Sekunden).")

    return Settings(
        stations=stations,
        db_path=db_path,
        mode=mode,
        poll_interval_seconds=poll_interval_seconds,
        min_alert_interval_minutes=int(min_alert_interval_minutes),
        request_timeout_seconds=int(request_timeout_seconds),
        email_enabled=bool(email_enabled),
        mail_to=mail_to,
        mail_from=mail_from,
        smtp_host=smtp_host,
        smtp_port=int(smtp_port),
        smtp_user=smtp_user,
        smtp_password=smtp_password,
        smtp_use_ssl=bool(smtp_use_ssl),
        smtp_use_starttls=bool(smtp_use_starttls),
        alert_on_start=bool(alert_on_start),
        alert_on_level_increase=bool(alert_on_level_increase),
        debug=bool(debug),
    )


def _table_columns(con: sqlite3.Connection, table: str) -> List[str]:
    cur = con.execute(f"PRAGMA table_info({table})")
    return [row[1] for row in cur.fetchall()]


def init_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as con:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS measurements (
                station_no TEXT NOT NULL,
                station_id_public TEXT,
                station_name TEXT,
                parameter  TEXT NOT NULL,
                ts         TEXT NOT NULL,
                value      REAL NOT NULL,
                level      INTEGER,
                source     TEXT,
                unit       TEXT,
                PRIMARY KEY (station_no, parameter, ts)
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS state (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )

        cols = _table_columns(con, "measurements")
        migrations = [
            ("station_id_public", "ALTER TABLE measurements ADD COLUMN station_id_public TEXT"),
            ("station_name", "ALTER TABLE measurements ADD COLUMN station_name TEXT"),
            ("level", "ALTER TABLE measurements ADD COLUMN level INTEGER"),
            ("source", "ALTER TABLE measurements ADD COLUMN source TEXT"),
            ("unit", "ALTER TABLE measurements ADD COLUMN unit TEXT"),
        ]
        for col, ddl in migrations:
            if col not in cols:
                con.execute(ddl)

        con.commit()


def db_get_state(con: sqlite3.Connection, key: str) -> Optional[str]:
    cur = con.execute("SELECT value FROM state WHERE key = ?", (key,))
    row = cur.fetchone()
    return row[0] if row else None


def db_set_state(con: sqlite3.Connection, key: str, value: str) -> None:
    con.execute(
        """
        INSERT INTO state(key, value) VALUES(?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (key, value),
    )


def _debug_print(settings: Settings, msg: str) -> None:
    if settings.debug:
        print(msg)


def _to_dt(ts: Any) -> Optional[datetime]:
    if ts is None:
        return None
    if isinstance(ts, (int, float)):
        v = float(ts)
        if v > 1e12:
            return datetime.fromtimestamp(v / 1000.0, tz=timezone.utc)
        if v > 1e9:
            return datetime.fromtimestamp(v, tz=timezone.utc)
        return None
    s = str(ts).strip()
    if not s:
        return None
    s = s.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


def _try_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if not s:
        return None
    s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None


def _format_local(dt: datetime) -> str:
    """Format: HH:MM TT.MM.JJJJ"""
    fmt = "%H:%M %d.%m.%Y"
    if ZoneInfo is None:
        return dt.strftime(fmt)
    try:
        berlin = ZoneInfo("Europe/Berlin")
        return dt.astimezone(berlin).strftime(fmt)
    except Exception:
        return dt.strftime(fmt)


def _compute_level(value: float, thresholds: Tuple[float, ...]) -> int:
    """Liefert Warnstufe 0..N, wobei N = Anzahl Thresholds."""
    level = 0
    for t in thresholds:
        if value >= t:
            level += 1
        else:
            break
    return level

def fetch_index(settings: Settings) -> List[dict]:
    """
    Lädt den aktuellen Index (letzte Messwerte) einmal pro Zyklus.
    Quelle: HLNUG WISKI-Web layers/10/index.json
    Rückgabe: Liste von Dicts.
    """
    session = requests.Session()
    session.headers.update({"User-Agent": "pegel-alarm/2.3"})
    r = session.get(HLNUG_LASTVALUES_INDEX, timeout=settings.request_timeout_seconds)
    _debug_print(settings, f"[DEBUG] GET {HLNUG_LASTVALUES_INDEX} -> {r.status_code}")
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        raise RuntimeError("index.json hat unerwartete Struktur (kein Array).")
    _debug_print(settings, f"[DEBUG] index entries: {len(data)}")
    return data

def build_index_map(arr: List[dict]) -> Dict[Tuple[str, str], dict]:
    """
    Map: (station_no, parameter) -> item.
    """
    m: Dict[Tuple[str, str], dict] = {}
    for item in arr:
        if not isinstance(item, dict):
            continue
        station_no = str(item.get("station_no", "")).strip()
        param = str(item.get("stationparameter_name", "")).strip()
        if station_no and param:
            m[(station_no, param)] = item
    return m


def latest_for_station(index_map: Dict[Tuple[str, str], dict], station: StationConfig) -> Tuple[str, float, str, str]:
    """Rückgabe: (timestamp_iso, value, source, unit)"""
    key = (str(station.station_no).strip(), station.parameter.strip())
    item = index_map.get(key)

    # fallback über station_id_public, falls station_no/param nicht matched
    if not item and station.station_id_public:
        wanted_id = str(station.station_id_public).strip()
        for (_, param), it in index_map.items():
            if param != station.parameter:
                continue
            if str(it.get("station_id", "")).strip() == wanted_id:
                item = it
                break

    if not item:
        raise RuntimeError(
            f"Station nicht im index.json gefunden: {station.name} (no={station.station_no}, param={station.parameter})"
        )

    ts = item.get("timestamp")
    val = item.get("ts_value")
    unit = str(item.get("ts_unitsymbol", "") or "").strip()

    dt = _to_dt(ts)
    fv = _try_float(val)

    if dt is None or fv is None:
        raise RuntimeError(f"timestamp/value nicht parsebar für {station.name}: timestamp={ts!r}, ts_value={val!r}")

    return dt.isoformat(), float(fv), "layers:10:index", unit


def _email_config_ok(settings: Settings) -> bool:
    if not settings.email_enabled:
        return False
    required = [settings.mail_to, settings.mail_from, settings.smtp_host, settings.smtp_user, settings.smtp_password]
    return all(x.strip() for x in required)


def send_email(settings: Settings, subject: str, body: str) -> None:
    msg = EmailMessage()
    msg["From"] = settings.mail_from
    msg["To"] = settings.mail_to
    msg["Subject"] = subject
    msg.set_content(body)

    if settings.smtp_use_ssl:
        with smtplib.SMTP_SSL(settings.smtp_host, settings.smtp_port, timeout=20) as s:
            s.login(settings.smtp_user, settings.smtp_password)
            s.send_message(msg)
        return

    with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=20) as s:
        s.ehlo()
        if settings.smtp_use_starttls:
            s.starttls()
            s.ehlo()
        s.login(settings.smtp_user, settings.smtp_password)
        s.send_message(msg)


def _parse_int_or_none(s: Optional[str]) -> Optional[int]:
    if s is None:
        return None
    s2 = str(s).strip()
    if not s2:
        return None
    try:
        return int(s2)
    except Exception:
        return None


def check_once(settings: Settings) -> int:
    init_db(settings.db_path)

    arr = fetch_index(settings)
    index_map = build_index_map(arr)

    now = datetime.now(timezone.utc)
    any_fail = False
    
    prefix = "Station: "
    name_width = len(prefix) + max(len(s.name) for s in settings.stations)  # dynamisch je nach längster Station
    value_width = 6  # z.B. "110.0" passt, ggf. 7 wenn du >999 erwartest
    time_width = 16  # "HH:MM TT:MM:JJJJ" = 16 Zeichen    
        
    with sqlite3.connect(settings.db_path) as con:
        for station in settings.stations:
            try:
                ts_iso, value, source, unit = latest_for_station(index_map, station)
                dt = _to_dt(ts_iso) or now
                unit_disp = f" {unit}".rstrip()
                time_disp = _format_local(dt)

                level = _compute_level(value, station.thresholds_cm)
                level_text = "OK" if level == 0 else f"{level} ({station.level_names[level-1] if (level-1) < len(station.level_names) else f'Warnstufe {level}'})"

                # Ausgabe (immer)
                display_name = f"{prefix}{station.name}"
                unit_disp = (unit or "cm").strip()  # falls mal leer
                print(
                    f"{display_name:<{name_width}} | "
                    f"Pegel: {value:>{value_width}.1f} {unit_disp:<3} | "
                    f"Zeitpunkt des Messwertes: {time_disp:<{time_width}} | "
                    f"Pegel-Stufe: {level_text}"
                )

                # DB speichern
                con.execute(
                    "INSERT OR IGNORE INTO measurements(station_no, station_id_public, station_name, parameter, ts, value, level, source, unit) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        station.station_no,
                        station.station_id_public,
                        station.name,
                        station.parameter,
                        ts_iso,
                        value,
                        level,
                        source,
                        unit,
                    ),
                )

                # State keys (pro Station/Parameter)
                key_alert_ts = f"last_alert_ts:{station.station_no}:{station.parameter}"
                key_last_level = f"last_level:{station.station_no}:{station.parameter}"

                last_alert_ts = db_get_state(con, key_alert_ts)
                last_alert_dt = _to_dt(last_alert_ts) if last_alert_ts else None

                last_level_str = db_get_state(con, key_last_level)
                last_level = _parse_int_or_none(last_level_str)

                # Alarm-Entscheidung:
                # - Warnstufe muss >=1 sein
                # - optional: nur bei Stufenanstieg
                # - zusätzlich Rate-Limit in Minuten (für gleiche Stufe / Wiederholungen)
                should_alert = level >= 1

                if should_alert and settings.alert_on_level_increase and last_level is not None:
                    if level <= last_level:
                        should_alert = False

                # Erstlauf ohne last_level: optional
                if should_alert and last_level is None and not settings.alert_on_start:
                    should_alert = False

                # Rate-Limit
                if should_alert and last_alert_dt is not None:
                    delta_min = (now - last_alert_dt).total_seconds() / 60.0
                    if delta_min < settings.min_alert_interval_minutes:
                        # wenn Stufenanstieg aktiv ist, darf ein echter Anstieg trotzdem durch
                        if not (settings.alert_on_level_increase and last_level is not None and level > last_level):
                            should_alert = False

                if should_alert:
                    if _email_config_ok(settings):
                        level_name = station.level_names[level - 1] if (level - 1) < len(station.level_names) else f"Warnstufe {level}"
                        thresholds = station.thresholds_cm
                        current_threshold = thresholds[level - 1]

                        subject = f"WARNSTUFE {level} {station.name}: {value:.1f}{unit_disp} (>= {current_threshold:.1f}{unit_disp})"
                        body = (
                            f"Pegel-Warnung (HLNUG / WISKI-Web)\n\n"
                            f"Station: {station.name}\n"
                            f"Warnstufe: {level} ({level_name})\n"
                            f"Station-ID (Web): {station.station_id_public}\n"
                            f"Station-No (Daten): {station.station_no}\n"
                            f"Parameter: {station.parameter}\n"
                            f"Messwert: {value:.1f}{unit_disp}\n"
                            f"Zeit (Berlin): {time_disp}\n"
                            f"Zeitstempel (Quelle, ISO): {ts_iso}\n\n"
                            f"Thresholds (cm): {', '.join('{:.1f}'.format(t) for t in thresholds)}\n"
                            f"Quelle (Endpoint): {source}\n"
                        )
                        send_email(settings, subject, body)
                        db_set_state(con, key_alert_ts, now.isoformat())
                        print(f"WARNMAIL: {station.name} -> an {settings.mail_to} gesendet.")
                    else:
                        print(
                            f"WARNUNG: {station.name} (Warnstufe {level}), aber E-Mail/SMTP-Konfig unvollständig.",
                            file=sys.stderr,
                        )

                # last_level immer aktualisieren
                db_set_state(con, key_last_level, str(level))

            except Exception as e:
                any_fail = True
                print(f"Fehler bei Station '{station.name}': {e}", file=sys.stderr)

        con.commit()

    return 1 if any_fail else 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.ini", help="Pfad zur config.ini (default: neben EXE/Script)")
    args = ap.parse_args()

    app_dir = get_app_dir()
    cfg = Path(args.config)
    if not cfg.is_absolute():
        cfg = (app_dir / cfg).resolve()

    settings = load_settings(cfg)

    if settings.debug:
        print(
            f"[DEBUG] Stationen: {len(settings.stations)} | "
            f"Intervall: {settings.poll_interval_seconds}s | Mode: {settings.mode} | "
            f"alert_on_start={settings.alert_on_start} | alert_on_level_increase={settings.alert_on_level_increase}"
        )

    if settings.mode == "daemon":
        while True:
            try:
                check_once(settings)
            except Exception as e:
                print(f"Fehler: {e}", file=sys.stderr)
            time.sleep(settings.poll_interval_seconds)

    return check_once(settings)


if __name__ == "__main__":
    raise SystemExit(main())
