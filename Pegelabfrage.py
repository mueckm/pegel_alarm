#!/usr/bin/env python3
# Pegelabfrage.py (multi-station + 3/4 Warnstufen pro Messstelle, v2.3-json)
#
# Identisches Verhalten wie die INI-Version, aber Konfiguration aus JSON-Datei.
# Erwartete Config-Datei: config.json (oder per --config Pfad angeben)

import argparse
import json
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
    level_names: Tuple[str, ...]      # Namen für Warnstufe 1..N


@dataclass(frozen=True)
class Settings:
    stations: List[StationConfig]

    db_path: Path

    mode: str  # once | daemon
    poll_interval_seconds: int
    min_alert_interval_minutes: int
    request_timeout_seconds: int

    rearm_below_hours: float  # Stunden unterhalb Schwelle, bevor erneuter Alarm für dieselbe Meldestufe möglich ist

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


def _as_bool(v: Any, default: bool) -> bool:
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    s = str(v).strip().lower()
    if s in ("1", "true", "yes", "y", "on"):
        return True
    if s in ("0", "false", "no", "n", "off"):
        return False
    return default


def _read_bool(cfg: Dict[str, Any], section: str, key: str, default: bool) -> bool:
    sec = cfg.get(section, {})
    if not isinstance(sec, dict):
        return default
    return _as_bool(sec.get(key), default)


def _parse_csv_floats(s: str) -> List[float]:
    vals: List[float] = []
    for part in (s or "").split(","):
        p = part.strip()
        if not p:
            continue
        p = p.replace(",", ".")
        vals.append(float(p))
    return vals


def _thresholds_from_any(value: Any) -> Optional[List[float]]:
    """Akzeptiert Liste [..] oder CSV-String "a,b,c"."""
    if value is None:
        return None
    if isinstance(value, list):
        out = []
        for x in value:
            if x is None:
                continue
            out.append(float(x))
        return out
    if isinstance(value, str):
        return _parse_csv_floats(value)
    # single number not supported here
    return None


def _parse_thresholds_for_section(section_data: Dict[str, Any], section_name: str, fallback_thresholds: Tuple[float, ...]) -> Tuple[float, ...]:
    """
    Unterstützte Keys (pro Station oder global):
      - thresholds_cm = [a,b,c] oder [a,b,c,d] oder CSV-String
      - threshold1_cm / threshold2_cm / threshold3_cm [/ threshold4_cm]
      - legacy: threshold_cm  -> eine Stufe (Warnstufe 1), wird als (x,) interpretiert
    """
    # thresholds_cm list/CSV
    if "thresholds_cm" in section_data:
        vals = _thresholds_from_any(section_data.get("thresholds_cm"))
        if vals is None:
            raise ValueError(f"[{section_name}] thresholds_cm ist gesetzt, aber nicht parsebar (Liste oder CSV erwartet)")
        if len(vals) not in (3, 4):
            raise ValueError(f"[{section_name}] thresholds_cm muss 3 oder 4 Werte haben")
        vals = sorted(vals)
        if any(v <= 0 for v in vals):
            raise ValueError(f"[{section_name}] thresholds_cm: alle Werte müssen > 0 sein")
        for i in range(1, len(vals)):
            if not (vals[i - 1] < vals[i]):
                raise ValueError(f"[{section_name}] thresholds_cm muss strikt aufsteigend sein")
        return tuple(vals)

    # Einzelwerte threshold1..4
    any_single = any(k in section_data for k in ("threshold1_cm", "threshold2_cm", "threshold3_cm", "threshold4_cm"))
    if any_single:
        t1 = float(section_data.get("threshold1_cm") or 0.0)
        t2 = float(section_data.get("threshold2_cm") or 0.0)
        t3 = float(section_data.get("threshold3_cm") or 0.0)
        t4 = float(section_data.get("threshold4_cm") or 0.0)
        vals = [t for t in (t1, t2, t3, t4) if t and t > 0]
        if len(vals) not in (3, 4):
            raise ValueError(f"[{section_name}] threshold1_cm..threshold4_cm: es müssen 3 oder 4 Werte > 0 gesetzt sein")
        vals = sorted(vals)
        for i in range(1, len(vals)):
            if not (vals[i - 1] < vals[i]):
                raise ValueError(f"[{section_name}] Thresholds müssen strikt aufsteigend sein")
        return tuple(vals)

    # legacy single threshold_cm
    if "threshold_cm" in section_data:
        x = float(section_data.get("threshold_cm") or 0.0)
        if x <= 0:
            raise ValueError(f"[{section_name}] threshold_cm muss > 0 sein")
        return (x,)

    return fallback_thresholds


def _parse_level_names_for_section(section_data: Dict[str, Any], section_name: str, fallback: Tuple[str, ...], n_levels: int) -> Tuple[str, ...]:
    """Optional: level_names als Liste oder CSV-String, muss zur Anzahl Thresholds passen."""
    if "level_names" in section_data:
        v = section_data.get("level_names")
        if isinstance(v, list):
            parts = [str(p).strip() for p in v if str(p).strip()]
        else:
            parts = [p.strip() for p in str(v or "").split(",") if p.strip()]
        if len(parts) != n_levels:
            raise ValueError(f"[{section_name}] level_names muss genau {n_levels} Werte haben (passend zu thresholds)")
        return tuple(parts)

    if len(fallback) == n_levels:
        return fallback

    return tuple(f"Warnstufe {i}" for i in range(1, n_levels + 1))


def load_settings(config_path: Path) -> Settings:
    if not config_path.exists():
        raise FileNotFoundError(f"Config-Datei nicht gefunden: {config_path}")

    cfg = json.loads(config_path.read_text(encoding="utf-8"))
    if not isinstance(cfg, dict):
        raise ValueError("Config muss ein JSON-Objekt sein (Top-Level dict).")

    # Global thresholds
    threshold_sec = cfg.get("threshold", {})
    if threshold_sec is None:
        threshold_sec = {}
    if not isinstance(threshold_sec, dict):
        threshold_sec = {}

    fallback_thresholds: Tuple[float, ...]
    if (
        "thresholds_cm" in threshold_sec
        or any(k in threshold_sec for k in ("threshold1_cm", "threshold2_cm", "threshold3_cm", "threshold4_cm"))
        or "threshold_cm" in threshold_sec
    ):
        fallback_thresholds = _parse_thresholds_for_section(threshold_sec, "threshold", (0.0, 0.0, 0.0))
    else:
        # legacy global value_cm (eine Stufe)
        global_value = float(threshold_sec.get("value_cm") or 0.0)
        if global_value > 0:
            fallback_thresholds = (global_value,)
        else:
            fallback_thresholds = (0.0, 0.0, 0.0)

    if min(fallback_thresholds) <= 0:
        raise ValueError(
            "Keine gültigen Thresholds gefunden. Setze entweder threshold.thresholds_cm=... (3 oder 4 Werte) "
            "oder pro Station thresholds_cm=..."
        )

    # Global level names (optional)
    global_level_names: Tuple[str, ...] = tuple(f"Warnstufe {i}" for i in range(1, len(fallback_thresholds) + 1))
    global_level_names = _parse_level_names_for_section(threshold_sec, "threshold", fallback=global_level_names, n_levels=len(fallback_thresholds))

    # Stationen: bevorzugt cfg['stations'] als Liste
    # Zusätzlich unterstützt: 1:1-INI->JSON Mapping mit Top-Level Keys "station:<Name>".
    stations: List[StationConfig] = []
    stations_raw = cfg.get("stations")

    station_sections: List[Tuple[str, Dict[str, Any]]] = []
    for k, v in cfg.items():
        if isinstance(k, str) and k.lower().startswith("station:") and isinstance(v, dict):
            station_sections.append((k, v))

    if isinstance(stations_raw, list) and stations_raw:
        for i, st in enumerate(stations_raw, start=1):
            if not isinstance(st, dict):
                raise ValueError(f"stations[{i}] muss ein Objekt sein")
            name = str(st.get("name") or f"station-{i}").strip()
            station_id_public = str(st.get("station_id_public") or st.get("station_id") or "").strip()
            station_no = str(st.get("station_no") or "").strip()
            parameter = str(st.get("parameter") or "W").strip()

            if not station_no:
                raise ValueError(f"stations[{i}] ({name}): station_no fehlt")

            thresholds = _parse_thresholds_for_section(st, name, fallback_thresholds)
            for j in range(1, len(thresholds)):
                if not (thresholds[j-1] < thresholds[j]):
                    raise ValueError(f"{name}: thresholds müssen strikt aufsteigend sein")
            if min(thresholds) <= 0:
                raise ValueError(f"{name}: thresholds müssen > 0 sein")

            level_names = _parse_level_names_for_section(st, name, fallback=global_level_names, n_levels=len(thresholds))

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
    elif station_sections:
        # 1:1: station:<Name> Sections
        for sec_name, st in station_sections:
            header_name = sec_name.split(":", 1)[1].strip() if ":" in sec_name else sec_name
            name = str(st.get("name") or header_name).strip() or header_name
            station_id_public = str(st.get("station_id_public") or st.get("station_id") or "").strip()
            station_no = str(st.get("station_no") or "").strip()
            parameter = str(st.get("parameter") or "W").strip()

            if not station_no:
                raise ValueError(f"{sec_name}: station_no fehlt")

            thresholds = _parse_thresholds_for_section(st, name, fallback_thresholds)
            for j in range(1, len(thresholds)):
                if not (thresholds[j-1] < thresholds[j]):
                    raise ValueError(f"{name}: thresholds müssen strikt aufsteigend sein")
            if min(thresholds) <= 0:
                raise ValueError(f"{name}: thresholds müssen > 0 sein")

            level_names = _parse_level_names_for_section(st, name, fallback=global_level_names, n_levels=len(thresholds))

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
        # Backward compatible: single cfg['station']
        st = cfg.get("station")
        if not isinstance(st, dict):
            raise ValueError("Keine Stationen gefunden. Nutze stations=[...] oder station={...}.")

        name = str(st.get("name") or "(unknown)").strip()
        station_id_public = str(st.get("station_id_public") or "").strip()
        station_no = str(st.get("station_no") or "").strip()
        parameter = str(st.get("parameter") or "W").strip()

        if not station_no:
            raise ValueError("station.station_no fehlt in config.json")

        thresholds = _parse_thresholds_for_section(st, "station", fallback_thresholds)
        level_names = _parse_level_names_for_section(st, "station", fallback=global_level_names, n_levels=len(thresholds))

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
    storage = cfg.get("storage", {})
    if not isinstance(storage, dict):
        storage = {}
    db_path = Path(str(storage.get("db_path") or "pegel.db")).expanduser()

    # Runtime
    runtime = cfg.get("runtime", {})
    if not isinstance(runtime, dict):
        runtime = {}
    mode = str(runtime.get("mode") or "once").strip().lower()

    poll_interval_minutes = int(runtime.get("poll_interval_minutes") or 15)
    poll_interval_seconds_raw = int(runtime.get("poll_interval_seconds") or 0)
    poll_interval_seconds = poll_interval_seconds_raw if poll_interval_seconds_raw > 0 else poll_interval_minutes * 60

    min_alert_interval_minutes = int(runtime.get("min_alert_interval_minutes") or 180)
    request_timeout_seconds = int(runtime.get("request_timeout_seconds") or 20)

    rearm_below_hours = float(runtime.get("rearm_below_hours") or 6)

    alert_on_start = _as_bool(runtime.get("alert_on_start"), True)
    alert_on_level_increase = _as_bool(runtime.get("alert_on_level_increase"), True)

    # Email + SMTP
    email = cfg.get("email", {})
    if not isinstance(email, dict):
        email = {}
    email_enabled = _as_bool(email.get("enabled"), True)
    mail_to = str(email.get("to") or "").strip()
    mail_from = str(email.get("from") or "").strip()

    smtp = cfg.get("smtp", {})
    if not isinstance(smtp, dict):
        smtp = {}
    smtp_host = str(smtp.get("host") or "").strip()
    smtp_port = int(smtp.get("port") or 465)
    smtp_user = str(smtp.get("user") or "").strip()
    smtp_password = str(smtp.get("password") or "").strip()
    smtp_use_ssl = _as_bool(smtp.get("use_ssl"), True)
    smtp_use_starttls = _as_bool(smtp.get("use_starttls"), False)

    debug_sec = cfg.get("debug", {})
    if not isinstance(debug_sec, dict):
        debug_sec = {}
    debug = _as_bool(debug_sec.get("enabled"), False)

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
        rearm_below_hours=float(rearm_below_hours),
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
                # State (pro Station/Parameter/Schwelle):
                # - E-Mail beim Erreichen/Überschreiten jeder Schwelle (Flanke).
                # - Wiederholung für dieselbe Schwelle erst, wenn der Pegel mindestens rearm_below_hours
                #   am Stück unterhalb dieser Schwelle war und danach erneut überschreitet.
                key_last_level = f"last_level:{station.station_no}:{station.parameter}"

                for th_idx, th in enumerate(station.thresholds_cm):
                    level_name = station.level_names[th_idx] if th_idx < len(station.level_names) else f"Meldestufe {th_idx + 1}"
                    key_armed = f"armed:{station.station_no}:{station.parameter}:{th_idx}"
                    key_below_since = f"below_since:{station.station_no}:{station.parameter}:{th_idx}"

                    armed_str = db_get_state(con, key_armed)
                    armed = True
                    if armed_str is not None and str(armed_str).strip() != "":
                        armed = str(armed_str).strip().lower() in ("1", "true", "yes", "y", "on")

                    below_since_str = db_get_state(con, key_below_since)
                    below_since_dt = _to_dt(below_since_str) if below_since_str else None

                    # Unterhalb der Schwelle: ggf. Re-Arm nach Ablauf der Zeit
                    if value < th:
                        if not armed:
                            if below_since_dt is None:
                                db_set_state(con, key_below_since, dt.isoformat())
                            else:
                                if (dt - below_since_dt).total_seconds() >= settings.rearm_below_hours * 3600:
                                    db_set_state(con, key_armed, "1")
                                    db_set_state(con, key_below_since, "")
                        else:
                            # aufgeräumt halten
                            if below_since_str:
                                db_set_state(con, key_below_since, "")
                        continue

                    # Ab hier: value >= th  (oberhalb der Schwelle)
                    if below_since_str:
                        # Nicht mehr kontinuierlich unterhalb
                        db_set_state(con, key_below_since, "")

                    if not armed:
                        continue

                    # Erstlauf-Unterdrückung (optional)
                    if armed_str is None and not settings.alert_on_start:
                        db_set_state(con, key_armed, "0")
                        continue

                    # E-Mail bei Schwellen-Erreichen
                    if _email_config_ok(settings):
                        subject = f"{level_name} {station.name}: {value:.1f}{unit_disp} (>= {th:.1f}{unit_disp})"
                        body = (
                            f"Pegel-Meldung (HLNUG-Messdaten)\n\n"
                            f"Station: {station.name}\n"
                            f"Meldestufe: {th_idx + 1} ({level_name})\n"
                            f"Schwelle: {th:.1f}{unit_disp}\n"
                            f"Messwert: {value:.1f}{unit_disp}\n"
                            f"Zeitpunkt der Messdaten: {time_disp}\n\n"
                            f"Station-ID (Web): {station.station_id_public}\n"
                            f"Station-No (Daten): {station.station_no}\n"
                            f"Quelle: Pegelwarnung via E-Mail V1.0 - © Marcel Mück\n"
                        )
                        try:
                            send_email(settings, subject, body)
                            print(f"***Pegel-Warnung*** E-Mail gesendet: {station.name} / {level_name}")
                            # Nach erfolgreichem Versand disarmen, bis Re-Arm-Bedingung erfüllt ist
                            db_set_state(con, key_armed, "0")
                        except Exception as e:
                            print(f"***Pegel-Warnung*** Fehler beim Senden der E-Mail: {e}", file=sys.stderr)
                    else:
                        print(
                            f"WARNUNG: {station.name} ({level_name}), aber E-Mail/SMTP-Konfig unvollständig.",
                            file=sys.stderr,
                        )

                # last_level weiterhin speichern (für Anzeige/Verlauf)
                db_set_state(con, key_last_level, str(level))

            except Exception as e:
                any_fail = True
                print(f"Fehler bei Station '{station.name}': {e}", file=sys.stderr)

        con.commit()

    return 1 if any_fail else 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.json", help="Pfad zur config.json (default: neben EXE/Script)")
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
