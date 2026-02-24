
import argparse
import configparser
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path
from typing import Any, List, Optional, Tuple

import requests
import smtplib

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None  # type: ignore


# HLNUG / WISKI-Web Endpoints
HLNUG_LASTVALUES_INDEX = "https://www.hlnug.de/static/pegel/wiskiweb3/data/internet/layers/10/index.json"


# -----------------------------
# EXE-taugliche Pfade
# -----------------------------
def get_app_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


# -----------------------------
# Config
# -----------------------------
@dataclass
class Settings:
    station_name: str
    station_id_public: str
    station_no: str
    parameter: str  # z.B. W

    threshold_cm: float
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

    debug: bool


def _read_bool(cp: configparser.ConfigParser, section: str, key: str, default: bool) -> bool:
    if not cp.has_section(section) or not cp.has_option(section, key):
        return default
    return cp.getboolean(section, key, fallback=default)


def load_settings(config_path: Path) -> Settings:
    cp = configparser.ConfigParser()
    if not config_path.exists():
        raise FileNotFoundError(f"Config-Datei nicht gefunden: {config_path}")

    cp.read(config_path, encoding="utf-8")

    station_name = cp.get("station", "name", fallback="(unknown)").strip()
    station_id_public = cp.get("station", "station_id_public", fallback="").strip()
    station_no = cp.get("station", "station_no", fallback="").strip()
    parameter = cp.get("station", "parameter", fallback="W").strip()

    threshold_cm = cp.getfloat("threshold", "value_cm", fallback=0.0)

    db_path = Path(cp.get("storage", "db_path", fallback="pegel.db").strip() or "pegel.db")

    mode = cp.get("runtime", "mode", fallback="once").strip().lower()

    # Intervall: Minuten ODER Sekunden (Minuten bevorzugt, falls gesetzt)
    poll_interval_minutes = cp.getint("runtime", "poll_interval_minutes", fallback=15)
    poll_interval_seconds = cp.getint("runtime", "poll_interval_seconds", fallback=0)
    if poll_interval_seconds and poll_interval_seconds > 0:
        interval_seconds = int(poll_interval_seconds)
    else:
        interval_seconds = int(poll_interval_minutes) * 60

    min_alert_interval_minutes = cp.getint("runtime", "min_alert_interval_minutes", fallback=180)
    request_timeout_seconds = cp.getint("runtime", "request_timeout_seconds", fallback=20)

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

    # Validierung
    if not station_no:
        raise ValueError("station.station_no fehlt in config.ini")
    if threshold_cm <= 0:
        raise ValueError("threshold.value_cm muss > 0 sein")
    if mode not in ("once", "daemon"):
        raise ValueError("runtime.mode muss 'once' oder 'daemon' sein")
    if interval_seconds < 10:
        raise ValueError("Intervall zu klein (mindestens 10 Sekunden).")

    return Settings(
        station_name=station_name,
        station_id_public=station_id_public,
        station_no=station_no,
        parameter=parameter,
        threshold_cm=float(threshold_cm),
        db_path=db_path,
        mode=mode,
        poll_interval_seconds=int(interval_seconds),
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
        debug=bool(debug),
    )


# -----------------------------
# DB (inkl. Migration)
# -----------------------------
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
                parameter  TEXT NOT NULL,
                ts         TEXT NOT NULL,
                value      REAL NOT NULL,
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
        if "source" not in cols:
            con.execute("ALTER TABLE measurements ADD COLUMN source TEXT")
        if "unit" not in cols:
            con.execute("ALTER TABLE measurements ADD COLUMN unit TEXT")
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


# -----------------------------
# Helpers
# -----------------------------
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
    """
    Ausgabeformat: HH:MM TT:MM:JJJJ
    Beispiel: 14:07 24:02:2026
    """
    fmt = "%H:%M %d:%m:%Y"
    if ZoneInfo is None:
        return dt.strftime(fmt)
    try:
        berlin = ZoneInfo("Europe/Berlin")
        return dt.astimezone(berlin).strftime(fmt)
    except Exception:
        return dt.strftime(fmt)


# -----------------------------
# Fetch: immer aktuellster Wert
# -----------------------------
def fetch_latest(settings: Settings) -> Tuple[str, float, str, str]:
    """
    Liefert den aktuellsten Wert über layers/10/index.json:
    Rückgabe: (timestamp_iso, value, source, unit)
    """
    session = requests.Session()
    session.headers.update({"User-Agent": "pegel-alarm/1.4"})

    r = session.get(HLNUG_LASTVALUES_INDEX, timeout=settings.request_timeout_seconds)
    _debug_print(settings, f"[DEBUG] GET {HLNUG_LASTVALUES_INDEX} -> {r.status_code}")
    r.raise_for_status()

    arr = r.json()
    if not isinstance(arr, list):
        raise RuntimeError("index.json hat unerwartete Struktur (kein Array).")

    wanted_no = str(settings.station_no).strip()
    wanted_id = str(settings.station_id_public).strip() if settings.station_id_public else ""
    wanted_param = settings.parameter.strip()

    _debug_print(settings, f"[DEBUG] index entries: {len(arr)}")

    for item in arr:
        if not isinstance(item, dict):
            continue

        station_no = str(item.get("station_no", "")).strip()
        station_id = str(item.get("station_id", "")).strip()
        param = str(item.get("stationparameter_name", "")).strip()

        # Parameter filtern (z.B. W)
        if param and wanted_param and param != wanted_param:
            continue

        if (wanted_no and station_no == wanted_no) or (wanted_id and station_id == wanted_id):
            ts = item.get("timestamp")
            val = item.get("ts_value")
            unit = str(item.get("ts_unitsymbol", "") or "").strip()

            dt = _to_dt(ts)
            fv = _try_float(val)

            _debug_print(settings, f"[DEBUG] match item keys={list(item.keys())}")

            if dt is None or fv is None:
                raise RuntimeError(
                    f"Match gefunden, aber timestamp/value nicht parsebar: timestamp={ts!r}, ts_value={val!r}"
                )

            return dt.isoformat(), float(fv), "layers:10:index", unit

    raise RuntimeError("Station im index.json nicht gefunden (station_no/station_id prüfen).")


# -----------------------------
# Email
# -----------------------------
def _email_config_ok(settings: Settings) -> bool:
    if not settings.email_enabled:
        return False
    required = [
        settings.mail_to,
        settings.mail_from,
        settings.smtp_host,
        settings.smtp_user,
        settings.smtp_password,
    ]
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


# -----------------------------
# Core
# -----------------------------
def check_once(settings: Settings) -> int:
    init_db(settings.db_path)

    ts_iso, value, source, unit = fetch_latest(settings)
    dt = _to_dt(ts_iso) or datetime.now(timezone.utc)

    unit_disp = f" {unit}".rstrip()
    time_disp = _format_local(dt)

    # Immer Stationsname + Pegel + Zeit ausgeben
    print(
        f"\n\nStation: {settings.station_name}\n"
        #f"(Web-ID {settings.station_id_public}, No {settings.station_no}) | "
        f"Pegel: {value:.1f}{unit_disp}\n"
        f"Zeit Pegelstand: {time_disp}\n"
        #f"Quelle: {source}\n"
        f"Erneute Abfrage in 15min - Verbindung zu Feuersoftware hergestellt, Pegelwarnung aktiv"
    )

    with sqlite3.connect(settings.db_path) as con:
        con.execute(
            "INSERT OR IGNORE INTO measurements(station_no, parameter, ts, value, source, unit) VALUES (?, ?, ?, ?, ?, ?)",
            (settings.station_no, settings.parameter, ts_iso, value, source, unit),
        )
        con.commit()

        now = datetime.now(timezone.utc)
        last_alert_ts = db_get_state(con, "last_alert_ts")
        last_alert_dt = _to_dt(last_alert_ts) if last_alert_ts else None

        should_alert = value >= settings.threshold_cm
        if should_alert and last_alert_dt:
            delta_min = (now - last_alert_dt).total_seconds() / 60.0
            if delta_min < settings.min_alert_interval_minutes:
                should_alert = False

        if should_alert:
            if not _email_config_ok(settings):
                print("Schwellwert erreicht, aber E-Mail/SMTP-Konfig unvollständig.", file=sys.stderr)
                return 3

            subject = (
                f"ALARM Pegel {settings.station_name}: {value:.1f}{unit_disp} "
                f"(>= {settings.threshold_cm:.1f}{unit_disp})"
            )
            body = (
                f"Pegel-Alarm (HLNUG / WISKI-Web)\n\n"
                f"Station: {settings.station_name}\n"
                f"Station-ID (Web): {settings.station_id_public}\n"
                f"Station-No (Daten): {settings.station_no}\n"
                f"Parameter: {settings.parameter}\n"
                f"Messwert: {value:.1f}{unit_disp}\n"
                f"Schwellwert: {settings.threshold_cm:.1f}{unit_disp}\n"
                f"Zeit (Berlin): {time_disp}\n"
                f"Zeitstempel (Quelle, ISO): {ts_iso}\n"
                f"Quelle (Endpoint): {source}\n\n"
                f"Übersicht:\n"
                f"https://www.hlnug.de/static/pegel/wiskiweb3/webpublic/#/overview/Wasserstand/"
                f"station/{settings.station_id_public}/{settings.station_name}/Wasserstand?period=P7D\n"
            )

            send_email(settings, subject, body)
            db_set_state(con, "last_alert_ts", now.isoformat())
            con.commit()

            print(
                f"ALARM: {settings.station_name} | "
                f"{value:.1f}{unit_disp} >= {settings.threshold_cm:.1f}{unit_disp} | "
                f"Mail an {settings.mail_to} gesendet."
            )

        return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.ini", help="Pfad zur config.ini (default: neben EXE/Script)")
    args = ap.parse_args()

    app_dir = get_app_dir()
    cfg = Path(args.config)
    if not cfg.is_absolute():
        cfg = (app_dir / cfg).resolve()

    settings = load_settings(cfg)

    if settings.mode == "daemon":
        while True:
            try:
                rc = check_once(settings)
                if rc not in (0, 3):
                    print(f"Fehlercode: {rc}", file=sys.stderr)
            except Exception as e:
                print(f"Fehler: {e}", file=sys.stderr)

            time.sleep(settings.poll_interval_seconds)

    return check_once(settings)


if __name__ == "__main__":
    raise SystemExit(main())