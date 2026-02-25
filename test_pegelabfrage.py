#!/usr/bin/env python3
# test_pegelabfrage.py
#
# Test-Harness für pegelabfrage.py:
# - mockt HLNUG index.json Abruf
# - erzeugt temporäre Config + DB
# - ruft load_settings() und check_once() direkt aus dem Hauptmodul auf
#
# Usage:
#   python .\test_pegelabfrage.py
#   python .\test_pegelabfrage.py --main .\pegelabfrage.py
#   python .\test_pegelabfrage.py --align-check

import argparse
import contextlib
import importlib.util
import io
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, List


class FakeResponse:
    def __init__(self, status_code: int, payload: Any):
        self.status_code = status_code
        self._payload = payload
        self.text = str(payload)[:500]

    def json(self) -> Any:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


def load_main_module(main_path: Path):
    spec = importlib.util.spec_from_file_location(main_path.stem, main_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Konnte Modul nicht laden: {main_path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[main_path.stem] = mod
    spec.loader.exec_module(mod)
    return mod


def make_index_payload() -> List[Dict[str, Any]]:
    """
    Simuliert HLNUG layers/10/index.json
    (nur Felder, die unsere Scripts typischerweise verwenden).
    Passe Werte/Stationen bei Bedarf an.
    """
    return [
        {
            "station_id": 41806,
            "station_no": "24810600",
            "station_name": "Unter-Schmitten - Nidda",
            "stationparameter_name": "W",
            "ts_unitsymbol": "cm",
            "timestamp": "2026-02-25T05:45:00+01:00",
            "ts_value": 110.0,  # OK (unter erster Schwelle)
        },
        {
            "station_id": 41801,
            "station_no": "24810552",
            "station_name": "Ulfa - Ulfa",
            "stationparameter_name": "W",
            "ts_unitsymbol": "cm",
            "timestamp": "2026-02-25T13:30:00+01:00",
            "ts_value": 95.0,  # über 4. Schwelle -> höchste Stufe (wenn thresholds passen)
        },
    ]


def write_temp_config(cfg_path: Path, db_path: Path) -> None:
    """
    Temporäre Config für den Test.
    Wichtig: email.enabled=false, damit keine Mails rausgehen.
    """
    cfg = f"""\
[storage]
db_path = {db_path.as_posix()}

[runtime]
mode = once
poll_interval_minutes = 15
poll_interval_seconds = 0
min_alert_interval_minutes = 180
request_timeout_seconds = 20

[email]
enabled = false
to = test@example.org
from = test@example.org

[smtp]
host = smtp.example.org
port = 465
user = test
password = test
use_ssl = true
use_starttls = false

[debug]
enabled = false

; --- 2 Teststationen (4 Stufen) ---
[station:Unter-Schmitten - Nidda]
station_id_public = 41806
station_no = 24810600
parameter = W
thresholds_cm = 150,180,200,220
level_names = OK,Stufe1,Stufe2,Stufe3

[station:Ulfa - Ulfa]
station_id_public = 41801
station_no = 24810552
parameter = W
thresholds_cm = 60,70,80,90
level_names = OK,Stufe1,Stufe2,Stufe3
"""
    cfg_path.write_text(cfg, encoding="utf-8")


def patch_requests(main_mod, payload: Any, index_url: str):
    """
    Patcht requests.Session.get (+ optional requests.get) im Hauptmodul,
    damit kein echter HTTP Call passiert.
    """
    if not hasattr(main_mod, "requests"):
        raise RuntimeError("Hauptscript importiert kein 'requests' – Harness kann nicht patchen.")

    real_session_cls = main_mod.requests.Session

    class PatchedSession(real_session_cls):
        def get(self, url, *args, **kwargs):
            u = str(url)
            if u == index_url or u.endswith("/layers/10/index.json"):
                return FakeResponse(200, payload)
            return FakeResponse(404, {"error": f"Unexpected URL in test harness: {u}"})

    # Patch Session
    main_mod.requests.Session = PatchedSession

    # Patch requests.get, falls dein Hauptscript das direkt nutzt
    def patched_get(url, *args, **kwargs):
        u = str(url)
        if u == index_url or u.endswith("/layers/10/index.json"):
            return FakeResponse(200, payload)
        return FakeResponse(404, {"error": f"Unexpected URL in test harness: {u}"})

    main_mod.requests.get = patched_get


def alignment_check(output: str) -> None:
    """
    Optional: prüft, ob 'Pegel:' in allen Zeilen an derselben Stelle startet.
    Das funktioniert nur, wenn dein Hauptscript pro Station genau eine Zeile druckt.
    """
    lines = [ln for ln in output.splitlines() if ln.strip()]
    pegel_positions = []
    for ln in lines:
        idx = ln.find("Pegel:")
        if idx >= 0:
            pegel_positions.append(idx)
    if len(pegel_positions) >= 2 and len(set(pegel_positions)) != 1:
        raise AssertionError(f"Alignment-Check fehlgeschlagen: Pegel: Positionen = {pegel_positions}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--main", default="pegelabfrage.py", help="Pfad zum Hauptscript (default: pegelabfrage.py)")
    ap.add_argument("--align-check", action="store_true", help="prüft Spalten-Ausrichtung (Pegel:)")
    args = ap.parse_args()

    main_path = Path(args.main).resolve()
    if not main_path.exists():
        raise SystemExit(f"Hauptscript nicht gefunden: {main_path}")

    main_mod = load_main_module(main_path)

    # Erwartete API prüfen
    for fn in ("load_settings", "check_once"):
        if not hasattr(main_mod, fn):
            raise SystemExit(
                f"Hauptscript '{main_path.name}' hat keine Funktion '{fn}()'. "
                "Bitte stelle sicher, dass load_settings() und check_once() existieren."
            )

    # Index-URL aus dem Hauptmodul übernehmen (falls dort definiert)
    index_url = getattr(main_mod, "HLNUG_LASTVALUES_INDEX", "https://www.hlnug.de/static/pegel/wiskiweb3/data/internet/layers/10/index.json")

    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)
        cfg_path = td_path / "config.ini"
        db_path = td_path / "pegel_test.db"

        write_temp_config(cfg_path, db_path)

        # Patch HTTP
        patch_requests(main_mod, make_index_payload(), index_url)

        # Laden + ausführen
        settings = main_mod.load_settings(cfg_path)

        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc = main_mod.check_once(settings)

        out = buf.getvalue()

        # Minimalchecks: beide Stationen müssen erscheinen
        assert "Unter-Schmitten" in out, "Erwartete Ausgabe für Unter-Schmitten fehlt"
        assert "Ulfa" in out, "Erwartete Ausgabe für Ulfa fehlt"

        if args.align_check:
            alignment_check(out)

        print("TEST OK – Szenarien erfolgreich durchgelaufen.")
        print("---- Beispielausgabe ----")
        print(out.rstrip())
        print("-------------------------")
        print(f"Return-Code: {rc}")
        print(f"Temp-Config: {cfg_path}")
        print(f"Temp-DB:     {db_path}")


if __name__ == "__main__":
    main()