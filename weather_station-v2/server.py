"""
Weather dashboard server
- Reads readings from weather.db and serves them to the web page
- Tracks device state over MQTT (online/offline, on/off)
- Sends on/off commands to weather/<NODE_ID>/cmd

Run:  pip install paho-mqtt   then   python server.py
Open http://localhost:8000  (from a phone on the same hotspot: http://<laptop IP>:8000)
"""
import json
import os
import socket
import sqlite3
import sys
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import paho.mqtt.client as mqtt

# ================== SETTINGS ==================
HERE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(HERE, "weather.db")
MQTT_HOST = "127.0.0.1"      # Mosquitto runs on this laptop
MQTT_PORT = 1883
NODE_ID = "node1"
PORT = 8000
DB_TIME_IS_UTC = False       # set to True if chart times are off by 7 hours
CONTROL_PIN = ""             # e.g. "1234" to require a PIN for the power switch (empty = no PIN)
MAX_POINTS = 800
# ==============================================

TOPIC_CMD = f"weather/{NODE_ID}/cmd"
TOPIC_POWER = f"weather/{NODE_ID}/power"
TOPIC_RESTART = f"weather/{NODE_ID}/restart"   # NOT retained, so the board cannot get stuck in a reboot loop
TOPIC_STATUS = f"weather/{NODE_ID}/status"

FIELDS = {
    "temp": ["temp", "temperature"],
    "hum": ["hum", "humidity"],
    "press": ["press", "pressure"],
    "sealevel": ["sealevel", "sea_level"],
    "dew": ["dew", "dewpoint", "dew_point"],
    "rain": ["rain"],
    "rssi": ["rssi"],
    "node": ["node", "node_id"],
}
TIME_COLS = ["ts", "timestamp", "time", "created_at", "datetime", "date_time", "recorded_at"]

state = {"online": None, "power": None}


# ---------- MQTT ----------
def make_client():
    try:
        return mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                           client_id=f"dashboard-{NODE_ID}")
    except AttributeError:  # paho-mqtt 1.x
        return mqtt.Client(client_id=f"dashboard-{NODE_ID}")


client = make_client()


def on_connect(c, userdata, flags, rc, *args):
    c.subscribe([(TOPIC_POWER, 0), (TOPIC_STATUS, 0)])


def on_message(c, userdata, msg):
    val = msg.payload.decode(errors="ignore").strip()
    if msg.topic == TOPIC_POWER:
        state["power"] = val
    elif msg.topic == TOPIC_STATUS:
        state["online"] = (val == "online")


client.on_connect = on_connect
client.on_message = on_message
client.connect_async(MQTT_HOST, MQTT_PORT, 30)
client.loop_start()


# ---------- Database ----------
def open_db():
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=5)
    conn.row_factory = sqlite3.Row
    return conn


def detect_schema(conn):
    for (name,) in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall():
        cols = [r[1] for r in conn.execute(f'PRAGMA table_info("{name}")')]
        low = {c.lower(): c for c in cols}
        if not any(k in low for k in FIELDS["temp"]):
            continue
        mapping = {}
        for field, names in FIELDS.items():
            for n in names:
                if n in low:
                    mapping[field] = low[n]
                    break
        tcol = next((low[c] for c in TIME_COLS if c in low), None)
        return name, mapping, tcol
    raise RuntimeError("No table with a temperature column found in weather.db")


def to_epoch(v):
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return v / 1000.0 if v > 1e12 else float(v)
    try:
        dt = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None and DB_TIME_IS_UTC:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def fetch_rows(limit):
    conn = open_db()
    try:
        table, mapping, tcol = detect_schema(conn)
        select = ", ".join(f'"{col}" AS {field}' for field, col in mapping.items())
        if tcol:
            select += f', "{tcol}" AS _t'
        sql = f'SELECT {select} FROM "{table}"'
        args = []
        if "node" in mapping:
            sql += f' WHERE "{mapping["node"]}" = ?'
            args.append(NODE_ID)
        sql += f' ORDER BY {"_t" if tcol else "rowid"} DESC LIMIT ?'
        args.append(limit)
        rows = []
        for r in conn.execute(sql, args).fetchall():
            d = {k: r[k] for k in mapping if k != "node"}
            d["t"] = to_epoch(r["_t"]) if tcol else None
            rows.append(d)
        rows.reverse()
        return rows
    finally:
        conn.close()


def api_latest():
    rows = fetch_rows(1)
    return {"node": NODE_ID, "reading": rows[-1] if rows else None}


def api_history(hours):
    hours = max(1, min(hours, 24 * 14))
    rows = fetch_rows(hours * 70)  # data arrives every 60 s, plus some margin
    if rows and rows[-1]["t"]:
        cutoff = rows[-1]["t"] - hours * 3600
        rows = [r for r in rows if r["t"] and r["t"] >= cutoff]
    if len(rows) > MAX_POINTS:
        step = -(-len(rows) // MAX_POINTS)
        rows = rows[::step]
    return {"hours": hours, "rows": rows}


def api_status():
    return {"node": NODE_ID, "online": state["online"], "power": state["power"],
            "mqtt": client.is_connected(), "pin_required": bool(CONTROL_PIN)}


def api_forecast():
    conn = open_db()
    try:
        try:
            f = conn.execute("SELECT ts, rain, temp3, temp6, reasons FROM forecasts ORDER BY ts DESC LIMIT 1").fetchone()
            checked = conn.execute("SELECT rain, rain_actual FROM forecasts WHERE rain_actual IS NOT NULL").fetchall()
        except sqlite3.OperationalError:
            return {"forecast": None}
        thr = 50
        tp = sum(1 for r, a in checked if r >= thr and a)
        fn = sum(1 for r, a in checked if r < thr and a)
        fp = sum(1 for r, a in checked if r >= thr and not a)
        return {
            "forecast": None if not f else {"t": f["ts"], "rain": f["rain"], "temp3": f["temp3"],
                                            "temp6": f["temp6"], "reasons": f["reasons"]},
            "score": {"checked": len(checked), "rain_events": tp + fn, "caught": tp, "false_alarms": fp},
        }
    finally:
        conn.close()


# ---------- HTTP ----------
class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def send_json(self, obj, code=200):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        url = urlparse(self.path)
        try:
            if url.path in ("/", "/index.html"):
                with open(os.path.join(HERE, "dashboard.html"), "rb") as f:
                    body = f.read()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            elif url.path == "/api/latest":
                self.send_json(api_latest())
            elif url.path == "/api/history":
                hours = int(parse_qs(url.query).get("hours", ["24"])[0])
                self.send_json(api_history(hours))
            elif url.path == "/api/forecast":
                self.send_json(api_forecast())
            elif url.path == "/api/status":
                self.send_json(api_status())
            else:
                self.send_json({"error": "not found"}, 404)
        except Exception as e:
            self.send_json({"error": str(e)}, 500)

    def do_POST(self):
        path = urlparse(self.path).path
        if path not in ("/api/power", "/api/restart"):
            return self.send_json({"error": "not found"}, 404)
        try:
            n = int(self.headers.get("Content-Length", 0))
            data = json.loads(self.rfile.read(n) or b"{}")
        except Exception:
            return self.send_json({"error": "bad request"}, 400)
        if CONTROL_PIN and data.get("pin") != CONTROL_PIN:
            return self.send_json({"error": "Incorrect PIN"}, 403)
        if not client.is_connected():
            return self.send_json({"error": "Can't connect to MQTT. Check that Mosquitto is running."}, 503)
        if path == "/api/restart":
            client.publish(TOPIC_RESTART, "restart", qos=0, retain=False)
            return self.send_json({"sent": "restart"})
        if data.get("state") not in ("on", "off"):
            return self.send_json({"error": "state must be on or off"}, 400)
        client.publish(TOPIC_CMD, data["state"], qos=0, retain=True)
        self.send_json({"sent": data["state"]})


def local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    except OSError:
        return "127.0.0.1"


if __name__ == "__main__":
    if not os.path.exists(DB_PATH):
        sys.exit(f"Cannot find {DB_PATH}. Put server.py in the same folder as weather.db")
    print(f"Dashboard: http://localhost:{PORT}   |   From phone: http://{local_ip()}:{PORT}")
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()