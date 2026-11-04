"""
Weather station logger (runs on your PC or Raspberry Pi)

 - Listens to MQTT topic  weather/+/data  and saves every reading in SQLite
 - Every 15 minutes, saves OpenWeather's current conditions for comparison
   and for labelling your rain-prediction data

Install:   pip install paho-mqtt requests
Run:       python logger.py
OpenWeather key (optional):
    Windows:  set OPENWEATHER_KEY=your_key
    Linux/Mac: export OPENWEATHER_KEY=your_key
"""

import json
import os
import sqlite3
import threading
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
import requests

# ================== SETTINGS ==================
MQTT_HOST = "localhost"        # broker runs on this machine
MQTT_PORT = 1883
DB_FILE = "weather.db"

LAT, LON = Lat, Lon # Change to your exact location
OPENWEATHER_KEY = os.environ.get("OPENWEATHER_KEY", "")
OPENWEATHER_EVERY_S = 15 * 60
# ==============================================

db_lock = threading.Lock()
db = sqlite3.connect(DB_FILE, check_same_thread=False)

db.executescript("""
CREATE TABLE IF NOT EXISTS readings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    node TEXT NOT NULL,
    temp REAL, hum REAL, press REAL, sealevel REAL, dew REAL,
    rain INTEGER, rssi INTEGER
);
CREATE INDEX IF NOT EXISTS idx_readings_ts ON readings(ts);

CREATE TABLE IF NOT EXISTS openweather (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    temp REAL, hum REAL, press REAL,
    condition TEXT, clouds INTEGER, wind REAL, rain_1h REAL
);
CREATE INDEX IF NOT EXISTS idx_ow_ts ON openweather(ts);
""")
db.commit()


def now_utc():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ---------------- MQTT ----------------
def on_connect(client, userdata, flags, rc, *args):
    print("MQTT connected, result:", rc)
    client.subscribe("weather/+/data")


def on_message(client, userdata, msg):
    try:
        d = json.loads(msg.payload.decode())
        with db_lock:
            db.execute(
                "INSERT INTO readings (ts,node,temp,hum,press,sealevel,dew,rain,rssi) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (now_utc(), d.get("node", "?"), d.get("temp"), d.get("hum"),
                 d.get("press"), d.get("sealevel"), d.get("dew"),
                 d.get("rain"), d.get("rssi")),
            )
            db.commit()
        print(f"saved {d.get('node')}: {d.get('temp')} C, {d.get('hum')} %, "
              f"{d.get('press')} hPa, rain={d.get('rain')}")
    except Exception as e:
        print("bad message:", msg.payload, e)


# ---------------- OpenWeather ----------------
def openweather_loop():
    url = "https://api.openweathermap.org/data/2.5/weather"
    while True:
        try:
            r = requests.get(
                url,
                params={"lat": LAT, "lon": LON, "appid": OPENWEATHER_KEY, "units": "metric"},
                timeout=15,
            )
            r.raise_for_status()
            j = r.json()
            with db_lock:
                db.execute(
                    "INSERT INTO openweather (ts,temp,hum,press,condition,clouds,wind,rain_1h) "
                    "VALUES (?,?,?,?,?,?,?,?)",
                    (now_utc(), j["main"]["temp"], j["main"]["humidity"],
                     j["main"]["pressure"], j["weather"][0]["main"],
                     j.get("clouds", {}).get("all"), j.get("wind", {}).get("speed"),
                     j.get("rain", {}).get("1h", 0.0)),
                )
                db.commit()
            print("OpenWeather saved:", j["weather"][0]["main"], j["main"]["temp"], "C")
        except Exception as e:
            print("OpenWeather error:", e)
        time.sleep(OPENWEATHER_EVERY_S)


def main():
    if OPENWEATHER_KEY:
        threading.Thread(target=openweather_loop, daemon=True).start()
    else:
        print("No OPENWEATHER_KEY set - skipping OpenWeather (sensor logging still works)")

    try:
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)   # paho-mqtt 2.x
    except AttributeError:
        client = mqtt.Client()                                    # paho-mqtt 1.x
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_HOST, MQTT_PORT, 60)
    client.loop_forever()


if __name__ == "__main__":
    main()
