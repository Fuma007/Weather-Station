"""
Forecaster - rain chance and temperature for the next 3-6 hours, from BME280 data only.

Every RUN_EVERY seconds it:
  1. reads recent readings from weather.db
  2. computes a rain chance (rule-based) and temperature at +3 h and +6 h
  3. publishes them to the board (weather/forecast/rain, weather/forecast/temp)
  4. saves each forecast in weather.db (table "forecasts")
  5. fetches real weather from OpenWeather as a reference (table "reference")
  6. scores old forecasts against that reference

Run:  python forecaster.py     (keep the window open, next to logger.py and server.py)
"""
import json
import math
import os
import sqlite3
import time
import urllib.error
import urllib.request
from bisect import bisect_left
from datetime import datetime, timezone

# ================== SETTINGS ==================
HERE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(HERE, "weather.db")
MQTT_HOST = "127.0.0.1"
MQTT_PORT = 1883
NODE_ID = "node1"
DB_TIME_IS_UTC = False          # same setting as server.py

OPENWEATHER_KEY = "Your-OpenWeather-api-key"
LAT, LON = Lat, Lon    # change to your station's exact coordinates (right-click on Google Maps)

RUN_EVERY = 600                 # seconds between forecasts / OpenWeather checks
HORIZON_H = 6                   # a forecast "rain" means rain within this many hours
RAIN_THRESHOLD = 50             # chance >= this counts as "forecast rain" when scoring
# ==============================================

TIME_COLS = ["ts", "timestamp", "time", "created_at", "datetime", "date_time", "recorded_at"]


# ---------- helpers ----------
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


def dew_point(t, rh):
    a, b = 17.62, 243.12
    g = math.log(max(rh, 1) / 100.0) + (a * t) / (b + t)
    return (b * g) / (a - g)


def local_hour(ts):
    d = datetime.fromtimestamp(ts)
    return d.hour + d.minute / 60.0


def nearest(times, rows, ts, tol):
    i = bisect_left(times, ts)
    best = None
    for j in (i - 1, i):
        if 0 <= j < len(rows) and abs(times[j] - ts) <= tol:
            if best is None or abs(times[j] - ts) < abs(times[best] - ts):
                best = j
    return rows[best] if best is not None else None


def detect(conn):
    for (name,) in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall():
        low = {r[1].lower(): r[1] for r in conn.execute(f'PRAGMA table_info("{name}")')}
        pick = lambda *n: next((low[x] for x in n if x in low), None)
        cols = dict(temp=pick("temp", "temperature"), hum=pick("hum", "humidity"),
                    press=pick("sealevel", "sea_level", "press", "pressure"),
                    node=pick("node", "node_id"), time=pick(*TIME_COLS))
        if cols["temp"] and cols["hum"] and cols["press"] and cols["time"]:
            return name, cols
    raise RuntimeError("Could not find the readings table (needs temp, hum, pressure and a time column).")


def load_rows(conn, days=14):
    table, c = detect(conn)
    sql = (f'SELECT "{c["time"]}", "{c["temp"]}", "{c["hum"]}", "{c["press"]}" FROM "{table}"')
    args = []
    if c["node"]:
        sql += f' WHERE "{c["node"]}" = ?'
        args.append(NODE_ID)
    sql += f' ORDER BY "{c["time"]}" DESC LIMIT ?'
    args.append(days * 1440 + 200)
    rows = []
    for t, T, H, P in conn.execute(sql, args).fetchall():
        ts = to_epoch(t)
        if ts is not None and None not in (T, H, P):
            rows.append((ts, float(T), float(H), float(P)))
    rows.sort()
    return rows


# ---------- forecast ----------
def tide(h):
    """Tropical daily pressure tide: peaks near 10:00 and 22:00, about +-1 hPa."""
    return math.cos(2 * math.pi * (h - 10) / 12)


def diurnal(h):
    """Typical daily temperature swing: warmest near 15:00."""
    return 5.0 * math.cos(2 * math.pi * (h - 15) / 24)


def make_forecast(rows):
    if len(rows) < 30:
        return None, "Not enough readings yet."
    times = [r[0] for r in rows]
    now, T, H, P = rows[-1]
    if time.time() - now > 900:
        return None, "Latest reading is older than 15 minutes - is the station on?"
    D = dew_point(T, H)
    spread = T - D

    r3 = nearest(times, rows, now - 3 * 3600, 1200)
    if r3 is None:
        return None, "Need at least 3 hours of data."
    dp3 = P - r3[3]
    r1 = nearest(times, rows, now - 3600, 900)
    dp1 = P - r1[3] if r1 else 0.0
    r24 = nearest(times, rows, now - 86400, 1800)
    dp24 = P - r24[3] if r24 else None

    # Pressure change expected at this time of day (daily tide), learned from past days when possible
    hr = local_hour(now)
    expected = tide(hr) - tide(hr - 3)
    learned = []
    for d in range(1, 15):
        a = nearest(times, rows, now - d * 86400 - 3 * 3600, 1200)
        b = nearest(times, rows, now - d * 86400, 1200)
        if a and b:
            learned.append(b[3] - a[3])
    if len(learned) >= 3:
        expected = sum(learned) / len(learned)
    anomaly = dp3 - expected

    # ----- rain score -----
    s, why = 10.0, []
    if 5 <= datetime.fromtimestamp(now).month <= 10:
        s += 8
    if anomaly <= -2.5:   s += 38; why.append(f"pressure dropping fast ({dp3:+.1f} hPa in 3 h)")
    elif anomaly <= -1.5: s += 26; why.append(f"pressure dropping ({dp3:+.1f} hPa in 3 h)")
    elif anomaly <= -0.8: s += 15; why.append(f"pressure easing down ({dp3:+.1f} hPa in 3 h)")
    elif anomaly <= -0.3: s += 6
    elif anomaly >= 1.0:  s -= 8;  why.append("pressure rising")
    if dp1 <= -1.0:
        s += 8; why.append(f"sharp drop in the last hour ({dp1:+.1f} hPa)")
    if dp24 is not None:
        if dp24 <= -3:   s += 10; why.append("lower than yesterday")
        elif dp24 <= -2: s += 5
        elif dp24 >= 2:  s -= 4
    if spread <= 2:    s += 25; why.append(f"air nearly saturated (T - dew point = {spread:.1f} C)")
    elif spread <= 4:  s += 15; why.append(f"humid air (T - dew point = {spread:.1f} C)")
    elif spread <= 6:  s += 7
    elif spread >= 10: s -= 8;  why.append("dry air")
    if H >= 90:   s += 14
    elif H >= 80: s += 7
    elif H <= 55: s -= 6
    rain = int(round(max(3, min(95, s))))
    if not why:
        why.append("no strong signs of change")

    # ----- temperature at +3 h and +6 h -----
    temps = {}
    for h in (3, 6):
        deltas = []
        for d in range(1, 15):
            a = nearest(times, rows, now - d * 86400, 1200)
            b = nearest(times, rows, now - d * 86400 + h * 3600, 1200)
            if a and b:
                deltas.append(b[1] - a[1])
        delta = (sum(deltas) / len(deltas)) if len(deltas) >= 3 else diurnal(hr + h) - diurnal(hr)
        if rain >= 40:
            delta -= (rain - 40) / 60.0 * 1.5       # rain cools things down
        temps[h] = round(T + max(-12, min(12, delta)), 1)

    return {"ts": now, "rain": rain, "temp3": temps[3], "temp6": temps[6],
            "dp3": round(dp3, 2), "anomaly": round(anomaly, 2), "spread": round(spread, 2),
            "hum": H, "reasons": "; ".join(why),
            "source": "learned" if len(learned) >= 3 else "default"}, None


# ---------- storage ----------
def init_tables(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS forecasts (
        ts REAL PRIMARY KEY, node TEXT, rain REAL, temp3 REAL, temp6 REAL,
        dp3 REAL, anomaly REAL, spread REAL, hum REAL, reasons TEXT, rain_actual INTEGER)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS reference (
        ts REAL PRIMARY KEY, ow_temp REAL, ow_hum REAL, ow_press REAL,
        ow_rain_mm REAL, ow_main TEXT, ow_clouds REAL, raining INTEGER)""")
    conn.commit()


def fetch_reference(conn):
    url = (f"https://api.openweathermap.org/data/2.5/weather?lat={LAT}&lon={LON}"
           f"&units=metric&appid={OPENWEATHER_KEY}")
    try:
        with urllib.request.urlopen(url, timeout=15) as resp:
            d = json.load(resp)
    except urllib.error.HTTPError as e:
        if e.code == 401:
            print("  OpenWeather: key not accepted yet (new keys can take up to 2 hours to activate).")
        else:
            print(f"  OpenWeather error: HTTP {e.code}")
        return
    except Exception as e:
        print(f"  OpenWeather unreachable: {e}")
        return
    wid = d["weather"][0]["id"]
    raining = 1 if 200 <= wid < 600 else 0
    conn.execute("INSERT OR REPLACE INTO reference VALUES (?,?,?,?,?,?,?,?)",
                 (time.time(), d["main"]["temp"], d["main"]["humidity"], d["main"]["pressure"],
                  d.get("rain", {}).get("1h", 0), d["weather"][0]["main"],
                  d.get("clouds", {}).get("all"), raining))
    conn.commit()
    print(f"  OpenWeather: {d['weather'][0]['main']}, {d['main']['temp']:.1f} C, raining={bool(raining)}")


def score_old_forecasts(conn):
    horizon = HORIZON_H * 3600
    due = conn.execute("SELECT ts FROM forecasts WHERE rain_actual IS NULL AND ts <= ?",
                       (time.time() - horizon,)).fetchall()
    for (ts,) in due:
        refs = conn.execute("SELECT raining FROM reference WHERE ts > ? AND ts <= ?",
                            (ts, ts + horizon)).fetchall()
        if len(refs) >= 10:     # need reasonable coverage of the 6 hours
            conn.execute("UPDATE forecasts SET rain_actual=? WHERE ts=?",
                         (1 if any(r[0] for r in refs) else 0, ts))
    conn.commit()
    rows = conn.execute("SELECT rain, rain_actual FROM forecasts WHERE rain_actual IS NOT NULL").fetchall()
    tp = sum(1 for r, a in rows if r >= RAIN_THRESHOLD and a)
    fn = sum(1 for r, a in rows if r < RAIN_THRESHOLD and a)
    fp = sum(1 for r, a in rows if r >= RAIN_THRESHOLD and not a)
    if rows:
        print(f"  Score: {len(rows)} forecasts checked | rain events caught {tp}/{tp + fn} | false alarms {fp}")


def publish_to_board(f):
    import paho.mqtt.publish as publish
    try:
        publish.multiple([
            {"topic": "weather/forecast/rain", "payload": str(f["rain"]), "retain": True},
            {"topic": "weather/forecast/temp", "payload": f"{f['temp3']:.1f}", "retain": True},
        ], hostname=MQTT_HOST, port=MQTT_PORT)
    except Exception as e:
        print(f"  MQTT publish failed: {e}")


def run_once():
    conn = sqlite3.connect(DB_PATH, timeout=15)
    try:
        init_tables(conn)
        stamp = datetime.now().strftime("%H:%M:%S")
        f, problem = make_forecast(load_rows(conn))
        if f is None:
            print(f"[{stamp}] No forecast: {problem}")
        else:
            conn.execute("INSERT OR REPLACE INTO forecasts VALUES (?,?,?,?,?,?,?,?,?,?,NULL)",
                         (f["ts"], NODE_ID, f["rain"], f["temp3"], f["temp6"], f["dp3"],
                          f["anomaly"], f["spread"], f["hum"], f["reasons"]))
            conn.commit()
            publish_to_board(f)
            print(f"[{stamp}] Rain {f['rain']}% | +3h {f['temp3']} C | +6h {f['temp6']} C "
                  f"({f['source']} baseline) - {f['reasons']}")
        fetch_reference(conn)
        score_old_forecasts(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    if not os.path.exists(DB_PATH):
        raise SystemExit(f"Cannot find {DB_PATH}. Put forecaster.py in the same folder as weather.db")
    print(f"Forecaster running (every {RUN_EVERY // 60} min). Press Ctrl+C to stop.")
    while True:
        try:
            run_once()
        except Exception as e:
            print(f"Error: {e}")
        time.sleep(RUN_EVERY)
