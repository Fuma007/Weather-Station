#include <ESP8266WiFi.h>
#include <PubSubClient.h>
#include <Wire.h>
#include <LiquidCrystal_I2C.h>
#include <Adafruit_BME280.h>

// ================== SETTINGS: EDIT THESE ==================
const char* WIFI_SSID   = "Your-SSID-WIFI";
const char* WIFI_PASS   = "Your-Wifi-Password";
const char* MQTT_HOST   = "Your-MQTT-Host";
const uint16_t MQTT_PORT = 1883; // Your MQTT Port
const char* NODE_ID     = "node1";

const float ALTITUDE_M   = 310.0;
const float TEMP_OFFSET  = 0.0;
const float HUM_OFFSET   = 0.0;
const float PRESS_OFFSET = 0.0;

const uint8_t LCD_ADDR = 0x27;

const unsigned long READ_INTERVAL    = 5000UL;
const unsigned long PUBLISH_INTERVAL = 60000UL;
const unsigned long PAGE_INTERVAL    = 4000UL;
// ===========================================================

#define PIN_SDA D6
#define PIN_SCL D5

LiquidCrystal_I2C lcd(LCD_ADDR, 16, 2);
Adafruit_BME280 bme;
WiFiClient wifiClient;
PubSubClient mqtt(wifiClient);

char topicData[48];
char topicStatus[48];
char topicCmd[48];     // รับคำสั่ง on/off จากแดชบอร์ด
char topicPower[48];   // รายงานสถานะ on/off กลับไป
char topicRestart[48]; // รับคำสั่งรีสตาร์ท (ไม่ retained)

float curTemp = NAN, curHum = NAN, curPress = NAN, curSea = NAN, curDew = NAN;

double sumT = 0, sumH = 0, sumP = 0;
int sampleCount = 0;

int   fcRain = -1;
float fcTemp = NAN;
unsigned long lastForecastMs = 0;

unsigned long lastRead = 0, lastPublish = 0, lastDraw = 0, lastMqttTry = 0;
bool bmeOk = false;
bool powerOn = true;   // false = โหมดปิด: จอดับ หยุดวัด หยุดส่งข้อมูล (ยังต่อ Wi-Fi/MQTT รอคำสั่งเปิด)

float dewPoint(float t, float rh) {
  const float a = 17.62, b = 243.12;
  float g = log(rh / 100.0) + (a * t) / (b + t);
  return (b * g) / (a - g);
}

float seaLevel(float p) {
  return p / pow(1.0 - (ALTITUDE_M / 44330.0), 5.255);
}

void lcdLine(uint8_t row, const char* text) {
  char buf[20];
  snprintf(buf, sizeof(buf), "%-16.16s", text);
  lcd.setCursor(0, row);
  lcd.print(buf);
}

void setPower(bool on) {
  powerOn = on;
  if (on) {
    sumT = sumH = sumP = 0;
    sampleCount = 0;
    lastRead = 0;
    lastDraw = 0;
    lastPublish = millis();
    lcd.backlight();
  } else {
    lcd.clear();
    lcd.noBacklight();
  }
  if (mqtt.connected()) mqtt.publish(topicPower, on ? "on" : "off", true);
  Serial.println(on ? "Power: ON" : "Power: OFF");
}

void onMessage(char* topic, byte* payload, unsigned int len) {
  char buf[24];
  if (len >= sizeof(buf)) len = sizeof(buf) - 1;
  memcpy(buf, payload, len);
  buf[len] = '\0';

  if (strcmp(topic, topicRestart) == 0) {
    if (strcmp(buf, "restart") == 0) {
      lcd.backlight();
      lcdLine(0, "Restarting...");
      lcdLine(1, "");
      delay(300);
      ESP.restart();
    }
  } else if (strcmp(topic, topicCmd) == 0) {
    if (strcmp(buf, "off") == 0 && powerOn) setPower(false);
    else if (strcmp(buf, "on") == 0 && !powerOn) setPower(true);
    else mqtt.publish(topicPower, powerOn ? "on" : "off", true);
  } else if (strcmp(topic, "weather/forecast/rain") == 0) {
    fcRain = constrain(atoi(buf), 0, 100);
    lastForecastMs = millis();
  } else if (strcmp(topic, "weather/forecast/temp") == 0) {
    fcTemp = atof(buf);
    lastForecastMs = millis();
  }
}

void connectMqtt() {
  if (WiFi.status() != WL_CONNECTED || mqtt.connected()) return;
  if (millis() - lastMqttTry < 10000UL) return;
  lastMqttTry = millis();

  String clientId = String("esp8266-") + NODE_ID;
  if (mqtt.connect(clientId.c_str(), NULL, NULL, topicStatus, 0, true, "offline")) {
    mqtt.publish(topicStatus, "online", true);
    mqtt.subscribe(topicCmd);
    mqtt.subscribe(topicRestart);
    mqtt.subscribe("weather/forecast/rain");
    mqtt.subscribe("weather/forecast/temp");
    mqtt.publish(topicPower, powerOn ? "on" : "off", true);
  }
}

void publishData() {
  if (sampleCount == 0 || !mqtt.connected()) return;

  float t = sumT / sampleCount;
  float h = sumH / sampleCount;
  float p = sumP / sampleCount;

  char msg[220];
  snprintf(msg, sizeof(msg),
           "{\"node\":\"%s\",\"temp\":%.2f,\"hum\":%.1f,\"press\":%.2f,"
           "\"sealevel\":%.2f,\"dew\":%.2f,\"rain\":0,\"rssi\":%d}",
           NODE_ID, t, h, p, seaLevel(p), dewPoint(t, h), WiFi.RSSI());

  if (mqtt.publish(topicData, msg)) {
    sumT = sumH = sumP = 0;
    sampleCount = 0;
  }
}

void readSensor() {
  float t = bme.readTemperature() + TEMP_OFFSET;
  float h = bme.readHumidity() + HUM_OFFSET;
  float p = bme.readPressure() / 100.0F + PRESS_OFFSET;

  if (isnan(t) || isnan(h) || isnan(p)) return;

  curTemp = t; curHum = h; curPress = p;
  curSea = seaLevel(p);
  curDew = dewPoint(t, h);

  sumT += t; sumH += h; sumP += p;
  sampleCount++;

  Serial.printf("T=%.1f C  H=%.0f %%  P=%.1f hPa\n", t, h, curSea);
}

void drawScreen() {
  char l0[24], l1[24];

  if (isnan(curTemp)) {
    lcdLine(0, "Reading sensor..");
    lcdLine(1, "");
    return;
  }

  if ((millis() / PAGE_INTERVAL) % 2 == 0) {
    snprintf(l0, sizeof(l0), "T:%.1fC H:%.0f%%", curTemp, curHum);
    snprintf(l1, sizeof(l1), "P:%.0fhPa D:%.0fC", curSea, curDew);
  } else {
    char r[10], t[10];
    if (fcRain >= 0) snprintf(r, sizeof(r), "%d%%", fcRain); else strcpy(r, "--");
    if (!isnan(fcTemp)) snprintf(t, sizeof(t), "%.1f", fcTemp); else strcpy(t, "--");
    snprintf(l0, sizeof(l0), "Rain:%s T:%s", r, t);
    snprintf(l1, sizeof(l1), "WiFi:%s MQTT:%s",
             WiFi.status() == WL_CONNECTED ? "OK" : "--",
             mqtt.connected() ? "OK" : "--");
  }
  lcdLine(0, l0);
  lcdLine(1, l1);
}

void setup() {
  Serial.begin(115200);
  Wire.begin(PIN_SDA, PIN_SCL);

  lcd.init();
  lcd.backlight();
  lcdLine(0, "Weather station");
  lcdLine(1, "Starting...");

  bmeOk = bme.begin(0x76) || bme.begin(0x77);
  if (!bmeOk) {
    lcdLine(0, "BME280 not found");
    lcdLine(1, "Check wiring");
    Serial.println("BME280 not found");
  }

  snprintf(topicData,   sizeof(topicData),   "weather/%s/data",   NODE_ID);
  snprintf(topicStatus, sizeof(topicStatus), "weather/%s/status", NODE_ID);
  snprintf(topicCmd,    sizeof(topicCmd),    "weather/%s/cmd",    NODE_ID);
  snprintf(topicPower,  sizeof(topicPower),  "weather/%s/power",  NODE_ID);
  snprintf(topicRestart, sizeof(topicRestart), "weather/%s/restart", NODE_ID);

  WiFi.mode(WIFI_STA);
  WiFi.disconnect();
  delay(200);

  // แสดงรายชื่อ Wi-Fi ที่บอร์ดมองเห็น เพื่อคัดลอกชื่อที่ถูกต้อง
  int n = WiFi.scanNetworks();
  Serial.printf("\nFound %d networks\n", n);
  for (int i = 0; i < n; i++) {
    Serial.printf("%d: [%s]  ch %d  %d dBm\n",
                  i + 1, WiFi.SSID(i).c_str(), WiFi.channel(i), WiFi.RSSI(i));
  }

  WiFi.setAutoReconnect(true);
  WiFi.begin(WIFI_SSID, WIFI_PASS);

  mqtt.setServer(MQTT_HOST, MQTT_PORT);
  mqtt.setCallback(onMessage);
}

void loop() {
  if (!bmeOk) {
    delay(2000);
    bmeOk = bme.begin(0x76) || bme.begin(0x77);
    return;
  }

  unsigned long now = millis();

  connectMqtt();
  if (mqtt.connected()) mqtt.loop();

  if (!powerOn) {          // โหมดปิด: รอคำสั่งเปิดอย่างเดียว
    delay(50);
    return;
  }

  if (now - lastRead >= READ_INTERVAL || lastRead == 0) {
    lastRead = now;
    readSensor();
  }

  if (now - lastPublish >= PUBLISH_INTERVAL) {
    lastPublish = now;
    publishData();
  }

  if (now - lastDraw >= 500UL) {
    lastDraw = now;
    drawScreen();
  }

  if (fcRain >= 0 && now - lastForecastMs > 3UL * 3600UL * 1000UL) {
    fcRain = -1;
    fcTemp = NAN;
  }
}
