import os, json, time, random, ssl
import paho.mqtt.client as mqtt
from dotenv import load_dotenv
load_dotenv()

HOST = os.getenv("MQTT_HOST")
PORT = int(os.getenv("MQTT_PORT","8883"))
USER = os.getenv("MQTT_USER") or None
PASS = os.getenv("MQTT_PASS") or None
TOPIC= os.getenv("MQTT_TOPIC","copel/teste/temperatura")
CA   = os.getenv("MQTT_CAFILE") or None

c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="pub-sim")
if USER: c.username_pw_set(USER, PASS)
if CA:
    c.tls_set(ca_certs=CA, tls_version=ssl.PROTOCOL_TLS_CLIENT)
    c.tls_insecure_set(False)
elif PORT == 8883:
    c.tls_set(tls_version=ssl.PROTOCOL_TLS_CLIENT)

c.connect(HOST, PORT, keepalive=30)

while True:
    msg = {"v":1, "temp_c": round(random.uniform(62, 86),1), "umid_pct": round(random.uniform(25,55),1), "rssi": -60}
    print("[PUB]", msg)
    c.publish(TOPIC, json.dumps(msg), qos=0, retain=False)
    time.sleep(3)
