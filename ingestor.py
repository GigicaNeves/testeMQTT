import os, ssl, json
import paho.mqtt.client as mqtt
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

# MQTT
MQTT_HOST   = os.getenv("MQTT_HOST")
MQTT_PORT   = int(os.getenv("MQTT_PORT", "8883"))
MQTT_USER   = os.getenv("MQTT_USER") or None
MQTT_PASS   = os.getenv("MQTT_PASS") or None
MQTT_TOPIC  = os.getenv("MQTT_TOPIC", "copel/teste/temperatura")
MQTT_CAFILE = os.getenv("MQTT_CAFILE") or None

# Supabase (service role para INSERT)
SUPABASE_URL = os.getenv("SUPABASE_URL")
SERVICE_ROLE = os.getenv("SUPABASE_SERVICE_ROLE")
sb = create_client(SUPABASE_URL, SERVICE_ROLE)

def on_connect(c, u, f, rc, p=None):
    if rc == 0:
        print("✅ Ingestor conectado ao broker")
        c.subscribe(MQTT_TOPIC, qos=1)
        print("📡 Assinado:", MQTT_TOPIC)
    else:
        print("❌ Falha ao conectar rc=", rc)

def on_message(c, u, msg):
    payload = msg.payload.decode("utf-8", errors="replace")
    print(f"[MQTT] {msg.topic}: {payload}")
    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        data = {"raw": payload}

    # Exemplo simples: device_id fixo; adapte para extrair do tópico
    record = {
        "device_id": "simulador",
        "temp_c": data.get("temp_c"),
        "umid_pct": data.get("umid_pct"),
        "rssi": data.get("rssi"),
        "raw": data
    }

    try:
        sb.table("measurements").insert(record).execute()
    except Exception as e:
        print("❌ Erro ao inserir:", e)

def main():
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="ingestor")
    if MQTT_USER: c.username_pw_set(MQTT_USER, MQTT_PASS)
    if MQTT_CAFILE:
        c.tls_set(ca_certs=MQTT_CAFILE, tls_version=ssl.PROTOCOL_TLS_CLIENT)
        c.tls_insecure_set(False)
    elif MQTT_PORT == 8883:
        c.tls_set(tls_version=ssl.PROTOCOL_TLS_CLIENT)

    c.on_connect = on_connect
    c.on_message = on_message
    c.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    c.loop_forever()

if __name__ == "__main__":
    main()
