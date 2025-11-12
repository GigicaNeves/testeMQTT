from flask import Flask, jsonify, request
import paho.mqtt.client as mqtt
import threading, ssl, os, json, time
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

# Supabase (opcional para endpoint de histórico)
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_ANON = os.getenv("SUPABASE_ANON")  # use anon só para SELECT
sb = create_client(SUPABASE_URL, SUPABASE_ANON) if SUPABASE_URL and SUPABASE_ANON else None

app = Flask(__name__)

estado = {"temperatura": None, "ts": None, "raw": None}
lock = threading.Lock()

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print("✅ Conectado ao broker")
        client.subscribe(MQTT_TOPIC, qos=1)
        print("📡 Assinado:", MQTT_TOPIC)
    else:
        print("❌ Falha ao conectar. RC=", rc)

def on_message(client, userdata, msg):
    payload = msg.payload.decode("utf-8", errors="replace")
    print(f"[MQTT] {msg.topic}: {payload}")
    try:
        data = json.loads(payload)
        valor = data.get("temp_c") or data.get("temperatura") or payload
    except json.JSONDecodeError:
        data, valor = None, payload
    with lock:
        estado["temperatura"] = valor
        estado["ts"] = time.time()
        estado["raw"] = data if data is not None else payload

def mqtt_loop():
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="server-sub")
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

@app.get("/")
def index():
    return """<!doctype html>
<html><head><meta charset="utf-8"><title>Hello IoT</title></head>
<body>
  <h1>Hello IoT</h1>
  <p><b>Última leitura:</b> <span id="v">---</span></p>
  <p><small>Atualiza a cada 2 s</small></p>
  <script>
    async function tick(){
      try{
        const r = await fetch('/api/ultimo');
        const d = await r.json();
        document.getElementById('v').textContent =
          (d.temperatura ?? '---') + (d.ts ? ' ('+new Date(d.ts*1000).toLocaleTimeString()+')' : '');
      }catch(e){ document.getElementById('v').textContent = 'erro'; }
    }
    tick(); setInterval(tick, 2000);
  </script>
</body></html>"""

@app.get("/api/ultimo")
def api_ultimo():
    with lock:
        return jsonify(estado)

# (Opcional) histórico via Supabase
@app.get("/api/ultimas")
def api_ultimas():
    if not sb: return jsonify({"error":"Supabase não configurado"}), 500
    limit = int(request.args.get("limit", "200"))
    resp = sb.table("measurements").select("device_id,ts,temp_c,umid_pct,rssi").order("ts", desc=True).limit(limit).execute()
    return jsonify(resp.data)

if __name__ == "__main__":
    # Evita múltiplas threads no modo debug reloader:
    if os.environ.get("WERKZEUG_RUN_MAIN") == "true":
        threading.Thread(target=mqtt_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=5000, debug=True)
