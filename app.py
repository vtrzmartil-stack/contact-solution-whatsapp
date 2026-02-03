from flask import Flask, request, jsonify
import os
import requests

app = Flask(__name__)

# ==================================================
# ESTADO EM MEMÓRIA (MVP)
# ==================================================
SESSIONS = {}

def get_session(phone: str) -> dict:
    if phone not in SESSIONS:
        SESSIONS[phone] = {
            "step": "START",
            "data": {}
        }
    return SESSIONS[phone]

def reset_session(phone: str):
    SESSIONS[phone] = {
        "step": "START",
        "data": {}
    }

# ==================================================
# CONFIG
# ==================================================
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "contact-solution-token")

WHATSAPP_TOKEN = os.getenv("WHATSAPP_TOKEN")
PHONE_NUMBER_ID = os.getenv("PHONE_NUMBER_ID")

# ==================================================
# ROTAS BÁSICAS
# ==================================================
@app.get("/")
def home():
    return "ok", 200

@app.get("/health")
def health():
    return jsonify(status="ok"), 200

# ==================================================
# WEBHOOK - VERIFICAÇÃO (META)
# ==================================================
@app.get("/webhook")
def verify():
    mode = request.args.get("hub.mode")
    token = request.args.get("hub.verify_token")
    challenge = request.args.get("hub.challenge")

    if mode == "subscribe" and token == VERIFY_TOKEN:
        return challenge, 200

    return "Forbidden", 403

# ==================================================
# HELPERS
# ==================================================
def extract_whatsapp_message(payload: dict):
    """
    Retorna (phone, text)
    """
    try:
        entry = (payload.get("entry") or [])[0]
        changes = (entry.get("changes") or [])[0]
        value = changes.get("value") or {}

        messages = value.get("messages") or []
        if not messages:
            return "desconhecido", ""

        msg = messages[0]

        phone = msg.get("from", "desconhecido")
        text = (msg.get("text") or {}).get("body", "")

        text = str(text).strip().lower()

        return phone, text

    except Exception as e:
        print("Erro ao extrair mensagem:", e)
        return "desconhecido", ""

def decide_reply(step: str, text: str, data: dict):
    """
    Retorna (reply, next_step)
    """
    if not text:
        return "Não recebi texto. Digite *oi* para começar 😊", step

    # INÍCIO
    if step == "START":
        if "oi" in text or "olá" in text or "ola" in text:
            return (
                "Olá! 👋\n"
                "Sou o atendimento automático 🤖\n\n"
                "Digite:\n"
                "1️⃣ para Vendas\n"
                "2️⃣ para Suporte",
                "MENU"
            )
        return "Digite *oi* para começar o atendimento.", "START"

    # MENU
    if step == "MENU":
        if text == "1":
            return "Perfeito! 👍 Vou te encaminhar para o setor de Vendas.", "VENDAS"
        if text == "2":
            return "Certo! 🛠️ Vou te encaminhar para o Suporte.", "SUPORTE"
        return "Opção inválida. Digite 1 para Vendas ou 2 para Suporte.", "MENU"

    # FALLBACK
    return "Não entendi sua mensagem. Digite *oi* para recomeçar.", "START"

# ==================================================
# ENVIO WHATSAPP (DESATIVADO NO MVP)
# ==================================================
def send_whatsapp_message(to_phone: str, message_text: str) -> bool:
    if not WHATSAPP_TOKEN or not PHONE_NUMBER_ID:
        print("Envio desativado (token ou phone id ausente)")
        return False

    url = f"https://graph.facebook.com/v20.0/{PHONE_NUMBER_ID}/messages"
    headers = {
        "Authorization": f"Bearer {WHATSAPP_TOKEN}",
        "Content-Type": "application/json"
    }
    data = {
        "messaging_product": "whatsapp",
        "to": to_phone,
        "type": "text",
        "text": {"body": message_text}
    }

    try:
        r = requests.post(url, headers=headers, json=data, timeout=15)
        print("WhatsApp status:", r.status_code, r.text)
        return 200 <= r.status_code < 300
    except Exception as e:
        print("Erro ao enviar WhatsApp:", e)
        return False

# ==================================================
# WEBHOOK - RECEBIMENTO (POST)
# ==================================================
@app.post("/webhook")
def webhook():
    payload = request.get_json(silent=True) or {}
    print("Payload recebido:", payload)

    phone, text = extract_whatsapp_message(payload)

    session = get_session(phone)
    step = session["step"]
    data = session["data"]

    reply, next_step = decide_reply(step, text, data)

    session["step"] = next_step

    print("Telefone:", phone)
    print("Mensagem:", text)
    print("Step:", step, "->", next_step)
    print("Resposta:", reply)

    # Envio real (quando integrar)
    # send_whatsapp_message(phone, reply)

    return jsonify(status="ok"), 200

# ==================================================
# START LOCAL / RENDER
# ==================================================
if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
