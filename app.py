from flask import Flask, request, jsonify
import os
import requests

app = Flask(__name__)
# ====== ESTADO EM MEMÓRIA (simples) ======
SESSIONS = {}  # { "5511...": {"step": "MENU", "data": {...}} }

def get_session(phone: str) -> dict:
    if phone not in SESSIONS:
        SESSIONS[phone] = {"step": "START", "data": {}}
    return SESSIONS[phone]

def reset_session(phone: str):
    SESSIONS[phone] = {"step": "START", "data": {}}

# =========================
# CONFIG
# =========================
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "contact-solution-token")

# Para enviar mensagem de volta via WhatsApp Cloud API (quando integrar):
WHATSAPP_TOKEN = os.getenv("WHATSAPP_TOKEN")           # Token do WhatsApp Cloud API
PHONE_NUMBER_ID = os.getenv("PHONE_NUMBER_ID")         # Phone Number ID do WhatsApp Cloud API

# =========================
# ROTAS BÁSICAS
# =========================
@app.get("/")
def home():
    return "ok", 200


@app.get("/health")
def health():
    return jsonify(status="ok"), 200


# =========================
# WEBHOOK - VERIFICAÇÃO (META)
# =========================
@app.get("/webhook")
def verify():
    mode = request.args.get("hub.mode")
    token = request.args.get("hub.verify_token")
    challenge = request.args.get("hub.challenge")

    # A Meta chama com hub.mode=subscribe
    if mode == "subscribe" and token == VERIFY_TOKEN:
        return challenge, 200

    return "Forbidden", 403


# =========================
# HELPERS
# =========================
def extract_whatsapp_message(payload: dict):
    """
    Tenta extrair:
      - phone (wa_id ou from)
      - text (mensagem)
    Retorna (phone, text)
    Se não achar, retorna ("desconhecido", "")
    """
    try:
        entry = (payload.get("entry") or [])[0]
        changes = (entry.get("changes") or [])[0]
        value = changes.get("value") or {}

        messages = value.get("messages") or []
        if not messages:
            return "desconhecido", ""

        msg = messages[0]

        # "from" costuma ser o wa_id (ex: 55119....)
        phone = msg.get("from") or "desconhecido"

        # Texto normal
        text_obj = msg.get("text") or {}
        text = text_obj.get("body") or ""

        # Normalização
        text = str(text).strip().lower()

        return phone, text
    except Exception as e:
        print("Erro ao extrair mensagem:", e)
        return "desconhecido", ""


def decide_reply(text: str) -> str:
    """
    Lógica simples de atendimento (MVP).
    """
    if not text:
        return "Não recebi texto. Digite *oi* para começar. 🙂"

    if "oi" in text or "olá" in text or "ola" in text:
        return (
            "Olá! 👋\n"
            "Sou o atendimento automático 🤖\n\n"
            "Digite:\n"
            "1️⃣ para Vendas\n"
            "2️⃣ para Suporte"
        )

    if text == "1":
        return "Perfeito! 👍 Vou te encaminhar para o setor de Vendas."

    if text == "2":
        return "Certo! 🛠️ Vou te encaminhar para o Suporte."

    return (
        "Não entendi sua mensagem 😅\n"
        "Digite *oi* para começar o atendimento."
    )


def send_whatsapp_message(to_phone: str, message_text: str) -> bool:
    """
    Envia mensagem via WhatsApp Cloud API.
    Só funciona quando WHATSAPP_TOKEN e PHONE_NUMBER_ID estiverem configurados.
    """
    if not WHATSAPP_TOKEN or not PHONE_NUMBER_ID:
        print("Envio desativado: WHATSAPP_TOKEN ou PHONE_NUMBER_ID ausente.")
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
        print("WhatsApp send status:", r.status_code, r.text)
        return 200 <= r.status_code < 300
    except Exception as e:
        print("Erro ao enviar WhatsApp:", e)
        return False


# =========================
# WEBHOOK - RECEBIMENTO (POST)
# =========================
@app.post("/webhook")
def webhook():
    payload = request.get_json(silent=True) or {}
    print("Payload recebido:", payload)

    phone, text = extract_whatsapp_message(payload)

    print("Telefone:", phone)
    print("Mensagem:", text)

    session = get_session(phone)
    step = session["step"]

    reply = decide_reply(text)
    print("Resposta gerada:", reply)

    # MVP: pode deixar SEM enviar (só loga). Quando integrar, habilite.
    # Se quiser já testar envio real quando tiver token/number id, deixe ligado:
    # send_whatsapp_message(phone, reply)

    return jsonify(status="ok"), 200


# =========================
# START LOCAL (opcional)
# =========================
if __name__ == "__main__":
    # Para rodar localmente: python app.py
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
