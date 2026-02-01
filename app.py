from flask import Flask, request, jsonify
import os
import requests

app = Flask(__name__)

# =========================
# CONFIG
# =========================
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "contact-solution-token")

# Para enviar mensagem via WhatsApp Cloud API (quando integrar):
WHATSAPP_TOKEN = os.getenv("WHATSAPP_TOKEN")          # Permanent/Temporary Token
PHONE_NUMBER_ID = os.getenv("PHONE_NUMBER_ID")        # Phone Number ID (Cloud API)
ENABLE_WHATSAPP_SEND = os.getenv("ENABLE_WHATSAPP_SEND", "0") == "1"


# =========================
# SESSÕES (MVP EM MEMÓRIA)
# =========================
SESSIONS = {}  # { "5511...": {"step": "START", "data": {}} }

def get_session(phone: str) -> dict:
    """Obtém (ou cria) uma sessão simples em memória."""
    if not phone:
        phone = "desconhecido"

    if phone not in SESSIONS:
        SESSIONS[phone] = {"step": "START", "data": {}}

    return SESSIONS[phone]


def reset_session(phone: str) -> None:
    """Reseta a sessão do usuário."""
    if not phone:
        return
    SESSIONS[phone] = {"step": "START", "data": {}}


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
    """
    Verificação do webhook pelo Meta:
    GET /webhook?hub.mode=subscribe&hub.verify_token=...&hub.challenge=...
    """
    mode = request.args.get("hub.mode")
    token = request.args.get("hub.verify_token")
    challenge = request.args.get("hub.challenge")

    if mode == "subscribe" and token == VERIFY_TOKEN:
        return challenge, 200

    return "Forbidden", 403


# =========================
# HELPERS
# =========================
def extract_whatsapp_message(payload: dict) -> tuple[str, str]:
    """
    Extrai telefone e texto do payload.
    Suporta:
      - Payload real da WhatsApp Cloud API (entry -> changes -> value -> messages[0])
      - Payload mock do Postman que você está usando
    Retorna: (phone, text)
    """
    try:
        # Padrão Meta Cloud API
        entry = (payload.get("entry") or [])[0]
        changes = (entry.get("changes") or [])[0]
        value = changes.get("value") or {}

        messages = value.get("messages") or []
        if not messages:
            # Pode ser evento sem mensagem (status, delivered, etc.)
            return "desconhecido", ""

        msg = messages[0]

        # "from" costuma vir como wa_id/telefone
        phone = msg.get("from") or "desconhecido"

        # Texto normal
        text_obj = msg.get("text") or {}
        text = text_obj.get("body") or ""

        # Normalização
        text = str(text).strip().lower()

        return phone, text

    except Exception as e:
        print("Erro ao extrair mensagem (padrão Meta):", e)

    # Fallback extra (caso payload venha em outro formato)
    try:
        phone = payload.get("from") or "desconhecido"
        text = payload.get("text") or ""
        text = str(text).strip().lower()
        return phone, text
    except Exception as e:
        print("Erro ao extrair mensagem (fallback):", e)

    return "desconhecido", ""


def decide_reply(text: str, session: dict) -> str:
    """
    Lógica do bot (MVP) usando step de sessão.
    (Fácil de evoluir depois)
    """
    if not text:
        return "Não recebi texto. Digite *oi* para começar. 🙂"

    step = session.get("step", "START")

    # Você pode sofisticar o fluxo por step (menu, vendas, suporte...)
    # Por enquanto, simples:

    if "oi" in text or "olá" in text or "ola" in text:
        session["step"] = "MENU"
        return (
            "Olá! 👋\n"
            "Sou o atendimento automático 🤖\n\n"
            "Digite:\n"
            "1️⃣ para Vendas\n"
            "2️⃣ para Suporte"
        )

    if step == "MENU":
        if text == "1":
            session["step"] = "VENDAS"
            return "Perfeito! 👍 Vou te encaminhar para o setor de Vendas."
        if text == "2":
            session["step"] = "SUPORTE"
            return "Certo! 🛠️ Vou te encaminhar para o Suporte."

        return "Opção inválida. Digite 1 para Vendas ou 2 para Suporte."

    # Se chegou aqui, não entendeu (ou usuário fora do fluxo)
    return (
        "Não entendi sua mensagem 😅\n"
        "Digite *oi* para começar o atendimento."
    )


def send_whatsapp_message(to_phone: str, message_text: str) -> bool:
    """
    Envia mensagem via WhatsApp Cloud API.
    Só funciona se:
      - ENABLE_WHATSAPP_SEND=1
      - WHATSAPP_TOKEN e PHONE_NUMBER_ID configurados
    """
    if not ENABLE_WHATSAPP_SEND:
        print("Envio desativado (ENABLE_WHATSAPP_SEND != 1).")
        return False

    if not WHATSAPP_TOKEN or not PHONE_NUMBER_ID:
        print("Envio desativado: WHATSAPP_TOKEN ou PHONE_NUMBER_ID ausente.")
        return False

    url = f"https://graph.facebook.com/v20.0/{PHONE_NUMBER_ID}/messages"
    headers = {
        "Authorization": f"Bearer {WHATSAPP_TOKEN}",
        "Content-Type": "application/json",
    }
    data = {
        "messaging_product": "whatsapp",
        "to": to_phone,
        "type": "text",
        "text": {"body": message_text},
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
    reply = decide_reply(text, session)
    print("Resposta gerada:", reply)

    # Envio real (quando integrar): ativar ENABLE_WHATSAPP_SEND=1
    # send_whatsapp_message(phone, reply)

    # Responde 200 pra Meta/Postman
    return jsonify(status="ok"), 200


# =========================
# START LOCAL
# =========================
if __name__ == "__main__":
    # Render usa a env PORT automaticamente
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
