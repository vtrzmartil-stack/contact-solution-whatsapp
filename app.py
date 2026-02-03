from flask import Flask, request, jsonify
import os
import json
import time
import hashlib
import requests

# Redis é opcional: se REDIS_URL existir, usa Redis; senão, usa memória (fallback)
try:
    import redis  # pip install redis
except Exception:
    redis = None

app = Flask(__name__)

# =========================
# CONFIG
# =========================
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "contact-solution-token")

WHATSAPP_TOKEN = os.getenv("WHATSAPP_TOKEN")          # Token do WhatsApp Cloud API
PHONE_NUMBER_ID = os.getenv("PHONE_NUMBER_ID")        # Phone Number ID do WhatsApp Cloud API

REDIS_URL = os.getenv("REDIS_URL")                    # ex: redis://...
SESSION_TTL_SECONDS = int(os.getenv("SESSION_TTL_SECONDS", "3600"))          # 1h
IDEMPOTENCY_TTL_SECONDS = int(os.getenv("IDEMPOTENCY_TTL_SECONDS", "86400")) # 24h

# =========================
# REDIS / FALLBACK
# =========================
rdb = None
if REDIS_URL and redis is not None:
    try:
        rdb = redis.from_url(REDIS_URL, decode_responses=True)
        rdb.ping()
        print("Redis: conectado ✅")
    except Exception as e:
        print("Redis: falhou, usando memória (fallback). Erro:", e)
        rdb = None
else:
    if REDIS_URL and redis is None:
        print("Redis: REDIS_URL existe mas 'redis' não instalado. Usando memória (fallback).")
    else:
        print("Redis: REDIS_URL não configurado. Usando memória (fallback).")

# fallback em memória (não escala entre instâncias)
MEM_SESSIONS = {}
MEM_IDEMPOTENCY = set()

# =========================
# HELPERS - SESSÃO (ESTADO)
# =========================
def _session_key(phone: str) -> str:
    return f"session:{phone}"

def get_session(phone: str) -> dict:
    """
    Retorna a sessão do telefone:
    {"step": "START" | "MENU" | "VENDAS" | "SUPORTE" | ..., "data": {...}}
    """
    if not phone:
        phone = "desconhecido"

    if rdb:
        key = _session_key(phone)
        raw = rdb.get(key)
        if raw:
            try:
                return json.loads(raw)
            except Exception:
                # se corromper, reseta
                session = {"step": "START", "data": {}}
                rdb.setex(key, SESSION_TTL_SECONDS, json.dumps(session, ensure_ascii=False))
                return session
        else:
            session = {"step": "START", "data": {}}
            rdb.setex(key, SESSION_TTL_SECONDS, json.dumps(session, ensure_ascii=False))
            return session

    # fallback memória
    if phone not in MEM_SESSIONS:
        MEM_SESSIONS[phone] = {"step": "START", "data": {}}
    return MEM_SESSIONS[phone]

def save_session(phone: str, session: dict) -> None:
    if not phone:
        phone = "desconhecido"

    if rdb:
        key = _session_key(phone)
        rdb.setex(key, SESSION_TTL_SECONDS, json.dumps(session, ensure_ascii=False))
        return

    MEM_SESSIONS[phone] = session

def reset_session(phone: str) -> None:
    session = {"step": "START", "data": {}}
    save_session(phone, session)

# =========================
# HELPERS - IDEMPOTÊNCIA
# =========================
def _idempotency_key(message_id: str) -> str:
    return f"idem:{message_id}"

def already_processed(message_id: str) -> bool:
    """
    True se já processamos esse message_id.
    Implementação:
      - Redis: SETNX com TTL
      - Memória: set()
    """
    if not message_id:
        return False

    if rdb:
        key = _idempotency_key(message_id)
        # setnx: só grava se não existe
        was_set = rdb.set(key, "1", nx=True, ex=IDEMPOTENCY_TTL_SECONDS)
        # was_set == True => primeira vez
        return was_set is None  # se não setou, já existia => já processado

    # fallback memória
    if message_id in MEM_IDEMPOTENCY:
        return True
    MEM_IDEMPOTENCY.add(message_id)
    return False

# =========================
# EXTRAÇÃO DE MENSAGEM (WhatsApp Cloud payload)
# =========================
def extract_whatsapp_message(payload: dict):
    """
    Retorna: (phone, text, message_id)

    Observação:
    - No WhatsApp Cloud API, normalmente existe msg["id"].
    - Em testes manuais (Postman), pode não existir. Nesse caso criamos um hash.
    """
    try:
        entry = (payload.get("entry") or [])[0]
        changes = (entry.get("changes") or [])[0]
        value = changes.get("value") or {}
        messages = value.get("messages") or []
        if not messages:
            return "desconhecido", "", ""

        msg = messages[0]
        phone = msg.get("from") or "desconhecido"

        # texto normal
        text_obj = msg.get("text") or {}
        text = text_obj.get("body") or ""

        # id do WhatsApp (quando vem da Meta)
        msg_id = msg.get("id") or ""

        # fallback: se não veio msg_id (ex: Postman), gera um id determinístico
        if not msg_id:
            # tenta usar timestamp se vier
            ts = msg.get("timestamp") or str(int(time.time()))
            base = f"{phone}|{text}|{ts}"
            msg_id = hashlib.sha256(base.encode("utf-8")).hexdigest()

        text = str(text).strip().lower()
        return phone, text, msg_id

    except Exception as e:
        print("Erro ao extrair mensagem:", e)
        return "desconhecido", "", ""

# =========================
# LÓGICA DO FLUXO (ESTADOS)
# =========================
def decide_reply(step: str, text: str, data: dict):
    """
    Retorna (reply_text, next_step, data_atualizado)
    """
    if data is None:
        data = {}

    # comandos globais
    if text in ("reiniciar", "reset", "menu"):
        data = {}
        return (
            "Beleza! Vamos reiniciar.\nDigite *oi* para começar. 🙂",
            "START",
            data,
        )

    if step == "START":
        if not text:
            return ("Não recebi texto. Digite *oi* para começar. 🙂", "START", data)

        if "oi" in text or "olá" in text or "ola" in text:
            return (
                "Olá! 👋\n"
                "Sou o atendimento automático 🤖\n\n"
                "Digite:\n"
                "1️⃣ para Vendas\n"
                "2️⃣ para Suporte",
                "MENU",
                data,
            )

        return ("Digite *oi* para começar o atendimento. 🙂", "START", data)

    if step == "MENU":
        if text == "1":
            return (
                "Perfeito! 👍 Vou te encaminhar para o setor de Vendas.\n"
                "Antes, me diga seu *nome*:",
                "LEAD_NOME",
                data,
            )

        if text == "2":
            return (
                "Certo! 🛠️ Vou te encaminhar para o Suporte.\n"
                "Antes, me diga seu *nome*:",
                "LEAD_NOME",
                data,
            )

        return ("Não entendi. Digite 1️⃣ para Vendas ou 2️⃣ para Suporte.", "MENU", data)

    # Coleta de lead (simples e escalável)
    if step == "LEAD_NOME":
        if not text:
            return ("Por favor, me diga seu *nome* 🙂", "LEAD_NOME", data)

        data["nome"] = text.title()
        return (
            f"Obrigado, {data['nome']}! ✅\nAgora me diga seu *e-mail* (se não tiver, digite 'pular'):",
            "LEAD_EMAIL",
            data,
        )

    if step == "LEAD_EMAIL":
        if not text:
            return ("Me diga seu *e-mail* (ou 'pular'):", "LEAD_EMAIL", data)

        if text != "pular":
            data["email"] = text

        return (
            "Perfeito.\nAgora descreva em 1 frase o que você precisa (ex: 'quero orçamento', 'meu sistema caiu'):",
            "LEAD_NEED",
            data,
        )

    if step == "LEAD_NEED":
        if not text:
            return ("Me diga rapidamente o que você precisa 🙂", "LEAD_NEED", data)

        data["necessidade"] = text

        # Decide pra onde vai baseado na escolha anterior (guardamos quando veio do MENU)
        # Como simplificação: se a pessoa caiu aqui, ela veio de MENU->LEAD_NOME, mas podemos inferir:
        # Para manter simples, vamos perguntar se foi vendas ou suporte antes de finalizar.
        return (
            "Show! ✅\nSó pra confirmar:\n"
            "1️⃣ Vendas\n"
            "2️⃣ Suporte",
            "CONFIRMA_SETOR",
            data,
        )

    if step == "CONFIRMA_SETOR":
        if text == "1":
            data["setor"] = "vendas"
            return (
                "Pronto! Registrei seus dados e vou encaminhar para *Vendas*.\n"
                "Em breve alguém te chama por aqui. ✅",
                "FINAL",
                data,
            )
        if text == "2":
            data["setor"] = "suporte"
            return (
                "Pronto! Registrei seus dados e vou encaminhar para *Suporte*.\n"
                "Em breve alguém te chama por aqui. ✅",
                "FINAL",
                data,
            )
        return ("Digite 1️⃣ para Vendas ou 2️⃣ para Suporte.", "CONFIRMA_SETOR", data)

    if step == "FINAL":
        # Mantém a conversa “fechada” até o user pedir menu/reiniciar
        return (
            "Já registrei seu atendimento ✅\n"
            "Se quiser começar de novo, digite *menu* ou *reiniciar*.",
            "FINAL",
            data,
        )

    # fallback geral
    return ("Digite *oi* para começar 🙂", "START", {})

# =========================
# ENVIO VIA WHATSAPP CLOUD API (quando integrar)
# =========================
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

    if mode == "subscribe" and token == VERIFY_TOKEN:
        return challenge, 200

    return "Forbidden", 403

# =========================
# WEBHOOK - RECEBIMENTO (POST)
# =========================
@app.post("/webhook")
def webhook():
    payload = request.get_json(silent=True) or {}
    print("Payload recebido:", payload)

    phone, text, msg_id = extract_whatsapp_message(payload)

    # E2: Idempotência
    if msg_id and already_processed(msg_id):
        print("Evento duplicado ignorado (idempotência). msg_id:", msg_id)
        return jsonify(status="ok", duplicated=True), 200

    session = get_session(phone)
    step = session.get("step", "START")
    data = session.get("data", {}) or {}

    reply, next_step, new_data = decide_reply(step, text, data)

    # salva estado
    session["step"] = next_step
    session["data"] = new_data
    save_session(phone, session)

    # logs
    print("Telefone:", phone)
    print("Mensagem:", text)
    print("Step:", step, "->", next_step)
    print("Dados:", new_data)
    print("Resposta gerada:", reply)

    # envio real (deixe comentado até integrar)
    # send_whatsapp_message(phone, reply)

    return jsonify(status="ok"), 200

# =========================
# START LOCAL
# =========================
if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
