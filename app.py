from flask import Flask, request, jsonify
import os
import requests
import time

app = Flask(__name__)

# =========================
# CONFIG
# =========================
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "contact-solution-token")

# (Opcional) Para enviar mensagem via WhatsApp Cloud API depois:
WHATSAPP_TOKEN = os.getenv("WHATSAPP_TOKEN")
PHONE_NUMBER_ID = os.getenv("PHONE_NUMBER_ID")

# =========================
# SESSIONS (memória)
# Em produção/escala: trocar por Redis/DB
# =========================
SESSIONS = {}  # { "5511...": {"step": "START", "data": {...}, "updated_at": 123456 } }

SESSION_TTL_SECONDS = 60 * 60 * 6  # 6 horas (ajuste como quiser)


def now_ts() -> int:
    return int(time.time())


def cleanup_sessions():
    """Remove sessões antigas para não crescer infinito (simples)."""
    if not SESSIONS:
        return
    limit = now_ts() - SESSION_TTL_SECONDS
    to_delete = [k for k, v in SESSIONS.items() if v.get("updated_at", 0) < limit]
    for k in to_delete:
        del SESSIONS[k]


def get_session(phone: str) -> dict:
    cleanup_sessions()
    if not phone:
        phone = "desconhecido"
    if phone not in SESSIONS:
        SESSIONS[phone] = {"step": "START", "data": {}, "updated_at": now_ts()}
    else:
        SESSIONS[phone]["updated_at"] = now_ts()
    return SESSIONS[phone]


def reset_session(phone: str):
    SESSIONS[phone] = {"step": "START", "data": {}, "updated_at": now_ts()}


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
# HELPERS
# =========================
def normalize_text(t: str) -> str:
    return str(t or "").strip().lower()


def extract_whatsapp_message(payload: dict):
    """
    Extrai:
      - phone (wa_id / from)
      - text (mensagem)
    Retorna: (phone, text)

    Se não achar, retorna ("desconhecido", "")
    """
    try:
        entry = (payload.get("entry") or [{}])[0]
        changes = (entry.get("changes") or [{}])[0]
        value = changes.get("value") or {}

        messages = value.get("messages") or []
        if not messages:
            return "desconhecido", ""

        msg = messages[0]

        # Geralmente vem em msg["from"]
        phone = msg.get("from") or "desconhecido"

        # Texto comum
        text_obj = msg.get("text") or {}
        text = text_obj.get("body") or ""

        return phone, normalize_text(text)

    except Exception as e:
        print("Erro ao extrair mensagem:", e)
        return "desconhecido", ""


def menu_text():
    return (
        "Olá! 👋\n"
        "Sou o atendimento automático 🤖\n\n"
        "Digite:\n"
        "1️⃣ para Vendas\n"
        "2️⃣ para Suporte\n\n"
        "Digite *reiniciar* a qualquer momento para recomeçar."
    )


def send_whatsapp_message(to_phone: str, message_text: str) -> bool:
    """
    Envia mensagem via WhatsApp Cloud API.
    Só use quando WHATSAPP_TOKEN e PHONE_NUMBER_ID estiverem configurados.
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
# FLUXO COM ESTADOS + COLETA DE LEAD (Vendas e Suporte)
# =========================
def decide_reply(step: str, text: str, data: dict):
    """
    Recebe:
      - step: estado atual
      - text: mensagem normalizada
      - data: dicionário com dados do lead

    Retorna:
      - reply: texto a responder
      - next_step: próximo estado
    """

    # Comandos globais
    if text == "reiniciar":
        data.clear()
        return "✅ Reiniciado. " + menu_text(), "MENU"

    if text == "0":
        data.clear()
        return "↩️ Voltando ao menu.\n\n" + menu_text(), "MENU"

    # Se não tem texto (ex: payload sem messages.text.body)
    if not text:
        return "Não recebi texto. Digite *oi* para começar. 🙂", "START"

    # START
    if step == "START":
        # aceita oi/ola/olá
        if "oi" in text or "olá" in text or "ola" in text:
            return menu_text(), "MENU"
        return "Digite *oi* para começar. 🙂", "START"

    # MENU
    if step == "MENU":
        if text == "1":
            data["area"] = "vendas"
            return "Perfeito! Vamos começar. Qual é o seu *nome*?", "VENDAS_NAME"
        if text == "2":
            data["area"] = "suporte"
            return "Certo! Qual é o *produto/serviço* que você precisa de suporte?", "SUPORTE_PRODUCT"

        # caso digite oi de novo
        if "oi" in text or "olá" in text or "ola" in text:
            return menu_text(), "MENU"

        return "Não entendi. No menu, digite:\n1️⃣ Vendas\n2️⃣ Suporte\n\nOu 0 para voltar.", "MENU"

    # ======================
    # VENDAS (lead)
    # ======================
    if step == "VENDAS_NAME":
        data["nome"] = text.title()
        return "Show, {0}! Qual é o *nome da empresa* (ou diga 'pessoa física')?".format(data["nome"]), "VENDAS_COMPANY"

    if step == "VENDAS_COMPANY":
        data["empresa"] = text.title()
        return "Legal. Em 1 frase: qual é a sua *necessidade/objetivo*? (ex: orçamento, consultoria, parceria)".strip(), "VENDAS_NEED"

    if step == "VENDAS_NEED":
        data["necessidade"] = text
        return "Perfeito. Qual sua *cidade/UF*? (ex: São Paulo/SP)", "VENDAS_CITY"

    if step == "VENDAS_CITY":
        data["cidade_uf"] = text.title()
        resumo = (
            "✅ Confirma seus dados?\n\n"
            f"Nome: {data.get('nome','')}\n"
            f"Empresa: {data.get('empresa','')}\n"
            f"Necessidade: {data.get('necessidade','')}\n"
            f"Cidade/UF: {data.get('cidade_uf','')}\n\n"
            "Digite:\n"
            "1️⃣ Confirmar\n"
            "2️⃣ Corrigir (recomeçar)\n"
            "0️⃣ Menu"
        )
        return resumo, "VENDAS_CONFIRM"

    if step == "VENDAS_CONFIRM":
        if text == "1":
            data["confirmado"] = True
            return (
                "✅ Perfeito! Já registrei seu lead.\n"
                "Em breve alguém do time de Vendas vai te chamar aqui. 🙌\n\n"
                "Se quiser voltar ao menu, digite 0."
            ), "DONE"
        if text == "2":
            # recomeça vendas
            area = data.get("area")
            data.clear()
            data["area"] = area or "vendas"
            return "Sem problemas. Qual é o seu *nome*?", "VENDAS_NAME"
        return "Digite 1 para confirmar, 2 para corrigir ou 0 para menu.", "VENDAS_CONFIRM"

    # ======================
    # SUPORTE (lead)
    # ======================
    if step == "SUPORTE_PRODUCT":
        data["produto"] = text
        return "Entendi. Descreva o *problema* (o que está acontecendo)?", "SUPORTE_PROBLEM"

    if step == "SUPORTE_PROBLEM":
        data["problema"] = text
        return (
            "Qual a *urgência*?\n"
            "1️⃣ Baixa (posso aguardar)\n"
            "2️⃣ Média\n"
            "3️⃣ Alta (parado/impactando muito)\n\n"
            "Ou responda com uma frase."
        ), "SUPORTE_URGENCY"

    if step == "SUPORTE_URGENCY":
        urg_map = {"1": "baixa", "2": "média", "3": "alta"}
        data["urgencia"] = urg_map.get(text, text)
        return (
            "Qual o melhor *contato* para retorno?\n"
            "Pode ser e-mail ou telefone (ou diga 'este número')."
        ), "SUPORTE_CONTACT"

    if step == "SUPORTE_CONTACT":
        data["contato_retorno"] = text
        resumo = (
            "✅ Confirma os dados do suporte?\n\n"
            f"Produto/Serviço: {data.get('produto','')}\n"
            f"Problema: {data.get('problema','')}\n"
            f"Urgência: {data.get('urgencia','')}\n"
            f"Contato: {data.get('contato_retorno','')}\n\n"
            "Digite:\n"
            "1️⃣ Confirmar\n"
            "2️⃣ Corrigir (recomeçar)\n"
            "0️⃣ Menu"
        )
        return resumo, "SUPORTE_CONFIRM"

    if step == "SUPORTE_CONFIRM":
        if text == "1":
            data["confirmado"] = True
            return (
                "✅ Beleza! Já registrei seu chamado.\n"
                "Em breve o Suporte vai te chamar por aqui. 🛠️\n\n"
                "Se quiser voltar ao menu, digite 0."
            ), "DONE"
        if text == "2":
            area = data.get("area")
            data.clear()
            data["area"] = area or "suporte"
            return "Certo! Qual é o *produto/serviço* que você precisa de suporte?", "SUPORTE_PRODUCT"
        return "Digite 1 para confirmar, 2 para corrigir ou 0 para menu.", "SUPORTE_CONFIRM"

    # DONE
    if step == "DONE":
        # se a pessoa digitar algo depois de concluído
        if "oi" in text or "olá" in text or "ola" in text:
            return menu_text(), "MENU"
        return "✅ Atendimento finalizado. Digite 0 para voltar ao menu ou *reiniciar* para recomeçar.", "DONE"

    # fallback
    return "Algo saiu do fluxo. Digite *reiniciar* para recomeçar.", "START"


# =========================
# WEBHOOK - RECEBIMENTO (POST)
# =========================
@app.post("/webhook")
def webhook():
    payload = request.get_json(silent=True) or {}
    print("Payload recebido:", payload)

    phone, text = extract_whatsapp_message(payload)

    session = get_session(phone)
    step = session["step"]
    data = session["data"]

    reply, next_step = decide_reply(step, text, data)

    # atualiza estado
    session["step"] = next_step
    session["data"] = data
    session["updated_at"] = now_ts()

    # logs úteis (Render)
    print("Telefone:", phone)
    print("Mensagem:", text)
    print("Step atual:", step, "-> Próximo:", next_step)
    print("Dados:", data)
    print("Resposta gerada:", reply)

    # Por enquanto, não envia de verdade (só loga).
    # Quando integrar, descomente:
    # send_whatsapp_message(phone, reply)

    return jsonify(status="ok"), 200


# =========================
# START LOCAL (opcional)
# =========================
if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
