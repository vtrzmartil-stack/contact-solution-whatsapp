from flask import Flask, request, jsonify
import os
import json
import base64
import re
import datetime
import requests

from google.oauth2 import service_account
from googleapiclient.discovery import build
import os

app = Flask(__name__)

# =========================
# ESTADO EM MEMÓRIA (MVP)
# =========================
SESSIONS = {}  # { "5511...": {"step": "START", "data": {...}} }

def get_session(phone: str) -> dict:
    if phone not in SESSIONS:
        SESSIONS[phone] = {"step": "START", "data": {}}
    return SESSIONS[phone]
    
def test_google_sheets():
    try:
        creds = service_account.Credentials.from_service_account_file(
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"],
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )

        service = build("sheets", "v4", credentials=creds)

        sheet_id = os.environ["SHEET_ID"]
        range_name = "Página1!A1"

        result = service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range=range_name
        ).execute()

        return {
            "status": "ok",
            "values": result.get("values", [])
        }

    except Exception as e:
        return {
            "status": "error",
            "error": str(e)
        }

@app.get("/test-sheets")
def test_sheets_route():
    return test_google_sheets()


def reset_session(phone: str):
    SESSIONS[phone] = {"step": "START", "data": {}}

# =========================
# CONFIG
# =========================
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "contact-solution-token")

# WhatsApp Cloud API (E2) — opcional, por enquanto pode ficar desligado
WHATSAPP_TOKEN = os.getenv("WHATSAPP_TOKEN")
PHONE_NUMBER_ID = os.getenv("PHONE_NUMBER_ID")

# E1 (Google Sheets)
GSHEET_ID = os.getenv("GSHEET_ID")  # id da planilha
GOOGLE_SA_B64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_B64")  # service account json em base64 (recomendado)

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
# WEBHOOK VERIFICAÇÃO (META)
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
        phone = msg.get("from") or "desconhecido"

        text_obj = msg.get("text") or {}
        text = text_obj.get("body") or ""
        text = str(text).strip().lower()

        return phone, text
    except Exception as e:
        print("Erro ao extrair mensagem:", e)
        return "desconhecido", ""

def normalize_cep(raw: str) -> str:
    """
    Aceita '12345-678' ou '12345678' e retorna '12345678'.
    Se inválido, retorna ''.
    """
    if not raw:
        return ""
    digits = re.sub(r"\D", "", raw)
    if len(digits) != 8:
        return ""
    return digits

def looks_like_email(s: str) -> bool:
    # validação simples (MVP)
    return bool(re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", s.strip()))

# =========================
# E1: PERSISTÊNCIA (GOOGLE SHEETS)
# =========================
def save_lead_to_gsheet(lead: dict) -> bool:
    """
    Salva uma linha na planilha (Google Sheets).
    Retorna True/False.
    """
    if not GSHEET_ID or not GOOGLE_SA_B64:
        print("E1 desativado: GSHEET_ID ou GOOGLE_SERVICE_ACCOUNT_B64 não configurado.")
        return False

    try:
        # lazy import (só carrega se usar)
        import gspread
        from google.oauth2.service_account import Credentials

        sa_json = base64.b64decode(GOOGLE_SA_B64).decode("utf-8")
        info = json.loads(sa_json)

        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_info(info, scopes=scopes)
        gc = gspread.authorize(creds)

        sh = gc.open_by_key(GSHEET_ID)
        ws = sh.sheet1  # primeira aba

        # cabeçalho esperado (se quiser, você pode criar manualmente na planilha)
        row = [
            lead.get("created_at", ""),
            lead.get("phone", ""),
            lead.get("setor", ""),
            lead.get("nome", ""),
            lead.get("email", ""),
            lead.get("produto", ""),
            lead.get("cep", ""),
            lead.get("necessidade", ""),
        ]

        ws.append_row(row, value_input_option="USER_ENTERED")
        return True

    except Exception as e:
        print("Erro ao salvar no Google Sheets:", e)
        return False

# =========================
# DECISÃO DE RESPOSTA (COM ESTADOS)
# =========================
def decide_reply(step: str, text: str, data: dict):
    """
    Retorna (reply_text, next_step)
    """
    # comandos globais
    if text in {"reset", "reiniciar", "começar", "comecar"}:
        data.clear()
        return "Perfeito. Vamos recomeçar. Digite 'oi' para iniciar. ✅", "START"

    # START: só avança com oi/olá
    if step == "START":
        if not text:
            return "Digite 'oi' para começar. 🙂", "START"
        if "oi" in text or "olá" in text or "ola" in text:
            return (
                "Olá! 👋\n"
                "Sou o atendimento automático 🤖\n\n"
                "Digite:\n"
                "1️⃣ para Vendas\n"
                "2️⃣ para Suporte"
            ), "MENU"
        return "Não entendi. Digite 'oi' para começar. 🙂", "START"

    # MENU: escolhe setor
    if step == "MENU":
        if text == "1":
            data["setor"] = "vendas"
            return "Show! Antes de te encaminhar, me diga seu nome:", "LEAD_NAME"
        if text == "2":
            data["setor"] = "suporte"
            return "Certo! Antes de te encaminhar, me diga seu nome:", "LEAD_NAME"
        return "Opção inválida. Digite 1️⃣ (Vendas) ou 2️⃣ (Suporte).", "MENU"

    # LEAD_NAME
    if step == "LEAD_NAME":
        if not text:
            return "Qual seu nome? (pode ser só o primeiro) 🙂", "LEAD_NAME"
        data["nome"] = text.title()
        return "Obrigado! Agora me diga seu e-mail (se não tiver, digite 'pular'):", "LEAD_EMAIL"

    # LEAD_EMAIL (opcional)
    if step == "LEAD_EMAIL":
        if text == "pular":
            return "Perfeito. Qual produto você está procurando? (ex: 'iPhone 13', 'câmera', 'notebook')", "LEAD_PRODUCT"
        if looks_like_email(text):
            data["email"] = text
            return "Perfeito. Qual produto você está procurando? (ex: 'iPhone 13', 'câmera', 'notebook')", "LEAD_PRODUCT"
        return "E-mail inválido. Digite um e-mail válido ou 'pular'.", "LEAD_EMAIL"

    # LEAD_PRODUCT
    if step == "LEAD_PRODUCT":
        if not text:
            return "Qual produto você está procurando? 🙂", "LEAD_PRODUCT"
        data["produto"] = text
        return "Boa! Agora me diga seu CEP (somente números ou com hífen). Ex: 01001-000", "LEAD_CEP"

    # LEAD_CEP
    if step == "LEAD_CEP":
        cep = normalize_cep(text)
        if not cep:
            return "CEP inválido. Envie no formato 01001-000 ou 01001000.", "LEAD_CEP"
        data["cep"] = cep
        return "Agora descreva em 1 frase o que você precisa (ex: 'quero orçamento', 'tirar dúvida', 'acompanhar pedido'):", "LEAD_NEED"

    # LEAD_NEED
    if step == "LEAD_NEED":
        if not text:
            return "Me diga em 1 frase o que você precisa 🙂", "LEAD_NEED"
        data["necessidade"] = text

        setor = data.get("setor", "vendas")
        setor_label = "Vendas" if setor == "vendas" else "Suporte"

        resumo = (
            "✅ Só pra confirmar:\n"
            f"• Nome: {data.get('nome','')}\n"
            f"• Email: {data.get('email','(não informado)')}\n"
            f"• Produto: {data.get('produto','')}\n"
            f"• CEP: {data.get('cep','')}\n"
            f"• Necessidade: {data.get('necessidade','')}\n"
            f"• Setor: {setor_label}\n\n"
            "Digite 1 para confirmar ✅\n"
            "Digite 2 para recomeçar 🔄"
        )
        return resumo, "CONFIRMA_SETOR"

    # CONFIRMA_SETOR
    if step == "CONFIRMA_SETOR":
        if text == "1":
            return "Perfeito! Registrando seus dados… ✅", "FINAL"
        if text == "2":
            data.clear()
            return "Beleza! Digite 'oi' para recomeçar. 🙂", "START"
        return "Digite 1 para confirmar ou 2 para recomeçar.", "CONFIRMA_SETOR"

    # FINAL
    if step == "FINAL":
        return "Pronto! Registrei seus dados e vou encaminhar. Em breve alguém te chama por aqui. ✅", "START"

    # fallback
    return "Não entendi. Digite 'oi' para começar. 🙂", "START"

# =========================
# ENVIO WHATSAPP (E2) — opcional
# =========================
def send_whatsapp_message(to_phone: str, message_text: str) -> bool:
    if not WHATSAPP_TOKEN or not PHONE_NUMBER_ID:
        print("Envio desativado (WHATSAPP_TOKEN/PHONE_NUMBER_ID ausentes).")
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
# WEBHOOK RECEBIMENTO (POST)
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

    # logs úteis
    print("Telefone:", phone)
    print("Mensagem:", text)
    print("Step:", step, "->", next_step)
    print("Dados:", data)
    print("Resposta gerada:", reply)

    # Se entrou no FINAL, salva lead
    if next_step == "FINAL":
        lead = {
            "created_at": datetime.datetime.utcnow().isoformat(),
            "phone": phone,
            "setor": data.get("setor", ""),
            "nome": data.get("nome", ""),
            "email": data.get("email", ""),
            "produto": data.get("produto", ""),
            "cep": data.get("cep", ""),
            "necessidade": data.get("necessidade", ""),
        }
        ok = save_lead_to_gsheet(lead)
        print("Lead salvo no Sheets?", ok)

        # depois de salvar, você pode resetar ou manter
        reset_session(phone)

    # (E2) Se quiser responder de verdade no WhatsApp, descomente:
    # send_whatsapp_message(phone, reply)

    return jsonify(status="ok"), 200

# =========================
# START LOCAL
# =========================
if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
