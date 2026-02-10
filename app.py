import os
import json
import base64
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse

from google.oauth2 import service_account
from googleapiclient.discovery import build

# ---------------------------
# Logging
# ---------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("contact-solution")

# ---------------------------
# Env (PADRÃO ÚNICO)
# ---------------------------
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "")

GSHEET_ID = os.getenv("GSHEET_ID", "")
GOOGLE_SA_B64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_B64", "")
SHEET_TAB_NAME = os.getenv("SHEET_TAB_NAME", "Página1")
APPEND_RANGE = f"{SHEET_TAB_NAME}!A:G"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# WhatsApp Cloud API
WHATSAPP_TOKEN = os.getenv("WHATSAPP_TOKEN", "")
WHATSAPP_PHONE_NUMBER_ID = os.getenv("WHATSAPP_PHONE_NUMBER_ID", "")
WHATSAPP_API_VERSION = os.getenv("WHATSAPP_API_VERSION", "v20.0")  # pode deixar assim

app = FastAPI(title="Contact Solution WhatsApp Backend")

# ---------------------------
# Sessões (memória simples)
# ---------------------------
# OBS: Em Render free pode reiniciar; isso é suficiente pra MVP/testes.
SESSIONS: Dict[str, Dict[str, Any]] = {}


# ---------------------------
# Helpers - Google Sheets
# ---------------------------
def _normalize_b64(s: str) -> str:
    s = (s or "").strip().replace("\n", "").replace("\r", "").replace(" ", "")
    missing = len(s) % 4
    if missing:
        s += "=" * (4 - missing)
    return s


def _get_sheets_service():
    if not GSHEET_ID:
        raise RuntimeError("GSHEET_ID ausente")
    if not GOOGLE_SA_B64:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_B64 ausente")

    b64 = _normalize_b64(GOOGLE_SA_B64)

    try:
        raw = base64.b64decode(b64).decode("utf-8")
    except Exception as e:
        raise RuntimeError(f"Falha ao decodificar GOOGLE_SERVICE_ACCOUNT_B64: {e}")

    try:
        info = json.loads(raw)
    except Exception as e:
        raise RuntimeError(f"JSON inválido na credencial decodificada: {e}")

    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def _append_row(row: List[Any]) -> Dict[str, Any]:
    service = _get_sheets_service()
    body = {"values": [row]}
    result = (
        service.spreadsheets()
        .values()
        .append(
            spreadsheetId=GSHEET_ID,
            range=APPEND_RANGE,
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body=body,
        )
        .execute()
    )
    updates = result.get("updates", {})
    return {
        "status": "ok",
        "updatedRange": updates.get("updatedRange"),
        "updatedRows": updates.get("updatedRows"),
    }


# ---------------------------
# Helpers - WhatsApp
# ---------------------------
def _send_whatsapp_text(to_phone: str, text: str) -> None:
    """
    Envia mensagem via WhatsApp Cloud API.
    Requer WHATSAPP_TOKEN e WHATSAPP_PHONE_NUMBER_ID.
    """
    if not WHATSAPP_TOKEN or not WHATSAPP_PHONE_NUMBER_ID:
        logger.warning("WHATSAPP_TOKEN/WHATSAPP_PHONE_NUMBER_ID ausente. Não enviei resposta.")
        return

    url = f"https://graph.facebook.com/{WHATSAPP_API_VERSION}/{WHATSAPP_PHONE_NUMBER_ID}/messages"
    headers = {
        "Authorization": f"Bearer {WHATSAPP_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {
        "messaging_product": "whatsapp",
        "to": to_phone,
        "type": "text",
        "text": {"body": text},
    }

    r = requests.post(url, headers=headers, json=payload, timeout=15)
    if r.status_code >= 300:
        logger.error(f"Falha ao enviar WhatsApp ({r.status_code}): {r.text}")
    else:
        logger.info(f"[WA] enviado para {to_phone}: {text}")


def _extract_whatsapp_message(payload: Dict[str, Any]) -> Optional[Dict[str, str]]:
    """
    Formato Cloud API.
    """
    try:
        entry = payload.get("entry", [])[0]
        changes = entry.get("changes", [])[0]
        value = changes.get("value", {})
        messages = value.get("messages", [])
        if not messages:
            return None
        msg = messages[0]
        sender = msg.get("from", "")
        text = (msg.get("text") or {}).get("body", "")
        if not sender:
            return None
        return {"from": sender, "text": text}
    except Exception:
        return None


def _is_valid_email(s: str) -> bool:
    s = s.strip()
    return "@" in s and "." in s and len(s) >= 6


def _normalize_cep(s: str) -> str:
    digits = "".join(ch for ch in s if ch.isdigit())
    if len(digits) == 8:
        return f"{digits[:5]}-{digits[5:]}"
    return ""


def _get_or_start_session(phone: str) -> Dict[str, Any]:
    if phone not in SESSIONS:
        SESSIONS[phone] = {
            "step": "nome",
            "data": {"setor": "", "nome": "", "email": "", "produto": "", "cep": ""},
        }
    return SESSIONS[phone]


# ---------------------------
# Routes básicas
# ---------------------------
@app.get("/")
def root():
    return {
        "status": "ok",
        "endpoints": ["/health", "/test-sheets", "/test-sheets-write", "/webhook"],
    }


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/test-sheets")
def test_sheets_read():
    try:
        service = _get_sheets_service()
        result = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=GSHEET_ID, range=APPEND_RANGE)
            .execute()
        )
        values = result.get("values", [])
        return {"status": "ok", "values": values[:10]}
    except Exception as e:
        logger.exception("Erro no /test-sheets")
        return JSONResponse(status_code=500, content={"status": "error", "error": str(e)})


@app.get("/test-sheets-write")
def test_sheets_write():
    try:
        now = datetime.now(timezone.utc).isoformat()
        row = [now, "5511999999999", "teste", "Lead Teste", "teste@email.com", "produto X", "00000-000"]
        return _append_row(row)
    except Exception as e:
        logger.exception("Erro no /test-sheets-write")
        return JSONResponse(status_code=500, content={"status": "error", "error": str(e)})


# ---------------------------
# Webhook WhatsApp (GET verify + POST receive)
# ---------------------------
@app.get("/webhook")
async def webhook_verify(request: Request):
    qp = request.query_params
    mode = qp.get("hub.mode")
    token = qp.get("hub.verify_token")
    challenge = qp.get("hub.challenge")

    if mode == "subscribe" and token and token == VERIFY_TOKEN and challenge:
        return PlainTextResponse(challenge)

    return JSONResponse(status_code=403, content={"status": "error", "error": "Verification failed"})


@app.post("/webhook")
async def webhook_receive(request: Request):
    payload = await request.json()
    msg = _extract_whatsapp_message(payload)

    # Cloud API também manda eventos sem "messages"
    if not msg:
        return {"status": "ignored"}

    phone = msg["from"]
    text = (msg["text"] or "").strip()
    now = datetime.now(timezone.utc).isoformat()

    session = _get_or_start_session(phone)
    step = session["step"]
    data = session["data"]

    logger.info(f"[FLOW] phone={phone} step={step} text='{text}'")

    # Se for primeira interação ou sessão recém-criada: manda olá e pergunta nome
    if step == "nome" and not data["nome"] and text.lower() in {"oi", "olá", "ola", "bom dia", "boa tarde", "boa noite"}:
        _send_whatsapp_text(phone, "Olá! 👋 Tudo bem? Qual é o seu nome?")
        return {"status": "ok"}

    # Etapa: NOME
    if step == "nome":
        data["nome"] = text
        session["step"] = "email"
        _send_whatsapp_text(phone, f"Prazer, {data['nome']}! Qual é o seu e-mail?")
        return {"status": "ok"}

    # Etapa: EMAIL
    if step == "email":
        if not _is_valid_email(text):
            _send_whatsapp_text(phone, "Esse e-mail parece inválido 😅 Pode enviar novamente?")
            return {"status": "ok"}
        data["email"] = text
        session["step"] = "produto"
        _send_whatsapp_text(phone, "Perfeito! Qual produto você tem interesse?")
        return {"status": "ok"}

    # Etapa: PRODUTO
    if step == "produto":
        data["produto"] = text
        session["step"] = "cep"
        _send_whatsapp_text(phone, "Boa! Agora me envie seu CEP (apenas números) pra eu preparar a oferta certinha.")
        return {"status": "ok"}

    # Etapa: CEP + FINALIZA
    if step == "cep":
        cep = _normalize_cep(text)
        if not cep:
            _send_whatsapp_text(phone, "CEP inválido. Envie apenas números (8 dígitos).")
            return {"status": "ok"}

        data["cep"] = cep

        # Salva no Sheets (created_at, phone, setor, nome, email, produto, cep)
        row = [now, phone, data["setor"], data["nome"], data["email"], data["produto"], data["cep"]]

        try:
            _append_row(row)
            # encerra sessão
            SESSIONS.pop(phone, None)

            _send_whatsapp_text(
                phone,
                f"Fechado, {data['nome']} ✅\n"
                f"Já registrei seu interesse em *{data['produto']}*.\n"
                "Um vendedor vai te chamar em breve com uma oferta preparada pra você."
            )
            return {"status": "ok"}
        except Exception as e:
            logger.exception("Erro ao salvar lead final")
            return JSONResponse(status_code=500, content={"status": "error", "error": str(e)})

    # fallback (se algo sair do fluxo)
    _send_whatsapp_text(phone, "Vamos recomeçar rapidinho 🙂 Qual é o seu nome?")
    SESSIONS[phone] = {"step": "nome", "data": {"setor": "", "nome": "", "email": "", "produto": "", "cep": ""}}
    return {"status": "ok"}
