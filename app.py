import os
import json
import base64
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

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

# Nome da aba do Google Sheets (no seu print era "Página1")
SHEET_TAB_NAME = os.getenv("SHEET_TAB_NAME", "Página1")

# 7 colunas: created_at, phone, setor, nome, email, produto, cep
APPEND_RANGE = f"{SHEET_TAB_NAME}!A:G"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


app = FastAPI(title="Contact Solution WhatsApp Backend")


# ---------------------------
# Sessões (memória simples em RAM)
# Obs: Render free pode reiniciar; suficiente pra MVP/testes.
# ---------------------------
SESSIONS: Dict[str, Dict[str, Any]] = {}


# ---------------------------
# Helpers - Google Sheets
# ---------------------------
def _normalize_b64(s: str) -> str:
    """Remove quebras de linha/espaços e corrige padding (=) do Base64."""
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
# Helpers - WhatsApp payload (simulado/real)
# ---------------------------
def _extract_whatsapp_message(payload: Dict[str, Any]) -> Optional[Dict[str, str]]:
    """
    Espera payload no formato WhatsApp Cloud API (ou simulado no Postman):
    {
      "entry":[{"changes":[{"value":{"messages":[{"from":"...","text":{"body":"..."}}]}}]}]
    }
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
        return {"from": sender, "text": text or ""}
    except Exception:
        return None


def _is_valid_email(s: str) -> bool:
    s = (s or "").strip()
    return "@" in s and "." in s and len(s) >= 6


def _normalize_cep(s: str) -> str:
    digits = "".join(ch for ch in (s or "") if ch.isdigit())
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
# Rotas básicas
# ---------------------------
@app.get("/")
def root():
    return {
        "status": "ok",
        "service": "contact-solution-whatsapp",
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
    """
    Cria uma linha de teste com 7 colunas:
    created_at, phone, setor, nome, email, produto, cep
    """
    try:
        now = datetime.now(timezone.utc).isoformat()
        row = [
            now,
            "5511999999999",
            "teste",
            "Lead Teste",
            "teste@email.com",
            "produto X",
            "00000-000",
        ]
        return _append_row(row)
    except Exception as e:
        logger.exception("Erro no /test-sheets-write")
        return JSONResponse(status_code=500, content={"status": "error", "error": str(e)})


# ---------------------------
# Webhook WhatsApp (GET verify)
# ---------------------------
@app.get("/webhook")
async def webhook_verify(request: Request):
    """
    Verificação da Meta (WhatsApp Cloud API):
    GET /webhook?hub.mode=subscribe&hub.verify_token=...&hub.challenge=...
    """
    qp = request.query_params
    mode = qp.get("hub.mode")
    token = qp.get("hub.verify_token")
    challenge = qp.get("hub.challenge")

    if mode == "subscribe" and token and token == VERIFY_TOKEN and challenge:
        return PlainTextResponse(challenge)

    return JSONResponse(status_code=403, content={"status": "error", "error": "Verification failed"})


# ---------------------------
# Webhook WhatsApp (POST receive) - fluxo por etapas
# ---------------------------
@app.post("/webhook")
async def webhook_receive(request: Request):
    payload = await request.json()
    msg = _extract_whatsapp_message(payload)

    # WhatsApp pode mandar eventos sem "messages" (status, etc.)
    if not msg:
        return {"status": "ignored"}

    phone = msg["from"]
    text = (msg["text"] or "").strip()
    now = datetime.now(timezone.utc).isoformat()

    session = _get_or_start_session(phone)
    step = session["step"]
    data = session["data"]

    logger.info(f"[FLOW] phone={phone} step={step} text='{text}'")

    greetings = {"oi", "olá", "ola", "bom dia", "boa tarde", "boa noite", "hello", "hi"}

    # Mensagem inicial
    if step == "nome" and not data["nome"] and text.lower() in greetings:
        return {"status": "ok", "reply": "Olá! 👋 Tudo bem? Qual é o seu nome?"}

    # Etapa: NOME
    if step == "nome":
        if not text:
            return {"status": "ok", "reply": "Qual é o seu nome?"}
        data["nome"] = text
        session["step"] = "email"
        return {"status": "ok", "reply": f"Prazer, {data['nome']}! Qual é o seu e-mail?"}

    # Etapa: EMAIL
    if step == "email":
        if not _is_valid_email(text):
            return {"status": "ok", "reply": "Esse e-mail parece inválido 😅 Pode enviar novamente?"}
        data["email"] = text
        session["step"] = "produto"
        return {"status": "ok", "reply": "Perfeito! Qual produto você tem interesse?"}

    # Etapa: PRODUTO
    if step == "produto":
        if not text:
            return {"status": "ok", "reply": "Qual produto você tem interesse?"}
        data["produto"] = text
        session["step"] = "cep"
        return {"status": "ok", "reply": "Boa! Agora me envie seu CEP (apenas números) pra eu preparar a oferta certinha."}

    # Etapa: CEP + FINALIZA
    if step == "cep":
        cep = _normalize_cep(text)
        if not cep:
            return {"status": "ok", "reply": "CEP inválido. Envie apenas números (8 dígitos)."}
        data["cep"] = cep

        # Salvar no Sheets (7 colunas)
        row = [now, phone, data["setor"], data["nome"], data["email"], data["produto"], data["cep"]]

        try:
            saved = _append_row(row)
            SESSIONS.pop(phone, None)
            return {
                "status": "ok",
                "saved": saved,
                "reply": (
                    f"Fechado, {data['nome']} ✅\n"
                    f"Já registrei seu interesse em *{data['produto']}*.\n"
                    "Um vendedor vai te chamar em breve com uma oferta preparada pra você."
                ),
            }
        except Exception as e:
            logger.exception("Erro ao salvar lead final")
            return JSONResponse(status_code=500, content={"status": "error", "error": str(e)})

    # fallback
    SESSIONS.pop(phone, None)
    return {"status": "ok", "reply": "Vamos recomeçar 🙂 Qual é o seu nome?"}
