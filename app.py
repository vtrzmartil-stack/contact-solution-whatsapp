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
WHATSAPP_TOKEN = os.getenv("WHATSAPP_TOKEN", "")  # opcional (só p/ enviar msg)
GSHEET_ID = os.getenv("GSHEET_ID", "")
GOOGLE_SA_B64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_B64", "")

# Nome da aba (você está usando Página1)
SHEET_TAB_NAME = os.getenv("SHEET_TAB_NAME", "Página1")

# 7 colunas: created_at, phone, setor, nome, email, produto, cep
APPEND_RANGE = f"{SHEET_TAB_NAME}!A:G"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

app = FastAPI(title="Contact Solution WhatsApp Backend")


# ---------------------------
# Helpers
# ---------------------------
def _normalize_b64(s: str) -> str:
    """Remove quebras de linha e espaços; corrige padding (=)."""
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


def _extract_whatsapp_message(payload: Dict[str, Any]) -> Optional[Dict[str, str]]:
    """
    Espera payload no formato WhatsApp Cloud API.
    Retorna: {"from": "...", "text": "..."} ou None.
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
        if not sender and not text:
            return None
        return {"from": sender, "text": text}
    except Exception:
        return None


# ---------------------------
# Routes
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


# WhatsApp Webhook Verification (GET) + Receive (POST) no mesmo path (/webhook)
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

    # WhatsApp pode mandar eventos sem "messages" (status, etc.)
    if not msg:
        return {"status": "ignored"}

    phone = msg["from"]
    text = msg["text"].strip()
    now = datetime.now(timezone.utc).isoformat()

    # Por enquanto: produto = texto recebido
    # Outros campos ficam vazios até a gente implementar o fluxo de perguntas
    row = [now, phone, "", "", "", text, ""]

    try:
        result = _append_row(row)
        return {"status": "ok", "saved": result}
    except Exception as e:
        logger.exception("Erro ao salvar lead do webhook")
        return JSONResponse(status_code=500, content={"status": "error", "error": str(e)})
