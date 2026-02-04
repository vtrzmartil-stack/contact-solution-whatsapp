import os
import json
import base64
from datetime import datetime

from flask import Flask, request, jsonify

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

app = Flask(__name__)

# =========================
# CONFIG (ENV VARS)
# =========================
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "contact-solution-token")

# WhatsApp Cloud API (se você estiver usando)
WHATSAPP_TOKEN = os.getenv("WHATSAPP_TOKEN")
PHONE_NUMBER_ID = os.getenv("PHONE_NUMBER_ID")

# Google Sheets (E1)
# Render está com GSHEET_ID -> perfeito. Mantemos fallback pra SHEET_ID só por segurança.
GSHEET_ID = os.getenv("GSHEET_ID") or os.getenv("SHEET_ID")
GOOGLE_SA_B64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_B64")

# Aba e range (no seu print: Página1 com colunas A..H)
SHEET_TAB_NAME = os.getenv("SHEET_TAB_NAME", "Página1")
READ_RANGE = f"{SHEET_TAB_NAME}!A1:H1"      # lê cabeçalho
WRITE_RANGE = f"{SHEET_TAB_NAME}!A:H"       # append no final, 8 colunas

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


# =========================
# HELPERS
# =========================
def build_sheets_service_from_b64(b64_str: str):
    """
    Recebe o JSON da service account em base64 (string),
    decodifica e cria o client do Google Sheets.
    """
    if not b64_str:
        raise ValueError("GOOGLE_SERVICE_ACCOUNT_B64 está vazio/ausente")

    try:
        raw = base64.b64decode(b64_str).decode("utf-8")
        info = json.loads(raw)
    except Exception as e:
        raise ValueError(f"Falha ao decodificar GOOGLE_SERVICE_ACCOUNT_B64: {e}")

    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    service = build("sheets", "v4", credentials=creds)
    return service


def require_sheets_env():
    """
    Valida env vars críticas e retorna (service, sheet_id).
    """
    if not GSHEET_ID:
        raise ValueError("GSHEET_ID (ou SHEET_ID) não configurado no Render")
    if not GOOGLE_SA_B64:
        raise ValueError("GOOGLE_SERVICE_ACCOUNT_B64 não configurado no Render")

    service = build_sheets_service_from_b64(GOOGLE_SA_B64)
    return service, GSHEET_ID


# =========================
# ROUTES
# =========================
@app.get("/")
def home():
    return jsonify({"status": "ok", "message": "contact-solution-whatsapp up"})


@app.get("/test-sheets")
def test_sheets_read():
    """
    Teste de leitura do cabeçalho (A1:H1).
    Deve retornar ["created_at","phone","setor","nome","email","produto","cep","necessidade"]
    """
    try:
        service, sheet_id = require_sheets_env()

        result = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=sheet_id, range=READ_RANGE)
            .execute()
        )

        values = result.get("values", [])
        return jsonify({"status": "ok", "range": READ_RANGE, "values": values})

    except Exception as e:
        # log no Render
        print("[/test-sheets] ERROR:", str(e))
        return jsonify({"status": "error", "error": str(e)}), 500


@app.get("/test-sheets-write")
def test_sheets_write():
    """
    Teste de escrita: adiciona uma linha no final da planilha.
    """
    try:
        service, sheet_id = require_sheets_env()

        now = datetime.utcnow().isoformat()

        row = [
            now,                 # created_at
            "5511999999999",     # phone
            "vendas",            # setor
            "Teste",             # nome
            "teste@email.com",   # email
            "iphone 13",         # produto
            "05068050",          # cep
            "quero orçamento",   # necessidade
        ]

        body = {"values": [row]}

        result = (
            service.spreadsheets()
            .values()
            .append(
                spreadsheetId=sheet_id,
                range=WRITE_RANGE,
                valueInputOption="USER_ENTERED",
                insertDataOption="INSERT_ROWS",
                body=body,
            )
            .execute()
        )

        return jsonify({"status": "ok", "updatedRange": result.get("updates", {}).get("updatedRange")})

    except Exception as e:
        print("[/test-sheets-write] ERROR:", str(e))
        return jsonify({"status": "error", "error": str(e)}), 500


# =========================
# MAIN (local)
# =========================
if __name__ == "__main__":
    # Para rodar local:
    # set GSHEET_ID=...
    # set GOOGLE_SERVICE_ACCOUNT_B64=...
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=True)
