import os
import json
import base64
from datetime import datetime
from flask import Flask, request, jsonify

from google.oauth2 import service_account
from googleapiclient.discovery import build

app = Flask(__name__)

# -----------------------------
# Config (ENV)
# -----------------------------
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "contact-solution-token")
WHATSAPP_TOKEN = os.getenv("WHATSAPP_TOKEN")  # opcional

# Aceita os dois nomes pra evitar dor de cabeça
SHEET_ID = os.getenv("SHEET_ID") or os.getenv("GSHEET_ID")

# Preferido: arquivo no Render Secret Files
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# Fallback (se insistir): base64 do JSON
GOOGLE_SERVICE_ACCOUNT_B64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_B64")


SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
]

DEFAULT_READ_RANGE = os.getenv("SHEETS_READ_RANGE", "Página1!A1:H1")
DEFAULT_WRITE_RANGE = os.getenv("SHEETS_WRITE_RANGE", "Página1!A:H")


# -----------------------------
# Google Sheets helpers
# -----------------------------
def _credentials_from_file(path: str):
    creds = service_account.Credentials.from_service_account_file(path, scopes=SCOPES)
    return creds

def _credentials_from_b64(b64_text: str):
    """
    Render às vezes quebra padding.
    A gente corrige adicionando '=' até múltiplo de 4.
    """
    s = (b64_text or "").strip()

    # remove aspas acidentais
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1].strip()

    # corrige padding
    missing = (-len(s)) % 4
    if missing:
        s += "=" * missing

    raw = base64.b64decode(s.encode("utf-8"))
    info = json.loads(raw.decode("utf-8"))
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return creds

def get_sheets_service():
    # 1) Preferido: arquivo
    if GOOGLE_APPLICATION_CREDENTIALS:
        return build("sheets", "v4", credentials=_credentials_from_file(GOOGLE_APPLICATION_CREDENTIALS))

    # 2) Fallback: base64
    if GOOGLE_SERVICE_ACCOUNT_B64:
        return build("sheets", "v4", credentials=_credentials_from_b64(GOOGLE_SERVICE_ACCOUNT_B64))

    raise RuntimeError(
        "Credenciais ausentes: defina GOOGLE_APPLICATION_CREDENTIALS (recomendado) "
        "ou GOOGLE_SERVICE_ACCOUNT_B64."
    )


# -----------------------------
# Routes - Health
# -----------------------------
@app.get("/")
def home():
    return jsonify({"status": "ok", "message": "Contact Solution API is running"})


# -----------------------------
# Routes - Sheets test (READ)
# -----------------------------
@app.get("/test-sheets")
def test_sheets_read():
    try:
        if not SHEET_ID:
            return jsonify({"status": "error", "error": "SHEET_ID/GSHEET_ID ausente"}), 500

        service = get_sheets_service()
        result = service.spreadsheets().values().get(
            spreadsheetId=SHEET_ID,
            range=DEFAULT_READ_RANGE
        ).execute()

        return jsonify({"status": "ok", "values": result.get("values", [])})

    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500


# -----------------------------
# Routes - Sheets test (WRITE)
# -----------------------------
@app.get("/test-sheets-write")
def test_sheets_write():
    try:
        if not SHEET_ID:
            return jsonify({"status": "error", "error": "SHEET_ID/GSHEET_ID ausente"}), 500

        service = get_sheets_service()

        now = datetime.utcnow().isoformat()
        row = [
            now,
            "5511999999999",
            "vendas",
            "Teste",
            "teste@email.com",
            "iphone 13",
            "05068050",
            "quero orçamento",
        ]

        result = service.spreadsheets().values().append(
            spreadsheetId=SHEET_ID,
            range=DEFAULT_WRITE_RANGE,
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": [row]},
        ).execute()

        return jsonify({"status": "ok", "updates": result.get("updates", {})})

    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500


# -----------------------------
# WhatsApp webhook (placeholder)
# -----------------------------
@app.get("/webhook")
def webhook_verify():
    mode = request.args.get("hub.mode")
    token = request.args.get("hub.verify_token")
    challenge = request.args.get("hub.challenge")

    if mode == "subscribe" and token == VERIFY_TOKEN:
        return challenge, 200
    return "Forbidden", 403


@app.post("/webhook")
def webhook_receive():
    data = request.get_json(silent=True) or {}
    # Aqui você mantém sua lógica real do WhatsApp.
    # Este exemplo só responde OK.
    return jsonify({"status": "ok"}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
