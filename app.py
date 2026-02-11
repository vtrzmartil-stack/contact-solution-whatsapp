import os
import json
import base64
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse, PlainTextResponse

import psycopg
from psycopg.rows import dict_row

from google.oauth2 import service_account
from googleapiclient.discovery import build


# ---------------------------
# Logging
# ---------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("contact-solution")


# ---------------------------
# Env
# ---------------------------
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "")

# DB
DATABASE_URL = os.getenv("DATABASE_URL", "")

# Sheets (fallback / opcional)
DEFAULT_SHEET_ID = os.getenv("GSHEET_ID", "")
DEFAULT_SHEET_TAB = os.getenv("SHEET_TAB_NAME", "Página1")
GOOGLE_SA_B64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_B64", "")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Admin token (opcional, mas recomendado)
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")

app = FastAPI(title="Contact Solution (Multi-Company)")

# ---------------------------
# SQL - DDL (criação automática)
# ---------------------------
DDL = """
create table if not exists companies (
  id text primary key,
  name text not null,
  sheet_id text,
  sheet_tab text default 'Página1',
  created_at timestamptz not null default now()
);

create table if not exists conversations (
  id bigserial primary key,
  company_id text not null references companies(id) on delete cascade,
  phone text not null,
  step text not null default 'nome',
  setor text default '',
  nome text default '',
  email text default '',
  produto text default '',
  cep text default '',
  status text not null default 'open',
  updated_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  unique(company_id, phone)
);

create table if not exists messages (
  id bigserial primary key,
  company_id text not null references companies(id) on delete cascade,
  phone text not null,
  direction text not null, -- 'in' | 'out'
  text text not null,
  created_at timestamptz not null default now()
);

create index if not exists idx_messages_company_phone_created
on messages(company_id, phone, created_at desc);
"""


def db_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL ausente")
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def ensure_tables():
    if not DATABASE_URL:
        logger.warning("DATABASE_URL ausente; pulando criação de tabelas.")
        return
    try:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(DDL)
            conn.commit()
        logger.info("DB OK: tabelas garantidas (ensure_tables).")
    except Exception as e:
        logger.exception(f"Falha ao criar/verificar tabelas: {e}")


@app.on_event("startup")
def _startup():
    ensure_tables()


# ---------------------------
# Helpers - Sheets (opcional / export)
# ---------------------------
def _normalize_b64(s: str) -> str:
    s = (s or "").strip().replace("\n", "").replace("\r", "").replace(" ", "")
    missing = len(s) % 4
    if missing:
        s += "=" * (4 - missing)
    return s


def _get_sheets_service():
    if not GOOGLE_SA_B64:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_B64 ausente")

    b64 = _normalize_b64(GOOGLE_SA_B64)
    raw = base64.b64decode(b64).decode("utf-8")
    info = json.loads(raw)

    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def append_to_sheets(sheet_id: str, sheet_tab: str, row: List[Any]) -> Dict[str, Any]:
    if not sheet_id:
        raise RuntimeError("sheet_id ausente para export")
    sheet_tab = sheet_tab or "Página1"
    rng = f"{sheet_tab}!A:G"

    service = _get_sheets_service()
    body = {"values": [row]}
    result = (
        service.spreadsheets()
        .values()
        .append(
            spreadsheetId=sheet_id,
            range=rng,
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body=body,
        )
        .execute()
    )
    updates = result.get("updates", {})
    return {"updatedRange": updates.get("updatedRange"), "updatedRows": updates.get("updatedRows")}


# ---------------------------
# Helpers - fluxo / validações
# ---------------------------
def _is_valid_email(s: str) -> bool:
    s = (s or "").strip()
    return "@" in s and "." in s and len(s) >= 6


def _normalize_cep(s: str) -> str:
    digits = "".join(ch for ch in (s or "") if ch.isdigit())
    if len(digits) == 8:
        return f"{digits[:5]}-{digits[5:]}"
    return ""


def extract_whatsapp_message(payload: Dict[str, Any]) -> Optional[Dict[str, str]]:
    """
    Payload no formato WhatsApp Cloud API (ou simulado via Postman).
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


# ---------------------------
# DB - operações
# ---------------------------
def require_admin(request: Request):
    if not ADMIN_TOKEN:
        # se não configurou, deixa aberto (MVP). Recomendado configurar.
        return
    token = request.headers.get("x-admin-token", "")
    if token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")


def get_company(company_id: str) -> Dict[str, Any]:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("select * from companies where id = %s", (company_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="company_id não encontrado")
            return row


def upsert_conversation(company_id: str, phone: str) -> Dict[str, Any]:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into conversations (company_id, phone)
                values (%s, %s)
                on conflict (company_id, phone) do update
                set updated_at = now()
                returning *
                """,
                (company_id, phone),
            )
            row = cur.fetchone()
            conn.commit()
            return row


def update_conversation(company_id: str, phone: str, **fields) -> Dict[str, Any]:
    allowed = {"step", "setor", "nome", "email", "produto", "cep", "status"}
    sets = []
    vals = []
    for k, v in fields.items():
        if k in allowed:
            sets.append(f"{k} = %s")
            vals.append(v)
    sets.append("updated_at = now()")
    vals.extend([company_id, phone])

    q = f"""
    update conversations
    set {", ".join(sets)}
    where company_id = %s and phone = %s
    returning *
    """
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(q, tuple(vals))
            row = cur.fetchone()
            conn.commit()
            return row


def log_message(company_id: str, phone: str, direction: str, text: str) -> None:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "insert into messages (company_id, phone, direction, text) values (%s, %s, %s, %s)",
                (company_id, phone, direction, text),
            )
            conn.commit()


# ---------------------------
# Rotas básicas
# ---------------------------
@app.get("/")
def root():
    return {
        "status": "ok",
        "service": "contact-solution-multi",
        "endpoints": [
            "/health",
            "/webhook/{company_id}",
            "/admin/companies",
            "/admin/leads/{company_id}",
        ],
    }


@app.get("/health")
def health():
    return {"status": "ok"}


# ---------------------------
# Admin (MVP)
# ---------------------------
@app.post("/admin/companies")
async def admin_create_company(request: Request):
    require_admin(request)
    body = await request.json()

    company_id = (body.get("id") or "").strip()
    name = (body.get("name") or "").strip()
    sheet_id = (body.get("sheet_id") or DEFAULT_SHEET_ID or "").strip()
    sheet_tab = (body.get("sheet_tab") or DEFAULT_SHEET_TAB or "Página1").strip()

    if not company_id or not name:
        return JSONResponse(status_code=400, content={"status": "error", "error": "id e name são obrigatórios"})

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into companies (id, name, sheet_id, sheet_tab)
                values (%s, %s, %s, %s)
                on conflict (id) do update set
                  name = excluded.name,
                  sheet_id = excluded.sheet_id,
                  sheet_tab = excluded.sheet_tab
                returning *
                """,
                (company_id, name, sheet_id, sheet_tab),
            )
            row = cur.fetchone()
            conn.commit()

    return {"status": "ok", "company": row}


@app.get("/admin/companies")
def admin_list_companies(request: Request):
    require_admin(request)
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("select * from companies order by created_at desc")
            rows = cur.fetchall()
    return {"status": "ok", "companies": rows}


@app.get("/admin/leads/{company_id}")
def admin_list_leads(company_id: str, request: Request):
    require_admin(request)
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select * from conversations
                where company_id = %s and status = 'completed'
                order by updated_at desc
                limit 200
                """,
                (company_id,),
            )
            rows = cur.fetchall()
    return {"status": "ok", "leads": rows}


# ---------------------------
# Webhook Verify (Meta) - opcional para futuro WhatsApp Cloud
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


# ---------------------------
# Webhook Multiempresa (POST) - fluxo por etapas (resposta via JSON)
# ---------------------------
@app.post("/webhook/{company_id}")
async def webhook_receive(company_id: str, request: Request):
    payload = await request.json()
    msg = extract_whatsapp_message(payload)

    if not msg:
        return {"status": "ignored"}

    phone = msg["from"]
    text = (msg["text"] or "").strip()
    now = datetime.now(timezone.utc).isoformat()

    # garante que empresa existe
    company = get_company(company_id)

    # garante conversa no DB
    convo = upsert_conversation(company_id, phone)

    step = convo["step"]
    greetings = {"oi", "olá", "ola", "bom dia", "boa tarde", "boa noite", "hello", "hi"}

    logger.info(f"[FLOW] company={company_id} phone={phone} step={step} text='{text}'")
    log_message(company_id, phone, "in", text)

    # Mensagem inicial
    if step == "nome" and not convo["nome"] and text.lower() in greetings:
        reply = "Olá! 👋 Tudo bem? Qual é o seu nome?"
        log_message(company_id, phone, "out", reply)
        return {"status": "ok", "reply": reply}

    # Etapa: NOME
    if step == "nome":
        if not text:
            reply = "Qual é o seu nome?"
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        convo = update_conversation(company_id, phone, nome=text, step="email")
        reply = f"Prazer, {convo['nome']}! Qual é o seu e-mail?"
        log_message(company_id, phone, "out", reply)
        return {"status": "ok", "reply": reply}

    # Etapa: EMAIL
    if step == "email":
        if not _is_valid_email(text):
            reply = "Esse e-mail parece inválido 😅 Pode enviar novamente?"
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        convo = update_conversation(company_id, phone, email=text, step="produto")
        reply = "Perfeito! Qual produto você tem interesse?"
        log_message(company_id, phone, "out", reply)
        return {"status": "ok", "reply": reply}

    # Etapa: PRODUTO
    if step == "produto":
        if not text:
            reply = "Qual produto você tem interesse?"
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        convo = update_conversation(company_id, phone, produto=text, step="cep")
        reply = "Boa! Agora me envie seu CEP (apenas números) pra eu preparar a oferta certinha."
        log_message(company_id, phone, "out", reply)
        return {"status": "ok", "reply": reply}

    # Etapa: CEP + FINALIZA
    if step == "cep":
        cep = _normalize_cep(text)
        if not cep:
            reply = "CEP inválido. Envie apenas números (8 dígitos)."
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        convo = update_conversation(company_id, phone, cep=cep, status="completed")

        # Export opcional pro Sheets (se tiver credencial + sheet_id configurado)
        export_info = None
        try:
            sheet_id = company.get("sheet_id") or DEFAULT_SHEET_ID
            sheet_tab = company.get("sheet_tab") or DEFAULT_SHEET_TAB

            if sheet_id and GOOGLE_SA_B64:
                row = [
                    now,
                    phone,
                    convo.get("setor") or "",
                    convo.get("nome") or "",
                    convo.get("email") or "",
                    convo.get("produto") or "",
                    convo.get("cep") or "",
                ]
                export_info = append_to_sheets(sheet_id, sheet_tab, row)
        except Exception as e:
            logger.error(f"Falha no export pro Sheets (ignorado): {e}")

        reply = (
            f"Fechado, {convo.get('nome','')} ✅\n"
            f"Já registrei seu interesse em *{convo.get('produto','')}*.\n"
            "Um vendedor vai te chamar em breve com uma oferta preparada pra você."
        )
        log_message(company_id, phone, "out", reply)
        return {"status": "ok", "reply": reply, "export": export_info}

    # fallback: reset
    update_conversation(company_id, phone, step="nome", nome="", email="", produto="", cep="", status="open")
    reply = "Vamos recomeçar 🙂 Qual é o seu nome?"
    log_message(company_id, phone, "out", reply)
    return {"status": "ok", "reply": reply}
