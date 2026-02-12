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
# SQL - DDL (criação)
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

  -- fluxo
  step text not null default 'nome',
  status text not null default 'open', -- open | completed

  -- dados
  setor text default '',
  nome text default '',
  email text default '',
  produto text default '',

  -- CEPs
  cep_padrao text default '',
  cep_usado text default '',
  cep_alterado text default '', -- guarda último cep alternativo
  salvou_cep_padrao boolean not null default true,

  -- orçamento
  quote_number int not null default 0, -- incrementa a cada novo orçamento
  is_returning boolean not null default false,

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

# ---------------------------
# SQL - migração leve (para DBs antigos)
# ---------------------------
MIGRATIONS = [
    # companies
    "alter table companies add column if not exists sheet_id text;",
    "alter table companies add column if not exists sheet_tab text;",

    # conversations - colunas novas
    "alter table conversations add column if not exists step text not null default 'nome';",
    "alter table conversations add column if not exists status text not null default 'open';",
    "alter table conversations add column if not exists setor text default '';",
    "alter table conversations add column if not exists nome text default '';",
    "alter table conversations add column if not exists email text default '';",
    "alter table conversations add column if not exists produto text default '';",

    "alter table conversations add column if not exists cep_padrao text default '';",
    "alter table conversations add column if not exists cep_usado text default '';",
    "alter table conversations add column if not exists cep_alterado text default '';",
    "alter table conversations add column if not exists salvou_cep_padrao boolean not null default true;",

    "alter table conversations add column if not exists quote_number int not null default 0;",
    "alter table conversations add column if not exists is_returning boolean not null default false;",

    "alter table conversations add column if not exists updated_at timestamptz not null default now();",
    "alter table conversations add column if not exists created_at timestamptz not null default now();",
]

def db_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL ausente")
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)

def ensure_tables_and_migrate():
    if not DATABASE_URL:
        logger.warning("DATABASE_URL ausente; pulando criação/migração de tabelas.")
        return
    try:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(DDL)
                for q in MIGRATIONS:
                    try:
                        cur.execute(q)
                    except Exception as e:
                        logger.warning(f"Migração ignorada (pode já existir): {q} | {e}")
            conn.commit()
        logger.info("DB OK: tabelas garantidas + migração leve aplicada.")
    except Exception as e:
        logger.exception(f"Falha ao criar/migrar tabelas: {e}")

@app.on_event("startup")
def _startup():
    ensure_tables_and_migrate()

# ---------------------------
# Helpers - Sheets
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

    # range precisa ser compatível com a sua planilha nova
    rng = f"{sheet_tab}!A:M"

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
# Helpers - fluxo
# ---------------------------
def _is_valid_email(s: str) -> bool:
    s = (s or "").strip()
    return "@" in s and "." in s and len(s) >= 6

def _normalize_cep_digits(s: str) -> str:
    return "".join(ch for ch in (s or "") if ch.isdigit())

def _format_cep(digits: str) -> str:
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
    allowed = {
        "step","setor","nome","email","produto",
        "cep_padrao","cep_usado","cep_alterado","salvou_cep_padrao",
        "status","quote_number","is_returning"
    }
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
# Rotas
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
# Admin
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
# Webhook Verify (Meta)
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
# Webhook Multiempresa (POST)
# ---------------------------
@app.post("/webhook/{company_id}")
async def webhook_receive(company_id: str, request: Request):
    payload = await request.json()
    msg = extract_whatsapp_message(payload)
    if not msg:
        return {"status": "ignored"}

    phone = msg["from"]
    text = (msg["text"] or "").strip()
    now_iso = datetime.now(timezone.utc).isoformat()

    company = get_company(company_id)
    convo = upsert_conversation(company_id, phone)

    # Protege: nunca acessar chaves com []
    step = (convo.get("step") or "nome").strip()
    status = (convo.get("status") or "open").strip()

    greetings = {"oi", "olá", "ola", "bom dia", "boa tarde", "boa noite", "hello", "hi"}

    logger.info(f"[FLOW] company={company_id} phone={phone} step={step} status={status} text='{text}'")
    log_message(company_id, phone, "in", text)

    # ---------
    # Lead retornando (já tem cadastro)
    # ---------
    has_profile = bool((convo.get("nome") or "").strip()) and bool((convo.get("email") or "").strip())
    has_default_cep = bool((convo.get("cep_padrao") or "").strip())

    # Se já estava completed e mandou oi/ola: abrir novo orçamento sem pedir tudo de novo
    if status == "completed" and text.lower() in greetings and has_profile:
        new_quote = int(convo.get("quote_number") or 0) + 1
        convo = update_conversation(
            company_id, phone,
            status="open",
            is_returning=True,
            quote_number=new_quote,
            step="produto",
            produto="",
            cep_usado="",
            cep_alterado=""
        )
        reply = (
            f"Oi, {convo.get('nome','')} 👋\n"
            f"Vamos fazer um novo orçamento (#{new_quote}).\n"
            "Qual produto/serviço você quer orçar agora?"
        )
        log_message(company_id, phone, "out", reply)
        return {"status": "ok", "reply": reply}

    # Comandos simples para trocar cep a qualquer momento
    if text.lower().startswith("trocar cep"):
        convo = update_conversation(company_id, phone, step="cep_confirm", status="open")
        reply = "Beleza! Me envie o CEP (8 dígitos, só números) que você quer usar neste orçamento."
        log_message(company_id, phone, "out", reply)
        return {"status": "ok", "reply": reply}

    # ---------
    # Mensagem inicial
    # ---------
    if step == "nome" and not (convo.get("nome") or "").strip() and text.lower() in greetings:
        reply = "Olá! 👋 Tudo bem? Qual é o seu nome?"
        log_message(company_id, phone, "out", reply)
        return {"status": "ok", "reply": reply}

    # ---------
    # Etapa: NOME
    # ---------
    if step == "nome":
        if not text:
            reply = "Qual é o seu nome?"
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        convo = update_conversation(company_id, phone, nome=text, step="email", status="open")
        reply = f"Prazer, {convo.get('nome','')}! Qual é o seu e-mail?"
        log_message(company_id, phone, "out", reply)
        return {"status": "ok", "reply": reply}

    # ---------
    # Etapa: EMAIL
    # ---------
    if step == "email":
        if not _is_valid_email(text):
            reply = "Esse e-mail parece inválido 😅 Pode enviar novamente?"
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        convo = update_conversation(company_id, phone, email=text, step="produto", status="open")
        reply = "Perfeito! Qual produto/serviço você tem interesse?"
        log_message(company_id, phone, "out", reply)
        return {"status": "ok", "reply": reply}

    # ---------
    # Etapa: PRODUTO
    # ---------
    if step == "produto":
        if not text:
            reply = "Qual produto/serviço você tem interesse?"
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        convo = update_conversation(company_id, phone, produto=text, step="cep_confirm", status="open")

        # Se já tem cep padrão, confirma
        if has_default_cep:
            reply = (
                f"Show! Vou preparar a oferta de *{convo.get('produto','')}*.\n"
                f"Posso usar o CEP padrão {convo.get('cep_padrao','')}?\n"
                "Responda: **1** = sim | **2** = trocar CEP"
            )
        else:
            reply = "Boa! Agora me envie seu CEP (8 dígitos, só números) pra eu preparar a oferta certinha."
            convo = update_conversation(company_id, phone, step="cep", status="open")

        log_message(company_id, phone, "out", reply)
        return {"status": "ok", "reply": reply}

    # ---------
    # Etapa: CEP CONFIRM (1/2)
    # ---------
    if step == "cep_confirm":
        if text.strip() == "1" and has_default_cep:
            # usa o cep padrão como cep usado
            convo = update_conversation(
                company_id, phone,
                cep_usado=convo.get("cep_padrao",""),
                salvou_cep_padrao=True,
                step="finalize",
                status="open"
            )
        elif text.strip() == "2":
            convo = update_conversation(company_id, phone, step="cep", status="open")
            reply = "Beleza! Me envie o CEP (8 dígitos, só números) que você quer usar neste orçamento."
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}
        else:
            reply = "Não entendi 😅 Responda: **1** = usar CEP padrão | **2** = trocar CEP"
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

    # ---------
    # Etapa: CEP (recebe cep)
    # ---------
    if step == "cep":
        digits = _normalize_cep_digits(text)
        cep = _format_cep(digits)
        if not cep:
            reply = "CEP inválido. Envie apenas números (8 dígitos)."
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        # Se não tinha cep padrão, salva como padrão
        if not (convo.get("cep_padrao") or "").strip():
            convo = update_conversation(
                company_id, phone,
                cep_padrao=cep,
                cep_usado=cep,
                cep_alterado="",
                salvou_cep_padrao=True,
                step="finalize",
                status="open"
            )
        else:
            # tinha padrão; esse é um cep alternativo usado no orçamento
            convo = update_conversation(
                company_id, phone,
                cep_usado=cep,
                cep_alterado=cep,
                salvou_cep_padrao=False,
                step="finalize",
                status="open"
            )

    # ---------
    # Etapa: FINALIZE (export + só conclui se export OK)
    # ---------
    if step == "finalize":
        # Monta a linha exatamente no padrão da sua planilha:
        # created_at, company_id, phone, is_returning, quote_number, nome, email, produto,
        # cep_usado, cep_padrao, cep_alterado, salvou_cep_padrao, status
        is_returning = bool(convo.get("is_returning"))
        quote_number = int(convo.get("quote_number") or 0)

        row = [
            now_iso,                         # created_at
            company_id,                      # company_id
            phone,                           # phone
            "TRUE" if is_returning else "FALSE",   # is_returning
            quote_number,                    # quote_number
            convo.get("nome") or "",         # nome
            convo.get("email") or "",        # email
            convo.get("produto") or "",      # produto
            convo.get("cep_usado") or "",    # cep_usado
            convo.get("cep_padrao") or "",   # cep_padrao
            convo.get("cep_alterado") or "", # cep_alterado
            "TRUE" if convo.get("salvou_cep_padrao") else "FALSE",  # salvou_cep_padrao
            "completed",                     # status (vai virar completed se export OK)
        ]

        export_info = None

        # travar finalização: só completa se export for sucesso (quando Sheets estiver configurado)
        sheet_id = (company.get("sheet_id") or DEFAULT_SHEET_ID or "").strip()
        sheet_tab = (company.get("sheet_tab") or DEFAULT_SHEET_TAB or "Página1").strip()
        sheets_enabled = bool(sheet_id) and bool(GOOGLE_SA_B64)

        if sheets_enabled:
            try:
                export_info = append_to_sheets(sheet_id, sheet_tab, row)
            except Exception as e:
                logger.error(f"Falha no export pro Sheets (mantendo conversa aberta): {e}")
                # mantém no finalize pra tentar de novo
                convo = update_conversation(company_id, phone, step="finalize", status="open")
                reply = (
                    "Quase lá ✅\n"
                    "Consegui captar seus dados, mas tive um problema ao registrar na planilha.\n"
                    "Pode me mandar um **ok** para eu tentar registrar novamente?"
                )
                log_message(company_id, phone, "out", reply)
                return {"status": "ok", "reply": reply, "export": None, "export_error": str(e)}

        # Se sheets não está configurado, finaliza normalmente (MVP)
        convo = update_conversation(company_id, phone, status="completed", step="nome")

        # prepara próxima conversa: novo orçamento começa em "produto"
        reply = (
            f"Fechado, {convo.get('nome','')} ✅\n"
            f"Registrei seu interesse em *{convo.get('produto','')}*.\n"
            "Um vendedor vai te chamar em breve com uma oferta preparada pra você.\n\n"
            "Se quiser outro orçamento depois, é só mandar **oi** 😉"
        )
        log_message(company_id, phone, "out", reply)
        return {"status": "ok", "reply": reply, "export": export_info}

    # fallback: reset seguro
    convo = update_conversation(
        company_id, phone,
        step="nome",
        status="open",
        produto="",
        cep_usado="",
        cep_alterado="",
        is_returning=False
    )
    reply = "Vamos recomeçar 🙂 Qual é o seu nome?"
    log_message(company_id, phone, "out", reply)
    return {"status": "ok", "reply": reply}
