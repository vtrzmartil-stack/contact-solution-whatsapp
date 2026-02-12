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

# Sheets
DEFAULT_SHEET_ID = os.getenv("GSHEET_ID", "")
DEFAULT_SHEET_TAB = os.getenv("SHEET_TAB_NAME", "Página1")
GOOGLE_SA_B64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_B64", "")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Admin token (opcional)
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")

app = FastAPI(title="Contact Solution (Multi-Company + Customers/Quotes)")


# ---------------------------
# SQL - DDL
# ---------------------------
DDL = """
create table if not exists companies (
  id text primary key,
  name text not null,
  sheet_id text,
  sheet_tab text default 'Página1',
  created_at timestamptz not null default now()
);

-- Perfil padrão por telefone (nome/email/cep_padrao)
create table if not exists customers (
  id bigserial primary key,
  company_id text not null references companies(id) on delete cascade,
  phone text not null,
  nome text default '',
  email text default '',
  cep_padrao text default '',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique(company_id, phone)
);

-- Histórico de orçamentos
create table if not exists quotes (
  id bigserial primary key,
  company_id text not null references companies(id) on delete cascade,
  phone text not null,
  customer_id bigint references customers(id) on delete set null,
  quote_number int not null default 1,
  produto text default '',
  cep_usado text default '',
  cep_alterado boolean not null default false,
  salvou_cep_padrao boolean not null default false,
  status text not null default 'open', -- open|pending_export|completed
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  exported_at timestamptz
);

create index if not exists idx_quotes_company_phone_created
on quotes(company_id, phone, created_at desc);

-- Estado do bot por telefone (sessão atual)
create table if not exists conversations (
  id bigserial primary key,
  company_id text not null references companies(id) on delete cascade,
  phone text not null,
  step text not null default 'nome',  -- nome|email|produto|cep_confirm|cep|cep_save|export_retry
  quote_id bigint references quotes(id) on delete set null,
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

    # Agora temos mais colunas (A:M)
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
# Helpers - validações
# ---------------------------
def _is_valid_email(s: str) -> bool:
    s = (s or "").strip()
    return "@" in s and "." in s and len(s) >= 6


def _normalize_cep(s: str) -> str:
    digits = "".join(ch for ch in (s or "") if ch.isdigit())
    if len(digits) == 8:
        return f"{digits[:5]}-{digits[5:]}"
    return ""


def _is_yes(s: str) -> bool:
    s = (s or "").strip().lower()
    return s in {"sim", "s", "1", "ok", "yes", "y", "claro"}


def _is_no(s: str) -> bool:
    s = (s or "").strip().lower()
    return s in {"nao", "não", "n", "2", "no"}


def extract_whatsapp_message(payload: Dict[str, Any]) -> Optional[Dict[str, str]]:
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
# DB - auth/admin
# ---------------------------
def require_admin(request: Request):
    if not ADMIN_TOKEN:
        return
    token = request.headers.get("x-admin-token", "")
    if token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")


# ---------------------------
# DB - companies
# ---------------------------
def get_company(company_id: str) -> Dict[str, Any]:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("select * from companies where id = %s", (company_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="company_id não encontrado")
            return row


# ---------------------------
# DB - customers
# ---------------------------
def get_customer(company_id: str, phone: str) -> Optional[Dict[str, Any]]:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "select * from customers where company_id = %s and phone = %s",
                (company_id, phone),
            )
            return cur.fetchone()


def upsert_customer(company_id: str, phone: str, nome: str, email: str, cep_padrao: str) -> Dict[str, Any]:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into customers (company_id, phone, nome, email, cep_padrao)
                values (%s, %s, %s, %s, %s)
                on conflict (company_id, phone) do update set
                  nome = excluded.nome,
                  email = excluded.email,
                  cep_padrao = excluded.cep_padrao,
                  updated_at = now()
                returning *
                """,
                (company_id, phone, nome, email, cep_padrao),
            )
            row = cur.fetchone()
            conn.commit()
            return row


def update_customer_cep(company_id: str, phone: str, cep_padrao: str) -> Dict[str, Any]:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                update customers
                set cep_padrao = %s, updated_at = now()
                where company_id = %s and phone = %s
                returning *
                """,
                (cep_padrao, company_id, phone),
            )
            row = cur.fetchone()
            conn.commit()
            return row


def customer_is_complete(c: Dict[str, Any]) -> bool:
    if not c:
        return False
    return bool((c.get("nome") or "").strip()) and bool((c.get("email") or "").strip()) and bool((c.get("cep_padrao") or "").strip())


# ---------------------------
# DB - quotes
# ---------------------------
def get_last_quote_number(company_id: str, phone: str) -> int:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "select max(quote_number) as mx from quotes where company_id=%s and phone=%s",
                (company_id, phone),
            )
            row = cur.fetchone()
            mx = row["mx"] if row else None
            return int(mx or 0)


def create_quote(company_id: str, phone: str, customer_id: Optional[int], is_returning: bool) -> Dict[str, Any]:
    next_num = get_last_quote_number(company_id, phone) + 1
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into quotes (company_id, phone, customer_id, quote_number, status)
                values (%s, %s, %s, %s, 'open')
                returning *
                """,
                (company_id, phone, customer_id, next_num),
            )
            row = cur.fetchone()
            conn.commit()
            return row


def update_quote(quote_id: int, **fields) -> Dict[str, Any]:
    allowed = {"produto", "cep_usado", "cep_alterado", "salvou_cep_padrao", "status", "exported_at"}
    sets = []
    vals = []
    for k, v in fields.items():
        if k in allowed:
            sets.append(f"{k} = %s")
            vals.append(v)
    sets.append("updated_at = now()")
    vals.append(quote_id)

    q = f"update quotes set {', '.join(sets)} where id=%s returning *"
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(q, tuple(vals))
            row = cur.fetchone()
            conn.commit()
            return row


# ---------------------------
# DB - conversations (estado)
# ---------------------------
def get_conversation(company_id: str, phone: str) -> Optional[Dict[str, Any]]:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "select * from conversations where company_id=%s and phone=%s",
                (company_id, phone),
            )
            return cur.fetchone()


def upsert_conversation(company_id: str, phone: str, step: str, quote_id: Optional[int]) -> Dict[str, Any]:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into conversations (company_id, phone, step, quote_id)
                values (%s, %s, %s, %s)
                on conflict (company_id, phone) do update set
                  step = excluded.step,
                  quote_id = excluded.quote_id,
                  updated_at = now()
                returning *
                """,
                (company_id, phone, step, quote_id),
            )
            row = cur.fetchone()
            conn.commit()
            return row


def update_conversation(company_id: str, phone: str, **fields) -> Dict[str, Any]:
    allowed = {"step", "quote_id"}
    sets = []
    vals = []
    for k, v in fields.items():
        if k in allowed:
            sets.append(f"{k}=%s")
            vals.append(v)
    sets.append("updated_at = now()")
    vals.extend([company_id, phone])

    q = f"update conversations set {', '.join(sets)} where company_id=%s and phone=%s returning *"
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(q, tuple(vals))
            row = cur.fetchone()
            conn.commit()
            return row


def reset_conversation(company_id: str, phone: str) -> None:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "update conversations set step='produto', quote_id=null, updated_at=now() where company_id=%s and phone=%s",
                (company_id, phone),
            )
            conn.commit()


# ---------------------------
# DB - messages log
# ---------------------------
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
                select * from quotes
                where company_id = %s and status in ('completed','pending_export')
                order by created_at desc
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
# Webhook (POST) - fluxo com retorno + troca de CEP
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

    greetings = {"oi", "olá", "ola", "bom dia", "boa tarde", "boa noite", "hello", "hi", "orçamento", "orcamento"}
    lower = text.lower()

    logger.info(f"[FLOW] company={company_id} phone={phone} text='{text}'")
    log_message(company_id, phone, "in", text)

    customer = get_customer(company_id, phone)
    conv = get_conversation(company_id, phone)

    # Se não existe conversa ainda, cria uma nova sessão adequada
    if not conv:
        if customer and customer_is_complete(customer):
            # cliente já tem dados -> começar direto no produto
            quote = create_quote(company_id, phone, customer["id"], is_returning=True)
            conv = upsert_conversation(company_id, phone, step="produto", quote_id=quote["id"])
            reply = f"Olá, {customer['nome']}! 👋 Qual produto você quer orçar agora?"
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}
        else:
            # novo cliente -> coletar nome
            quote = create_quote(company_id, phone, customer["id"] if customer else None, is_returning=False)
            conv = upsert_conversation(company_id, phone, step="nome", quote_id=quote["id"])
            # se ele só cumprimentou
            if lower in greetings:
                reply = "Olá! 👋 Tudo bem? Qual é o seu nome?"
                log_message(company_id, phone, "out", reply)
                return {"status": "ok", "reply": reply}
            reply = "Qual é o seu nome?"
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

    step = conv["step"]
    quote_id = conv["quote_id"]

    # helper: carrega quote atual
    def load_quote(qid: int) -> Dict[str, Any]:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("select * from quotes where id=%s", (qid,))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=500, detail="quote não encontrada")
                return row

    quote = load_quote(quote_id) if quote_id else None

    # Se a conversa está em pending_export, tenta exportar de novo ao receber qualquer msg
    if quote and quote["status"] == "pending_export":
        try:
            sheet_id = company.get("sheet_id") or DEFAULT_SHEET_ID
            sheet_tab = company.get("sheet_tab") or DEFAULT_SHEET_TAB
            if sheet_id and GOOGLE_SA_B64:
                cust = get_customer(company_id, phone) or {}
                row = [
                    now_iso,                       # created_at
                    company_id,                    # company_id
                    phone,                         # phone
                    "true",                        # is_returning (quando existe perfil, tende a ser true, mas ok)
                    str(quote.get("quote_number") or 1),
                    cust.get("nome", ""),
                    cust.get("email", ""),
                    quote.get("produto", ""),
                    quote.get("cep_usado", ""),
                    cust.get("cep_padrao", ""),
                    "true" if quote.get("cep_alterado") else "false",
                    "true" if quote.get("salvou_cep_padrao") else "false",
                    "completed",
                ]
                append_to_sheets(sheet_id, sheet_tab, row)
                update_quote(quote["id"], status="completed", exported_at=datetime.now(timezone.utc))
                reset_conversation(company_id, phone)
                reply = "✅ Pronto! Consegui registrar seu orçamento agora. Um vendedor vai te chamar em breve."
                log_message(company_id, phone, "out", reply)
                return {"status": "ok", "reply": reply, "export": "ok"}
        except Exception as e:
            logger.error(f"Falha ao reexportar (mantendo pending_export): {e}")
            reply = "Ainda estou com dificuldade para registrar no sistema 😅 Tente novamente em instantes."
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply, "export": "failed"}

    # --- Etapa: NOME
    if step == "nome":
        if not text or lower in greetings:
            reply = "Qual é o seu nome?"
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        # guarda nome em memória temporária no customer (cria se não existir)
        if not customer:
            customer = upsert_customer(company_id, phone, nome=text, email="", cep_padrao="")
        else:
            customer = upsert_customer(company_id, phone, nome=text, email=customer.get("email", ""), cep_padrao=customer.get("cep_padrao", ""))

        update_conversation(company_id, phone, step="email")
        reply = f"Prazer, {customer['nome']}! Qual é o seu e-mail?"
        log_message(company_id, phone, "out", reply)
        return {"status": "ok", "reply": reply}

    # --- Etapa: EMAIL
    if step == "email":
        if not _is_valid_email(text):
            reply = "Esse e-mail parece inválido 😅 Pode enviar novamente?"
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        if not customer:
            customer = upsert_customer(company_id, phone, nome="", email=text, cep_padrao="")
        else:
            customer = upsert_customer(company_id, phone, nome=customer.get("nome", ""), email=text, cep_padrao=customer.get("cep_padrao", ""))

        update_conversation(company_id, phone, step="produto")
        reply = "Perfeito! Qual produto você tem interesse?"
        log_message(company_id, phone, "out", reply)
        return {"status": "ok", "reply": reply}

    # --- Etapa: PRODUTO
    if step == "produto":
        if not text or lower in greetings:
            # se já tem customer completo, só pergunta produto mesmo
            if customer and customer_is_complete(customer):
                reply = f"Qual produto você quer orçar agora, {customer['nome']}?"
            else:
                reply = "Qual produto você tem interesse?"
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        # atualiza quote produto
        quote = update_quote(quote_id, produto=text)

        # se já tem cep padrão, vai para confirmação. Senão pede cep direto.
        if customer and (customer.get("cep_padrao") or "").strip():
            update_conversation(company_id, phone, step="cep_confirm")
            reply = (
                f"Perfeito! Seu CEP padrão é *{customer['cep_padrao']}*.\n"
                "Responda *1* para confirmar ou envie um *novo CEP* (apenas números)."
            )
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        update_conversation(company_id, phone, step="cep")
        reply = "Boa! Agora me envie seu CEP (apenas números) pra eu preparar a oferta certinha."
        log_message(company_id, phone, "out", reply)
        return {"status": "ok", "reply": reply}

    # --- Etapa: CEP_CONFIRM (usar padrão ou trocar)
    if step == "cep_confirm":
        # confirmar padrão
        if _is_yes(text) or text.strip() == "1":
            cep_usado = (customer.get("cep_padrao") or "").strip()
            if not cep_usado:
                update_conversation(company_id, phone, step="cep")
                reply = "Não achei seu CEP padrão aqui 😅 Envie seu CEP (apenas números)."
                log_message(company_id, phone, "out", reply)
                return {"status": "ok", "reply": reply}

            update_quote(quote_id, cep_usado=cep_usado, cep_alterado=False)
            update_conversation(company_id, phone, step="export_retry")
            # cai para export
        else:
            # enviou outro cep
            cep = _normalize_cep(text)
            if not cep:
                reply = "CEP inválido. Envie apenas números (8 dígitos) ou responda 1 para usar o CEP padrão."
                log_message(company_id, phone, "out", reply)
                return {"status": "ok", "reply": reply}

            update_quote(quote_id, cep_usado=cep, cep_alterado=True, salvou_cep_padrao=False)
            update_conversation(company_id, phone, step="cep_save")
            reply = (
                f"Beleza! Vou usar o CEP *{cep}* nesse orçamento.\n"
                "Quer salvar esse CEP como *padrão* para próximos orçamentos? (sim/não)"
            )
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

    # --- Etapa: CEP (quando não existe padrão)
    if step == "cep":
        cep = _normalize_cep(text)
        if not cep:
            reply = "CEP inválido. Envie apenas números (8 dígitos)."
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        # salva no customer como padrão (primeira vez)
        if not customer:
            customer = upsert_customer(company_id, phone, nome="", email="", cep_padrao=cep)
        else:
            customer = upsert_customer(company_id, phone, nome=customer.get("nome",""), email=customer.get("email",""), cep_padrao=cep)

        update_quote(quote_id, cep_usado=cep, cep_alterado=False, salvou_cep_padrao=True)
        update_conversation(company_id, phone, step="export_retry")
        # cai para export

    # --- Etapa: CEP_SAVE (pergunta se quer salvar novo cep como padrão)
    if step == "cep_save":
        if _is_yes(text):
            # salva novo cep como padrão
            q = load_quote(quote_id)
            new_cep = q.get("cep_usado") or ""
            if new_cep:
                customer = update_customer_cep(company_id, phone, new_cep)
                update_quote(quote_id, salvou_cep_padrao=True)
            update_conversation(company_id, phone, step="export_retry")
        elif _is_no(text):
            update_conversation(company_id, phone, step="export_retry")
        else:
            reply = "Responda *sim* ou *não*: quer salvar esse CEP como padrão?"
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

    # --- Etapa: EXPORT_RETRY (tenta exportar e só finaliza se gravar)
    if step == "export_retry":
        # garante customer completo (mínimo)
        customer = get_customer(company_id, phone) or {}
        quote = load_quote(quote_id)

        # se ainda não tem dados obrigatórios, volta para o fluxo correto
        if not (customer.get("nome") and customer.get("email") and (quote.get("produto") or "") and (quote.get("cep_usado") or "")):
            # fallback para não “quebrar”
            update_conversation(company_id, phone, step="produto")
            reply = "Vamos continuar 🙂 Qual produto você quer orçar?"
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        # tenta export
        try:
            sheet_id = company.get("sheet_id") or DEFAULT_SHEET_ID
            sheet_tab = company.get("sheet_tab") or DEFAULT_SHEET_TAB

            if not (sheet_id and GOOGLE_SA_B64):
                # Se não tiver Sheets configurado, ainda assim finaliza no DB.
                update_quote(quote_id, status="completed")
                reset_conversation(company_id, phone)
                reply = (
                    f"Fechado, {customer.get('nome','')} ✅\n"
                    f"Já registrei seu interesse em *{quote.get('produto','')}*.\n"
                    "Um vendedor vai te chamar em breve com uma oferta preparada pra você."
                )
                log_message(company_id, phone, "out", reply)
                return {"status": "ok", "reply": reply, "export": None}

            is_returning = "true" if get_last_quote_number(company_id, phone) > 1 else "false"
            row = [
                now_iso,                                   # created_at
                company_id,                                # company_id
                phone,                                     # phone
                is_returning,                              # is_returning
                str(quote.get("quote_number") or 1),       # quote_number
                customer.get("nome", ""),                  # nome
                customer.get("email", ""),                 # email
                quote.get("produto", ""),                  # produto
                quote.get("cep_usado", ""),                # cep_usado
                customer.get("cep_padrao", ""),            # cep_padrao
                "true" if quote.get("cep_alterado") else "false",
                "true" if quote.get("salvou_cep_padrao") else "false",
                "completed",
            ]

            append_to_sheets(sheet_id, sheet_tab, row)

            # só agora finaliza e reseta
            update_quote(quote_id, status="completed", exported_at=datetime.now(timezone.utc))
            reset_conversation(company_id, phone)

            reply = (
                f"Fechado, {customer.get('nome','')} ✅\n"
                f"Já registrei seu interesse em *{quote.get('produto','')}*.\n"
                "Um vendedor vai te chamar em breve com uma oferta preparada pra você."
            )
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply, "export": "ok"}

        except Exception as e:
            # NÃO reseta conversa; deixa pendente para tentar de novo
            logger.error(f"Falha no export pro Sheets: {e}")
            update_quote(quote_id, status="pending_export")
            reply = (
                "Quase lá ✅\n"
                "Tive um problema ao registrar seu orçamento no sistema.\n"
                "Pode me mandar qualquer mensagem daqui a pouco que eu tento de novo automaticamente."
            )
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply, "export": "failed"}

    # fallback de segurança
    update_conversation(company_id, phone, step="produto")
    reply = "Vamos seguir 🙂 Qual produto você quer orçar?"
    log_message(company_id, phone, "out", reply)
    return {"status": "ok", "reply": reply}
