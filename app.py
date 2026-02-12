import os
import json
import base64
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

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

DATABASE_URL = os.getenv("DATABASE_URL", "")

# Sheets (fallback / opcional)
DEFAULT_SHEET_ID = os.getenv("GSHEET_ID", "")
DEFAULT_SHEET_TAB = os.getenv("SHEET_TAB_NAME", "Página1")
GOOGLE_SA_B64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_B64", "")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Admin token (opcional, mas recomendado)
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")

app = FastAPI(title="Contact Solution (Multi-Company)")


# =========================================================
# Utils - normalização (evitar erro de espaço/caps/acentos)
# =========================================================
def norm_text(s: Any) -> str:
    """Normaliza texto pra comparação (trim + lower)."""
    return (str(s) if s is not None else "").strip().lower()


def norm_phone(s: Any) -> str:
    return "".join(ch for ch in (str(s) if s is not None else "") if ch.isdigit())


def norm_sheet_tab(s: Any) -> str:
    # Mantém acentos, mas remove espaços extras e normaliza string vazia.
    tab = (str(s) if s is not None else "").strip()
    return tab or "Página1"


def step_pack(kind: str, *parts: str) -> str:
    """
    Empacota step com separador fixo e seguro.
    Usamos ':::' para diminuir chance do usuário digitar igual.
    """
    k = norm_text(kind)
    cleaned = [p.replace(":::", " ").strip() for p in parts if p is not None]
    return ":::".join([k] + cleaned)


def step_unpack(step: str) -> Tuple[str, List[str]]:
    """Desempacota step (tolerante a caps/espacos)."""
    raw = (step or "").strip()
    if ":::".join(["", ""]) in raw:  # improvável, mas só pra evitar edge.
        raw = raw.replace("::::::", ":::")
    parts = raw.split(":::") if "::: " not in raw else raw.replace("::: ", ":::").split(":::")
    kind = norm_text(parts[0]) if parts else ""
    rest = [p.strip() for p in parts[1:]] if len(parts) > 1 else []
    return kind, rest


# ---------------------------
# DB helpers
# ---------------------------
def db_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL ausente")
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def _exec_many(cur, sql: str):
    """Executa múltiplos statements separados por ';'."""
    parts = [p.strip() for p in (sql or "").split(";") if p.strip()]
    for stmt in parts:
        cur.execute(stmt)


def ensure_tables_and_migrate():
    """
    - Cria tabelas se não existirem
    - Migra colunas se DB já tinha versão antiga
    - IMPORTANTE: migra QUOTES também (isso evita o erro is_returning não existir)
    """
    if not DATABASE_URL:
        logger.warning("DATABASE_URL ausente; pulando criação/migração de tabelas.")
        return

    ddl = """
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
      nome text default '',
      email text default '',
      cep_padrao text default '',
      status text not null default 'open', -- open | completed
      updated_at timestamptz not null default now(),
      created_at timestamptz not null default now(),
      unique(company_id, phone)
    );

    create table if not exists quotes (
      id bigserial primary key,
      company_id text not null references companies(id) on delete cascade,
      phone text not null,
      quote_number int not null,
      produto text not null default '',
      cep_usado text not null default '',
      cep_alterado boolean not null default false,
      salvou_cep_padrao boolean not null default false,
      is_returning boolean not null default false,
      status text not null default 'ok', -- ok | error
      created_at timestamptz not null default now(),
      unique(company_id, phone, quote_number)
    );

    create table if not exists messages (
      id bigserial primary key,
      company_id text not null references companies(id) on delete cascade,
      phone text not null,
      direction text not null, -- in | out
      text text not null,
      created_at timestamptz not null default now()
    );

    create index if not exists idx_messages_company_phone_created
    on messages(company_id, phone, created_at desc);

    create index if not exists idx_quotes_company_phone_created
    on quotes(company_id, phone, created_at desc);
    """

    migrations = [
        # conversations
        "alter table conversations add column if not exists step text not null default 'nome'",
        "alter table conversations add column if not exists nome text default ''",
        "alter table conversations add column if not exists email text default ''",
        "alter table conversations add column if not exists cep_padrao text default ''",
        "alter table conversations add column if not exists status text not null default 'open'",
        "alter table conversations add column if not exists updated_at timestamptz not null default now()",
        "alter table conversations add column if not exists created_at timestamptz not null default now()",

        # quotes (o que faltava antes!)
        "alter table quotes add column if not exists quote_number int",
        "alter table quotes add column if not exists produto text not null default ''",
        "alter table quotes add column if not exists cep_usado text not null default ''",
        "alter table quotes add column if not exists cep_alterado boolean not null default false",
        "alter table quotes add column if not exists salvou_cep_padrao boolean not null default false",
        "alter table quotes add column if not exists is_returning boolean not null default false",
        "alter table quotes add column if not exists status text not null default 'ok'",
        "alter table quotes add column if not exists created_at timestamptz not null default now()",
    ]

    try:
        with db_conn() as conn:
            with conn.cursor() as cur:
                _exec_many(cur, ddl)
                for m in migrations:
                    cur.execute(m)

                # garante quote_number NOT NULL (caso exista nulo em DB antigo)
                cur.execute("update quotes set quote_number = 1 where quote_number is null")
                cur.execute("alter table quotes alter column quote_number set not null")

            conn.commit()
        logger.info("DB OK: tabelas garantidas + migrações aplicadas.")
    except Exception as e:
        logger.exception(f"Falha ao criar/verificar tabelas: {e}")


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
    """
    Exporta para A:M (13 colunas), de acordo com a planilha nova.
    """
    if not sheet_id:
        raise RuntimeError("sheet_id ausente para export")

    sheet_tab = norm_sheet_tab(sheet_tab)
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
    return {
        "updatedRange": updates.get("updatedRange"),
        "updatedRows": updates.get("updatedRows"),
    }


# ---------------------------
# Helpers - validações
# ---------------------------
def _is_valid_email(s: str) -> bool:
    s = (s or "").strip()
    return "@" in s and "." in s and len(s) >= 6


def _normalize_cep_digits_only(s: str) -> str:
    return "".join(ch for ch in (s or "") if ch.isdigit())


def _normalize_cep(s: str) -> str:
    digits = _normalize_cep_digits_only(s)
    if len(digits) == 8:
        return f"{digits[:5]}-{digits[5:]}"
    return ""


def extract_whatsapp_message(payload: Dict[str, Any]) -> Optional[Dict[str, str]]:
    """
    Payload no formato WhatsApp Cloud API (ou simulado via Postman).
    """
    try:
        entry = (payload.get("entry") or [])[0]
        changes = (entry.get("changes") or [])[0]
        value = changes.get("value") or {}
        messages = value.get("messages") or []
        if not messages:
            return None
        msg = messages[0] or {}
        sender = (msg.get("from") or "").strip()
        text = ((msg.get("text") or {}).get("body") or "").strip()
        if not sender:
            return None
        return {"from": sender, "text": text}
    except Exception:
        return None


# ---------------------------
# DB - operações
# ---------------------------
def require_admin(request: Request):
    if not ADMIN_TOKEN:
        return  # MVP: aberto se não configurar
    token = (request.headers.get("x-admin-token") or "").strip()
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
    allowed = {"step", "nome", "email", "cep_padrao", "status"}
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


def get_next_quote_number(company_id: str, phone: str) -> int:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "select coalesce(max(quote_number), 0) as mx from quotes where company_id=%s and phone=%s",
                (company_id, phone),
            )
            row = cur.fetchone()
            mx = int((row or {}).get("mx") or 0)
            return mx + 1


def insert_quote(
    company_id: str,
    phone: str,
    quote_number: int,
    produto: str,
    cep_usado: str,
    cep_alterado: bool,
    salvou_cep_padrao: bool,
    is_returning: bool,
    status: str = "ok",
) -> Dict[str, Any]:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into quotes
                  (company_id, phone, quote_number, produto, cep_usado, cep_alterado, salvou_cep_padrao, is_returning, status)
                values
                  (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                returning *
                """,
                (
                    company_id,
                    phone,
                    int(quote_number),
                    (produto or "").strip(),
                    (cep_usado or "").strip(),
                    bool(cep_alterado),
                    bool(salvou_cep_padrao),
                    bool(is_returning),
                    (status or "ok").strip(),
                ),
            )
            row = cur.fetchone()
            conn.commit()
            return row


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
            "/admin/quotes/{company_id}",
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
    sheet_tab = norm_sheet_tab(body.get("sheet_tab") or DEFAULT_SHEET_TAB)

    if not company_id or not name:
        return JSONResponse(
            status_code=400,
            content={"status": "error", "error": "id e name são obrigatórios"},
        )

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
    """Perfis (conversations) completados."""
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


@app.get("/admin/quotes/{company_id}")
def admin_list_quotes(company_id: str, request: Request):
    """Orçamentos registrados."""
    require_admin(request)
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select * from quotes
                where company_id = %s
                order by created_at desc
                limit 500
                """,
                (company_id,),
            )
            rows = cur.fetchall()
    return {"status": "ok", "quotes": rows}


# ---------------------------
# Webhook Verify (Meta) - opcional
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


# =========================================================
# Webhook Multiempresa (POST) - fluxo robusto (caps/espaço)
# =========================================================
@app.post("/webhook/{company_id}")
async def webhook_receive(company_id: str, request: Request):
    payload = await request.json()
    msg = extract_whatsapp_message(payload)

    if not msg:
        return {"status": "ignored"}

    phone = norm_phone(msg["from"])
    text_raw = (msg["text"] or "")
    text = text_raw.strip()
    text_n = norm_text(text_raw)

    now_iso = datetime.now(timezone.utc).isoformat()

    company = get_company(company_id)

    convo = upsert_conversation(company_id, phone)

    # step sempre normalizado pra comparar, mas preservamos o valor real no DB
    step_raw = (convo.get("step") or "nome").strip()
    step_kind, step_args = step_unpack(step_raw)

    greetings = {"oi", "olá", "ola", "bom dia", "boa tarde", "boa noite", "hello", "hi"}

    logger.info(
        f"[FLOW] company={company_id} phone={phone} step={step_raw} status={convo.get('status')} text='{text}'"
    )
    log_message(company_id, phone, "in", text)

    is_completed = (convo.get("status") == "completed")
    has_profile = bool((convo.get("nome") or "").strip()) and bool((convo.get("email") or "").strip())
    cep_padrao = (convo.get("cep_padrao") or "").strip()

    # Se já completou e mandar "oi"/mensagem curta: vai direto pro produto
    if is_completed and step_kind not in {"produto", "cep_confirm", "cep", "cep_save"}:
        convo = update_conversation(company_id, phone, step=step_pack("produto"), status="open")
        step_kind, step_args = "produto", []

    # ---------------------------
    # Step: NOME
    # ---------------------------
    if step_kind in {"", "nome"}:
        if text_n in greetings:
            reply = "Olá! 👋 Tudo bem? Qual é o seu nome?"
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        if not text:
            reply = "Qual é o seu nome?"
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        convo = update_conversation(company_id, phone, nome=text, step=step_pack("email"), status="open")
        reply = f"Prazer, {convo.get('nome','')}! Qual é o seu e-mail?"
        log_message(company_id, phone, "out", reply)
        return {"status": "ok", "reply": reply}

    # ---------------------------
    # Step: EMAIL
    # ---------------------------
    if step_kind == "email":
        if not _is_valid_email(text):
            reply = "Esse e-mail parece inválido 😅 Pode enviar novamente?"
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        convo = update_conversation(company_id, phone, email=text, step=step_pack("produto"), status="open")
        reply = "Perfeito! Qual serviço/produto você tem interesse?"
        log_message(company_id, phone, "out", reply)
        return {"status": "ok", "reply": reply}

    # ---------------------------
    # Step: PRODUTO
    # ---------------------------
    if step_kind == "produto":
        if not text or text_n in greetings:
            if is_completed and has_profile:
                reply = f"Olá, {convo.get('nome','')}! 😄 Qual serviço/produto você quer orçar agora?"
            else:
                reply = "Qual serviço/produto você tem interesse?"
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        produto = text.strip()

        if cep_padrao:
            convo = update_conversation(company_id, phone, step=step_pack("cep_confirm", produto), status="open")
            reply = (
                f"Show! Vou preparar o orçamento de *{produto}*.\n"
                f"Quer usar o seu CEP padrão *{cep_padrao}*?\n"
                "Responda:\n"
                "1 = Sim (usar padrão)\n"
                "2 = Não (informar outro CEP)"
            )
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        convo = update_conversation(company_id, phone, step=step_pack("cep", produto), status="open")
        reply = "Perfeito! Agora me envie seu CEP (apenas números) pra eu preparar a oferta certinha."
        log_message(company_id, phone, "out", reply)
        return {"status": "ok", "reply": reply}

    # ---------------------------
    # Step: CEP_CONFIRM
    # ---------------------------
    if step_kind == "cep_confirm":
        produto = (step_args[0] if step_args else "").strip()
        if not produto:
            convo = update_conversation(company_id, phone, step=step_pack("produto"), status="open")
            reply = "Vamos seguir 🙂 Qual serviço/produto você quer orçar?"
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        # aceita " 1 " / "Sim" / "s" etc.
        ans = norm_text(text)
        if ans in {"1", "sim", "s", "yes", "y"}:
            # usa cep_padrao e finaliza
            return await _finalize_quote(
                company_id=company_id,
                phone=phone,
                company=company,
                convo=convo,
                produto=produto,
                cep_usado=cep_padrao,
                cep_alterado=False,
                salvou_cep_padrao=False,
                is_returning=is_completed and has_profile,
                now_iso=now_iso,
            )

        if ans in {"2", "nao", "não", "n", "no"}:
            convo = update_conversation(company_id, phone, step=step_pack("cep", produto), status="open")
            reply = "Beleza. Me envie o CEP (8 dígitos, só números)."
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        reply = "Me responde com 1 (usar CEP padrão) ou 2 (informar outro CEP)."
        log_message(company_id, phone, "out", reply)
        return {"status": "ok", "reply": reply}

    # ---------------------------
    # Step: CEP
    # ---------------------------
    if step_kind == "cep":
        produto = (step_args[0] if step_args else "").strip()
        if not produto:
            convo = update_conversation(company_id, phone, step=step_pack("produto"), status="open")
            reply = "Vamos seguir 🙂 Qual serviço/produto você quer orçar?"
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        cep_fmt = _normalize_cep(text)
        if not cep_fmt:
            reply = "CEP inválido. Envie apenas números (8 dígitos)."
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        # se já tinha cep_padrao e mudou, pergunta se quer salvar como padrão
        if cep_padrao and cep_fmt != cep_padrao:
            convo = update_conversation(company_id, phone, step=step_pack("cep_save", produto, cep_fmt), status="open")
            reply = (
                f"Entendi ✅ Vou usar o CEP *{cep_fmt}*.\n"
                "Quer salvar esse CEP como seu novo CEP padrão?\n"
                "1 = Sim\n"
                "2 = Não"
            )
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        # primeira vez (sem cep_padrao) -> oferecer salvar
        if not cep_padrao:
            convo = update_conversation(company_id, phone, step=step_pack("cep_save", produto, cep_fmt), status="open")
            reply = (
                f"Perfeito ✅ Vou usar o CEP *{cep_fmt}*.\n"
                "Quer salvar esse CEP como padrão para próximos orçamentos?\n"
                "1 = Sim\n"
                "2 = Não"
            )
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        # cep igual ao padrão -> finaliza
        return await _finalize_quote(
            company_id=company_id,
            phone=phone,
            company=company,
            convo=convo,
            produto=produto,
            cep_usado=cep_fmt,
            cep_alterado=False,
            salvou_cep_padrao=False,
            is_returning=is_completed and has_profile,
            now_iso=now_iso,
        )

    # ---------------------------
    # Step: CEP_SAVE
    # ---------------------------
    if step_kind == "cep_save":
        produto = (step_args[0] if len(step_args) >= 1 else "").strip()
        cep_fmt = (step_args[1] if len(step_args) >= 2 else "").strip()

        if not produto or not cep_fmt:
            convo = update_conversation(company_id, phone, step=step_pack("produto"), status="open")
            reply = "Vamos seguir 🙂 Qual serviço/produto você quer orçar?"
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        ans = norm_text(text)
        if ans not in {"1", "2", "sim", "s", "não", "nao", "n", "yes", "y", "no"}:
            reply = "Me responde com 1 (salvar como padrão) ou 2 (não salvar)."
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        salvou = ans in {"1", "sim", "s", "yes", "y"}
        cep_alterado = bool(cep_padrao) and (cep_fmt != cep_padrao)

        if salvou:
            convo = update_conversation(company_id, phone, cep_padrao=cep_fmt, status="open")
        else:
            convo = update_conversation(company_id, phone, status="open")

        return await _finalize_quote(
            company_id=company_id,
            phone=phone,
            company=company,
            convo=convo,
            produto=produto,
            cep_usado=cep_fmt,
            cep_alterado=cep_alterado,
            salvou_cep_padrao=salvou,
            is_returning=is_completed and has_profile,
            now_iso=now_iso,
        )

    # fallback seguro
    convo = update_conversation(company_id, phone, step=step_pack("nome"), status="open")
    reply = "Vamos recomeçar 🙂 Qual é o seu nome?"
    log_message(company_id, phone, "out", reply)
    return {"status": "ok", "reply": reply}


# ---------------------------
# Finalização: DB -> Sheets
# ---------------------------
async def _finalize_quote(
    company_id: str,
    phone: str,
    company: Dict[str, Any],
    convo: Dict[str, Any],
    produto: str,
    cep_usado: str,
    cep_alterado: bool,
    salvou_cep_padrao: bool,
    is_returning: bool,
    now_iso: str,
):
    """
    Finaliza:
    1) Insere quote no DB (se falhar, NÃO exporta)
    2) Exporta pro Sheets (opcional)
    3) Marca convo como completed e step=produto (pronto pra novo orçamento)
    """
    quote_number = get_next_quote_number(company_id, phone)

    # 1) DB first
    try:
        qrow = insert_quote(
            company_id=company_id,
            phone=phone,
            quote_number=quote_number,
            produto=produto,
            cep_usado=cep_usado,
            cep_alterado=cep_alterado,
            salvou_cep_padrao=salvou_cep_padrao,
            is_returning=is_returning,
            status="ok",
        )
    except Exception as e:
        logger.exception(f"Falha ao salvar quote no DB: {e}")
        reply = "Tive um probleminha pra registrar seu pedido 😥 Pode me mandar de novo o produto/serviço?"
        log_message(company_id, phone, "out", reply)
        return {"status": "error", "reply": reply}

    # 2) Sheets after DB ok
    export_info = None
    export_error = None
    try:
        sheet_id = (company.get("sheet_id") or DEFAULT_SHEET_ID or "").strip()
        sheet_tab = norm_sheet_tab(company.get("sheet_tab") or DEFAULT_SHEET_TAB)

        if sheet_id and GOOGLE_SA_B64:
            row = [
                now_iso,                               # A created_at
                company_id,                            # B company_id
                phone,                                 # C phone
                1 if is_returning else 0,              # D is_returning
                int(quote_number),                     # E quote_number
                (convo.get("nome") or "").strip(),      # F nome
                (convo.get("email") or "").strip(),     # G email
                (produto or "").strip(),                # H produto
                (cep_usado or "").strip(),              # I cep_usado
                (convo.get("cep_padrao") or "").strip(),# J cep_padrao (pós save)
                1 if cep_alterado else 0,               # K cep_alterado
                1 if salvou_cep_padrao else 0,          # L salvou_cep_padrao
                "ok",                                   # M status
            ]
            export_info = append_to_sheets(sheet_id, sheet_tab, row)
    except Exception as e:
        export_error = str(e)
        logger.error(f"Falha no export pro Sheets (não bloqueia): {e}")

    # 3) conversation ready for next quote
    convo2 = update_conversation(company_id, phone, step=step_pack("produto"), status="completed")

    reply = (
        f"Fechado, {convo2.get('nome','')} ✅\n"
        f"Já registrei seu interesse em *{produto}*.\n"
        f"CEP considerado: *{cep_usado}*.\n\n"
        "Um vendedor vai te chamar em breve com uma oferta preparada pra você. 🤝"
    )
    log_message(company_id, phone, "out", reply)

    payload = {"status": "ok", "reply": reply, "quote": qrow, "export": export_info}
    if export_error:
        payload["export_error"] = export_error
    return payload
