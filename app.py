import os
import json
import base64
import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware

import psycopg
from psycopg.rows import dict_row

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


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

DEFAULT_SHEET_ID = os.getenv("GSHEET_ID", "")
DEFAULT_SHEET_TAB = os.getenv("SHEET_TAB_NAME", "Página1")

# IMPORTANTE: mantenha este nome exatamente igual ao do Render
GOOGLE_SA_B64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_B64", "")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")

# Para o painel (React), você pode setar no Render:
# CORS_ORIGINS=http://localhost:5173,http://127.0.0.1:5173,https://SEU-PAINEL.vercel.app
CORS_ORIGINS = os.getenv("CORS_ORIGINS", "")

app = FastAPI(title="Contact Solution (Multi-Company + Settings in companies.settings)")


# ---------------------------
# CORS (painel admin)
# ---------------------------
if CORS_ORIGINS.strip():
    origins = [o.strip() for o in CORS_ORIGINS.split(",") if o.strip()]
else:
    # MVP: libera localhost do Vite (5173/5174) e seu 127.0.0.1
    origins = [
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:5174",
        "http://127.0.0.1:5174",
    ]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------
# DB helpers
# ---------------------------
def db_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL ausente")
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def ensure_tables_and_migrate():
    """
    Cria tabelas e faz migrações idempotentes.
    Agora inclui:
    - companies.bot_enabled
    - companies.bot_mode
    - companies.settings (jsonb)
    """
    if not DATABASE_URL:
        logger.warning("DATABASE_URL ausente; pulando criação de tabelas.")
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
      export_status text not null default 'pending', -- pending | ok | error
      export_error text not null default '',
      created_at timestamptz not null default now(),
      unique(company_id, phone, quote_number)
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

    create index if not exists idx_quotes_company_phone_created
    on quotes(company_id, phone, created_at desc);
    """

    migrations = [
        # conversations
        "alter table conversations add column if not exists nome text default ''",
        "alter table conversations add column if not exists email text default ''",
        "alter table conversations add column if not exists cep_padrao text default ''",
        "alter table conversations add column if not exists step text not null default 'nome'",
        "alter table conversations add column if not exists status text not null default 'open'",

        # quotes extras
        "alter table quotes add column if not exists export_status text not null default 'pending'",
        "alter table quotes add column if not exists export_error text not null default ''",

        # companies settings
        "alter table companies add column if not exists bot_enabled boolean not null default true",
        "alter table companies add column if not exists bot_mode text not null default 'active'",
        "alter table companies add column if not exists settings jsonb not null default '{}'::jsonb",
    ]

    try:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(ddl)
                for m in migrations:
                    cur.execute(m)
            conn.commit()
        logger.info("DB OK: tabelas garantidas + migração aplicada.")
    except Exception as e:
        logger.exception(f"Falha ao criar/verificar tabelas: {e}")


@app.on_event("startup")
def _startup():
    ensure_tables_and_migrate()


# ---------------------------
# Helpers - Auth/Admin
# ---------------------------
def require_admin(request: Request):
    """
    Se ADMIN_TOKEN estiver vazio, libera (modo dev).
    Se estiver setado, exige header x-admin-token.
    """
    if not ADMIN_TOKEN:
        return
    token = request.headers.get("x-admin-token", "")
    if token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")


# ---------------------------
# Helpers - validações/normalizações
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


def normalize_sheet_id(value: str) -> str:
    """
    Aceita:
    - ID puro
    - URL do Sheets (extrai o /d/<ID>/)
    - remove aspas e espaços
    """
    s = (value or "").strip().strip('"').strip("'")
    if not s:
        return ""
    m = re.search(r"/d/([a-zA-Z0-9-_]+)", s)
    if m:
        s = m.group(1)
    s = s.replace(" ", "")
    return s


def normalize_sheet_tab(value: str) -> str:
    s = (value or "").strip()
    if not s:
        return "Página1"
    if s.lower() == "pagina1":
        return "Página1"
    return s


def a1_quote_sheet_tab(tab: str) -> str:
    tab = normalize_sheet_tab(tab)
    tab = tab.replace("'", "''")
    return f"'{tab}'"


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
# Helpers - Settings/Messages (companies.settings)
# ---------------------------
DEFAULT_MESSAGES: Dict[str, str] = {
    "ask_name_greeting": "Olá! 👋 Tudo bem? Qual é o seu nome?",
    "ask_name": "Qual é o seu nome?",
    "ask_email": "Prazer, {nome}! Qual é o seu e-mail?",
    "invalid_email": "Esse e-mail parece inválido 😅 Pode enviar novamente?",
    "ask_product_first": "Perfeito! Qual serviço/produto você tem interesse?",
    "ask_product_returning": "Olá, {nome}! 😄 Qual serviço/produto você quer orçar agora?",
    "ask_cep": "Perfeito! Agora me envie seu CEP (apenas números) pra eu preparar a oferta certinha.",
    "invalid_cep": "CEP inválido. Envie apenas números (8 dígitos).",
    "confirm_use_default_cep": (
        "Show! Vou preparar o orçamento de *{produto}*.\n"
        "Quer usar o seu CEP padrão *{cep_padrao}*?\n"
        "Responda:\n"
        "1 = Sim (usar padrão)\n"
        "2 = Não (informar outro CEP)"
    ),
    "ask_other_cep": "Beleza. Me envie o CEP (8 dígitos, só números).",
    "ask_save_cep_as_default": (
        "Perfeito ✅ Vou usar o CEP *{cep}*.\n"
        "Quer salvar esse CEP como padrão para próximos orçamentos?\n"
        "1 = Sim\n"
        "2 = Não"
    ),
    "ask_replace_default_cep": (
        "Entendi ✅ Vou usar o CEP *{cep}*.\n"
        "Quer salvar esse CEP como seu novo CEP padrão?\n"
        "1 = Sim\n"
        "2 = Não"
    ),
    "need_1_or_2": "Me responde com 1 ou 2, por favor 🙂",
    "continue_product": "Vamos seguir 🙂 Qual serviço/produto você quer orçar?",
    "restart": "Vamos recomeçar 🙂 Qual é o seu nome?",
    "final_reply": (
        "Fechado, {nome} ✅\n"
        "Já registrei seu interesse em *{produto}*.\n"
        "CEP considerado: *{cep}*.\n\n"
        "Um vendedor vai te chamar em breve com uma oferta preparada pra você. 🤝"
    ),
}

DEFAULT_FLAGS = {
    "export_sheets_enabled": True
}


def _safe_settings(value: Any) -> Dict[str, Any]:
    """
    Garante que settings é dict.
    Algumas configs/DB podem retornar JSON como str -> tenta json.loads.
    """
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return {}
    return {}


def _render_template(text: str, **vars) -> str:
    """
    Format seguro: se faltar chave, mantém placeholder.
    """
    try:
        class SafeDict(dict):
            def __missing__(self, key):
                return "{" + key + "}"
        return text.format_map(SafeDict(**vars))
    except Exception:
        return text


def get_company_message(company: Dict[str, Any], key: str) -> str:
    settings = _safe_settings(company.get("settings"))
    messages = settings.get("messages") or {}
    if isinstance(messages, dict):
        val = (messages.get(key) or "").strip()
        if val:
            return val
    return DEFAULT_MESSAGES.get(key, "")


def get_company_flag(company: Dict[str, Any], key: str, default: bool = False) -> bool:
    settings = _safe_settings(company.get("settings"))
    flags = settings.get("flags") or {}
    if isinstance(flags, dict) and key in flags:
        return bool(flags.get(key))
    return bool(DEFAULT_FLAGS.get(key, default))


# ---------------------------
# Helpers - Sheets
# ---------------------------
def _normalize_b64(s: str) -> str:
    s = (s or "").strip().replace("\n", "").replace("\r", "").replace(" ", "")
    missing = len(s) % 4
    if missing:
        s += "=" * (4 - missing)
    return s


def _load_service_account_info() -> Dict[str, Any]:
    if not GOOGLE_SA_B64:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_B64 ausente")
    raw = base64.b64decode(_normalize_b64(GOOGLE_SA_B64)).decode("utf-8")
    return json.loads(raw)


def _get_sheets_service():
    info = _load_service_account_info()
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def get_service_account_email() -> str:
    try:
        info = _load_service_account_info()
        return (info.get("client_email") or "").strip()
    except Exception:
        return ""


def sheets_check_access(sheet_id: str) -> Tuple[bool, str]:
    sheet_id = normalize_sheet_id(sheet_id)
    if not sheet_id:
        return False, "sheet_id vazio após normalização"

    service = _get_sheets_service()
    try:
        _ = service.spreadsheets().get(spreadsheetId=sheet_id).execute()
        return True, "ok"
    except HttpError as e:
        return False, f"HttpError {getattr(e, 'status_code', '')}: {e}"
    except Exception as e:
        return False, str(e)


def append_to_sheets(sheet_id: str, sheet_tab: str, row: List[Any]) -> Dict[str, Any]:
    sheet_id = normalize_sheet_id(sheet_id)
    sheet_tab = normalize_sheet_tab(sheet_tab)
    if not sheet_id:
        raise RuntimeError("sheet_id ausente/invalidado para export")

    rng = f"{a1_quote_sheet_tab(sheet_tab)}!A:M"

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
# DB - operações
# ---------------------------
def get_company(company_id: str) -> Dict[str, Any]:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("select * from companies where id = %s", (company_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="company_id não encontrado")
            # normaliza settings
            row["settings"] = _safe_settings(row.get("settings"))
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
                  (company_id, phone, quote_number, produto, cep_usado, cep_alterado, salvou_cep_padrao, is_returning, status, export_status, export_error)
                values
                  (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'pending', '')
                returning *
                """,
                (
                    company_id,
                    phone,
                    quote_number,
                    produto or "",
                    cep_usado or "",
                    bool(cep_alterado),
                    bool(salvou_cep_padrao),
                    bool(is_returning),
                    status,
                ),
            )
            row = cur.fetchone()
            conn.commit()
            return row


def mark_quote_export(company_id: str, phone: str, quote_number: int, ok: bool, err: str = ""):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                update quotes
                set export_status=%s,
                    export_error=%s
                where company_id=%s and phone=%s and quote_number=%s
                """,
                ("ok" if ok else "error", (err or "")[:2000], company_id, phone, int(quote_number)),
            )
        conn.commit()


def update_company_settings(
    company_id: str,
    bot_enabled: Optional[bool] = None,
    bot_mode: Optional[str] = None,
    settings_patch: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Atualiza settings com merge raso (top-level).
    """
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("select bot_enabled, bot_mode, settings from companies where id=%s", (company_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="company_id não encontrado")

            current_settings = _safe_settings(row.get("settings"))
            merged = dict(current_settings)

            if settings_patch is not None:
                for k, v in settings_patch.items():
                    merged[k] = v

            cur.execute(
                """
                update companies
                set bot_enabled = coalesce(%s, bot_enabled),
                    bot_mode = coalesce(%s, bot_mode),
                    settings = %s::jsonb
                where id=%s
                returning *
                """,
                (
                    bot_enabled,
                    bot_mode,
                    json.dumps(merged),
                    company_id,
                ),
            )
            updated = cur.fetchone()
        conn.commit()

    updated["settings"] = _safe_settings(updated.get("settings"))
    return updated


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
            "/webhook (GET verify)",
            "/webhook/{company_id} (POST)",
            "/admin/companies (GET/POST)",
            "/admin/company/{company_id}/settings (GET/PUT)",
            "/admin/leads/{company_id}",
            "/admin/quotes/{company_id}",
            "/admin/sheets-check/{company_id}",
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

    sheet_id_raw = (body.get("sheet_id") or DEFAULT_SHEET_ID or "").strip()
    sheet_tab_raw = (body.get("sheet_tab") or DEFAULT_SHEET_TAB or "Página1").strip()

    sheet_id = normalize_sheet_id(sheet_id_raw)
    sheet_tab = normalize_sheet_tab(sheet_tab_raw)

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

    row["settings"] = _safe_settings(row.get("settings"))
    return {"status": "ok", "company": row}


@app.get("/admin/companies")
def admin_list_companies(request: Request):
    require_admin(request)
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("select * from companies order by created_at desc")
            rows = cur.fetchall()

    for r in rows:
        r["settings"] = _safe_settings(r.get("settings"))
    return {"status": "ok", "companies": rows}


@app.get("/admin/company/{company_id}/settings")
def admin_get_company_settings(company_id: str, request: Request):
    require_admin(request)
    company = get_company(company_id)

    # devolve defaults mesclados (painel consegue ver tudo)
    settings = _safe_settings(company.get("settings"))
    messages = settings.get("messages") if isinstance(settings.get("messages"), dict) else {}
    flags = settings.get("flags") if isinstance(settings.get("flags"), dict) else {}

    merged_messages = dict(DEFAULT_MESSAGES)
    merged_messages.update(messages)

    merged_flags = dict(DEFAULT_FLAGS)
    merged_flags.update(flags)

    settings_view = dict(settings)
    settings_view["messages"] = merged_messages
    settings_view["flags"] = merged_flags

    company_view = dict(company)
    company_view["settings"] = settings_view

    return {"status": "ok", "company": company_view}


@app.put("/admin/company/{company_id}/settings")
async def admin_put_company_settings(company_id: str, request: Request):
    require_admin(request)
    body = await request.json()

    bot_enabled = body.get("bot_enabled")
    bot_mode = body.get("bot_mode")
    settings = body.get("settings")

    if bot_enabled is not None and not isinstance(bot_enabled, bool):
        return JSONResponse(status_code=400, content={"status": "error", "error": "bot_enabled deve ser boolean"})

    if bot_mode is not None:
        bot_mode = str(bot_mode).strip().lower()
        allowed = {"active", "paused", "human", "test"}
        if bot_mode not in allowed:
            return JSONResponse(
                status_code=400,
                content={"status": "error", "error": "bot_mode inválido", "allowed": sorted(list(allowed))},
            )

    if settings is not None and not isinstance(settings, dict):
        return JSONResponse(status_code=400, content={"status": "error", "error": "settings deve ser um objeto (json)"})

    updated = update_company_settings(company_id, bot_enabled=bot_enabled, bot_mode=bot_mode, settings_patch=settings)
    return {"status": "ok", "company": updated}


@app.get("/admin/leads/{company_id}")
def admin_list_leads(company_id: str, request: Request):
    """
    Lista perfis (conversations) já completados.
    """
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
    """
    Lista orçamentos (quotes).
    """
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


@app.get("/admin/sheets-check/{company_id}")
def admin_sheets_check(company_id: str, request: Request):
    require_admin(request)
    company = get_company(company_id)

    sheet_id = normalize_sheet_id((company.get("sheet_id") or DEFAULT_SHEET_ID or ""))
    sheet_tab = normalize_sheet_tab((company.get("sheet_tab") or DEFAULT_SHEET_TAB or "Página1"))

    sa_email = get_service_account_email()

    if not GOOGLE_SA_B64:
        return {
            "status": "error",
            "error": "GOOGLE_SERVICE_ACCOUNT_B64 ausente no ambiente",
            "company_id": company_id,
            "sheet_id": sheet_id,
            "sheet_tab": sheet_tab,
            "service_account_email": sa_email,
        }

    ok, detail = sheets_check_access(sheet_id)

    return {
        "status": "ok" if ok else "error",
        "company_id": company_id,
        "sheet_id": sheet_id,
        "sheet_tab": sheet_tab,
        "range_example": f"{a1_quote_sheet_tab(sheet_tab)}!A:M",
        "service_account_email": sa_email,
        "access": "ok" if ok else "fail",
        "detail": detail,
        "hint": (
            "Se der 404/403, compartilhe a planilha com o service_account_email "
            "como Editor e aguarde alguns segundos."
        ),
    }


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
# Webhook Multiempresa
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

    # Sempre registra conversa e mensagem de entrada
    convo = upsert_conversation(company_id, phone)
    log_message(company_id, phone, "in", text)

    # --- GATE: painel controla automação ---
    bot_enabled = bool(company.get("bot_enabled", True))
    bot_mode = (company.get("bot_mode") or "active").strip().lower()

    # Se estiver desligado/pausado/humano: não responde automático.
    # Ainda assim, mantemos log e DB atualizados.
    if (not bot_enabled) or (bot_mode in {"paused", "human"}):
        logger.info(f"[BOT-OFF] company={company_id} phone={phone} mode={bot_mode}")
        return {"status": "ok", "reply": None, "note": f"bot disabled/mode={bot_mode}"}

    # (Opcional) test mode: você pode implementar whitelist depois.
    # if bot_mode == "test": ...

    step = (convo.get("step") or "nome").strip()
    greetings = {"oi", "olá", "ola", "bom dia", "boa tarde", "boa noite", "hello", "hi"}

    logger.info(f"[FLOW] company={company_id} phone={phone} step={step} status={convo.get('status')} text='{text}'")

    is_completed = (convo.get("status") == "completed")
    has_profile = bool((convo.get("nome") or "").strip()) and bool((convo.get("email") or "").strip())
    cep_padrao = (convo.get("cep_padrao") or "").strip()

    # Se já completou e mandou msg “genérica”, joga pro fluxo de orçamento direto
    if is_completed and step not in {"produto", "cep_confirm", "cep", "cep_save"}:
        convo = update_conversation(company_id, phone, step="produto", status="open")
        step = "produto"

    # ---------------------------
    # NOME
    # ---------------------------
    if step == "nome":
        if text.lower() in greetings:
            reply = get_company_message(company, "ask_name_greeting")
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        if not text:
            reply = get_company_message(company, "ask_name")
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        convo = update_conversation(company_id, phone, nome=text, step="email", status="open")
        reply = _render_template(get_company_message(company, "ask_email"), nome=convo.get("nome", ""))
        log_message(company_id, phone, "out", reply)
        return {"status": "ok", "reply": reply}

    # ---------------------------
    # EMAIL
    # ---------------------------
    if step == "email":
        if not _is_valid_email(text):
            reply = get_company_message(company, "invalid_email")
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        convo = update_conversation(company_id, phone, email=text, step="produto", status="open")
        reply = get_company_message(company, "ask_product_first")
        log_message(company_id, phone, "out", reply)
        return {"status": "ok", "reply": reply}

    # ---------------------------
    # PRODUTO
    # ---------------------------
    if step == "produto":
        if not text or text.lower() in greetings:
            if is_completed and has_profile:
                reply = _render_template(get_company_message(company, "ask_product_returning"), nome=convo.get("nome", ""))
            else:
                reply = get_company_message(company, "ask_product_first")
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        produto = text.strip()

        if cep_padrao:
            convo = update_conversation(company_id, phone, step=f"cep_confirm::{produto}", status="open")
            reply = _render_template(
                get_company_message(company, "confirm_use_default_cep"),
                produto=produto,
                cep_padrao=cep_padrao,
            )
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        convo = update_conversation(company_id, phone, step=f"cep::{produto}", status="open")
        reply = get_company_message(company, "ask_cep")
        log_message(company_id, phone, "out", reply)
        return {"status": "ok", "reply": reply}

    # ---------------------------
    # CEP_CONFIRM
    # ---------------------------
    if step.startswith("cep_confirm::"):
        produto = step.split("::", 1)[1].strip()

        if text not in {"1", "2"}:
            reply = get_company_message(company, "need_1_or_2")
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        if text == "1":
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

        convo = update_conversation(company_id, phone, step=f"cep::{produto}", status="open")
        reply = get_company_message(company, "ask_other_cep")
        log_message(company_id, phone, "out", reply)
        return {"status": "ok", "reply": reply}

    # ---------------------------
    # CEP
    # ---------------------------
    if step.startswith("cep::"):
        produto = step.split("::", 1)[1].strip()

        cep_fmt = _normalize_cep(text)
        if not cep_fmt:
            reply = get_company_message(company, "invalid_cep")
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        if cep_padrao and cep_fmt != cep_padrao:
            convo = update_conversation(company_id, phone, step=f"cep_save::{produto}::{cep_fmt}", status="open")
            reply = _render_template(get_company_message(company, "ask_replace_default_cep"), cep=cep_fmt)
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        if not cep_padrao:
            convo = update_conversation(company_id, phone, step=f"cep_save::{produto}::{cep_fmt}", status="open")
            reply = _render_template(get_company_message(company, "ask_save_cep_as_default"), cep=cep_fmt)
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

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
    # CEP_SAVE
    # ---------------------------
    if step.startswith("cep_save::"):
        try:
            rest = step.split("cep_save::", 1)[1]
            produto, cep_fmt = rest.split("::", 1)
            produto = produto.strip()
            cep_fmt = cep_fmt.strip()
        except Exception:
            convo = update_conversation(company_id, phone, step="produto", status="open")
            reply = get_company_message(company, "continue_product")
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        if text not in {"1", "2"}:
            reply = get_company_message(company, "need_1_or_2")
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        salvou = (text == "1")
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

    # fallback
    convo = update_conversation(company_id, phone, step="nome", status="open")
    reply = get_company_message(company, "restart")
    log_message(company_id, phone, "out", reply)
    return {"status": "ok", "reply": reply}


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
    quote_number = get_next_quote_number(company_id, phone)

    # 1) DB primeiro
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

    # 2) Sheets depois (não bloqueia, mas registra status na quote)
    export_info = None
    export_error = None
    export_ok = False

    export_enabled = get_company_flag(company, "export_sheets_enabled", default=True)

    try:
        sheet_id = normalize_sheet_id((company.get("sheet_id") or DEFAULT_SHEET_ID or ""))
        sheet_tab = normalize_sheet_tab((company.get("sheet_tab") or DEFAULT_SHEET_TAB or "Página1"))

        if export_enabled and sheet_id and GOOGLE_SA_B64:
            row = [
                now_iso,                                 # A created_at
                company_id,                              # B company_id
                phone,                                   # C phone
                1 if is_returning else 0,                # D is_returning
                int(quote_number),                       # E quote_number
                (convo.get("nome") or "").strip(),        # F nome
                (convo.get("email") or "").strip(),       # G email
                (produto or "").strip(),                  # H produto
                (cep_usado or "").strip(),                # I cep_usado
                (convo.get("cep_padrao") or "").strip(),  # J cep_padrao
                1 if cep_alterado else 0,                 # K cep_alterado
                1 if salvou_cep_padrao else 0,            # L salvou_cep_padrao
                "ok",                                     # M status
            ]
            export_info = append_to_sheets(sheet_id, sheet_tab, row)
            export_ok = True
    except Exception as e:
        export_error = str(e)
        logger.error(f"Falha no export pro Sheets (não bloqueia): {e}")
    finally:
        try:
            mark_quote_export(company_id, phone, quote_number, export_ok, export_error or "")
        except Exception as e:
            logger.warning(f"Falha ao marcar status export no DB: {e}")

    # 3) conversa pronta pra novo orçamento
    convo2 = update_conversation(company_id, phone, step="produto", status="completed")

    reply = _render_template(
        get_company_message(company, "final_reply"),
        nome=(convo2.get("nome") or "").strip(),
        produto=(produto or "").strip(),
        cep=(cep_usado or "").strip(),
    )
    log_message(company_id, phone, "out", reply)

    payload = {"status": "ok", "reply": reply, "quote": qrow, "export": export_info}
    if export_error:
        payload["export_error"] = export_error
    return payload
# --- ADICIONE ISSO AO FINAL DO ARQUIVO (DEPOIS DA LINHA 1163) ---

if __name__ == "__main__":
    import uvicorn
    # O Render exige que leiamos a porta da variável de ambiente
    port = int(os.environ.get("PORT", 10000))
    # O host PRECISA ser 0.0.0.0 para comunicação externa
    uvicorn.run(app, host="0.0.0.0", port=port)
