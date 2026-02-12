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

# Admin token (opcional, recomendado)
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")

app = FastAPI(title="Contact Solution (Multi-Company)")


# ---------------------------
# DB Helpers
# ---------------------------
def db_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL ausente")
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


# ---------------------------
# SQL - DDL + migrações seguras
# ---------------------------
DDL_BASE = """
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
  cep_padrao text default '',     -- CEP padrão do cliente
  cep_atual text default '',      -- CEP usado no orçamento atual (pode ser alternativo)

  -- controle de retornos
  quote_number integer not null default 0,
  last_quote_at timestamptz,

  -- export
  exported_at timestamptz,

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

MIGRATIONS = [
    # Se você tinha coluna "cep" antiga, manter compatibilidade:
    "alter table conversations add column if not exists cep text",
    "alter table conversations add column if not exists cep_padrao text default ''",
    "alter table conversations add column if not exists cep_atual text default ''",
    "alter table conversations add column if not exists quote_number integer not null default 0",
    "alter table conversations add column if not exists last_quote_at timestamptz",
    "alter table conversations add column if not exists exported_at timestamptz",
]


def ensure_tables():
    if not DATABASE_URL:
        logger.warning("DATABASE_URL ausente; pulando criação de tabelas.")
        return
    try:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(DDL_BASE)
                for q in MIGRATIONS:
                    try:
                        cur.execute(q)
                    except Exception:
                        # algumas versões/estados podem falhar em add column duplicado etc.
                        pass
            conn.commit()
        logger.info("DB OK: tabelas garantidas + migrações aplicadas.")
    except Exception as e:
        logger.exception(f"Falha ao criar/verificar tabelas: {e}")


@app.on_event("startup")
def _startup():
    ensure_tables()


# ---------------------------
# Sheets helpers
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
    Sua planilha nova tem 13 colunas (A:M).
    """
    if not sheet_id:
        raise RuntimeError("sheet_id ausente para export")
    sheet_tab = sheet_tab or "Página1"

    # 13 colunas -> A:M
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
        "spreadsheetId": sheet_id,
        "sheetTab": sheet_tab,
    }


# ---------------------------
# Validations / parsing
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
    return s in {"1", "sim", "s", "yes", "y"}


def _is_no(s: str) -> bool:
    s = (s or "").strip().lower()
    return s in {"2", "nao", "não", "n", "no"}


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
# DB operations
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
        "step", "setor", "nome", "email", "produto", "status",
        "cep_padrao", "cep_atual", "quote_number", "last_quote_at", "exported_at",
        # compatibilidade
        "cep",
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
# Export logic (LOCKED)
# ---------------------------
def build_sheets_row(
    created_at_iso: str,
    company_id: str,
    phone: str,
    is_returning: bool,
    quote_number: int,
    nome: str,
    email: str,
    produto: str,
    cep_usado: str,
    cep_padrao: str,
    cep_alterado: bool,
    salvou_cep_padrao: bool,
    status: str,
) -> List[Any]:
    # A:M -> 13 colunas
    return [
        created_at_iso,                # A created_at
        company_id,                    # B company_id
        phone,                         # C phone
        "1" if is_returning else "0",  # D is_returning
        quote_number,                  # E quote_number
        nome,                          # F nome
        email,                         # G email
        produto,                       # H produto
        cep_usado,                     # I cep_usado
        cep_padrao,                    # J cep_padrao
        "1" if cep_alterado else "0",  # K cep_alterado
        "1" if salvou_cep_padrao else "0",  # L salvou_cep_padrao
        status,                        # M status
    ]


def try_export_and_finalize(
    company: Dict[str, Any],
    convo: Dict[str, Any],
    *,
    is_returning: bool,
    quote_number: int,
    cep_usado: str,
    cep_alterado: bool,
    salvou_cep_padrao: bool,
) -> Dict[str, Any]:
    """
    Regra: Só considera finalizado se:
      - Sheets NÃO está configurado -> ok (finaliza)
      - Sheets configurado -> append precisa dar sucesso
    """
    sheet_id = (company.get("sheet_id") or DEFAULT_SHEET_ID or "").strip()
    sheet_tab = (company.get("sheet_tab") or DEFAULT_SHEET_TAB or "Página1").strip()

    created_at_iso = datetime.now(timezone.utc).isoformat()
    row = build_sheets_row(
        created_at_iso=created_at_iso,
        company_id=company["id"],
        phone=convo["phone"],
        is_returning=is_returning,
        quote_number=quote_number,
        nome=convo.get("nome") or "",
        email=convo.get("email") or "",
        produto=convo.get("produto") or "",
        cep_usado=cep_usado,
        cep_padrao=convo.get("cep_padrao") or "",
        cep_alterado=cep_alterado,
        salvou_cep_padrao=salvou_cep_padrao,
        status="ok",
    )

    # Se não tem config de Sheets, não trava o fluxo
    if not sheet_id or not GOOGLE_SA_B64:
        update_conversation(
            company["id"], convo["phone"],
            status="completed",
            exported_at=datetime.now(timezone.utc),
            last_quote_at=datetime.now(timezone.utc),
            step="produto",  # deixa pronto pro próximo orçamento
        )
        return {"exported": False, "reason": "sheets_not_configured"}

    # Sheets configurado -> precisa sucesso
    try:
        export_info = append_to_sheets(sheet_id, sheet_tab, row)

        # sucesso -> FINALIZA
        update_conversation(
            company["id"], convo["phone"],
            status="completed",
            exported_at=datetime.now(timezone.utc),
            last_quote_at=datetime.now(timezone.utc),
            step="produto",
        )
        return {"exported": True, "info": export_info}
    except Exception as e:
        logger.error(f"Falha no export pro Sheets (TRAVADO): {e}")
        # trava para retry (não finaliza)
        update_conversation(
            company["id"], convo["phone"],
            step="export_retry",
            status="open",
        )
        return {"exported": False, "reason": str(e)}


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
                select * from conversations
                where company_id = %s
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
    try:
        payload = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"status": "error", "error": "JSON inválido"})

    msg = extract_whatsapp_message(payload)
    if not msg:
        return {"status": "ignored"}

    phone = msg["from"]
    text = (msg["text"] or "").strip()
    text_l = text.lower()

    # garante empresa
    company = get_company(company_id)

    # garante conversa
    convo = upsert_conversation(company_id, phone)

    # compat: se tinha "cep" antigo e cep_padrao vazio, tenta aproveitar
    if not convo.get("cep_padrao") and convo.get("cep"):
        convo = update_conversation(company_id, phone, cep_padrao=convo.get("cep") or "")

    step = convo["step"]

    greetings = {"oi", "olá", "ola", "bom dia", "boa tarde", "boa noite", "hello", "hi"}

    logger.info(f"[FLOW] company={company_id} phone={phone} step={step} status={convo.get('status')} text='{text}'")
    log_message(company_id, phone, "in", text)

    # ---------------------------
    # Retry export (se falhou antes)
    # ---------------------------
    if step == "export_retry":
        # tenta exportar novamente com o que já temos
        is_returning = True if convo.get("quote_number", 0) > 0 else False
        quote_number = int(convo.get("quote_number") or 0)

        cep_usado = convo.get("cep_atual") or convo.get("cep_padrao") or ""
        if not cep_usado:
            # sem CEP, volta pro CEP
            convo = update_conversation(company_id, phone, step="cep")
            reply = "Só falta seu CEP (8 dígitos) para eu concluir o registro. Pode me enviar?"
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        export_result = try_export_and_finalize(
            company, convo,
            is_returning=is_returning,
            quote_number=quote_number,
            cep_usado=cep_usado,
            cep_alterado=(cep_usado != (convo.get("cep_padrao") or "")),
            salvou_cep_padrao=False,
        )

        if export_result.get("exported"):
            reply = (
                f"Perfeito, {convo.get('nome','')} ✅\n"
                f"Registro confirmado! Seu interesse em *{convo.get('produto','')}* foi anotado.\n"
                "Um vendedor vai te chamar em breve com uma oferta preparada pra você."
            )
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply, "export": export_result}
        else:
            reply = "Ainda estou com dificuldade para registrar no sistema. Tente novamente em instantes 🙏"
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply, "export": export_result}

    # ---------------------------
    # Lead retornando (já tem dados)
    # ---------------------------
    has_profile = bool(convo.get("nome")) and bool(convo.get("email")) and bool(convo.get("cep_padrao"))
    is_returning_now = has_profile and (convo.get("quote_number", 0) > 0 or convo.get("status") == "completed")

    # Se o lead voltou e mandou saudação, não recolhe dados:
    if is_returning_now and text_l in greetings:
        # novo orçamento começa pedindo produto
        convo = update_conversation(company_id, phone, step="produto", status="open", produto="", cep_atual="")
        reply = (
            f"Oi, {convo.get('nome')}! 👋\n"
            "Quer fazer um novo orçamento? Me diga qual produto/serviço você tem interesse."
        )
        log_message(company_id, phone, "out", reply)
        return {"status": "ok", "reply": reply}

    # ---------------------------
    # Novo lead - saudação
    # ---------------------------
    if step == "nome" and not convo.get("nome") and text_l in greetings:
        reply = "Olá! 👋 Tudo bem? Qual é o seu nome?"
        log_message(company_id, phone, "out", reply)
        return {"status": "ok", "reply": reply}

    # ---------------------------
    # Step: NOME
    # ---------------------------
    if step == "nome":
        if not text:
            reply = "Qual é o seu nome?"
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        convo = update_conversation(company_id, phone, nome=text, step="email", status="open")
        reply = f"Prazer, {convo['nome']}! Qual é o seu e-mail?"
        log_message(company_id, phone, "out", reply)
        return {"status": "ok", "reply": reply}

    # ---------------------------
    # Step: EMAIL
    # ---------------------------
    if step == "email":
        if not _is_valid_email(text):
            reply = "Esse e-mail parece inválido 😅 Pode enviar novamente?"
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        convo = update_conversation(company_id, phone, email=text, step="produto", status="open")
        reply = "Perfeito! Qual produto/serviço você tem interesse?"
        log_message(company_id, phone, "out", reply)
        return {"status": "ok", "reply": reply}

    # ---------------------------
    # Step: PRODUTO
    # ---------------------------
    if step == "produto":
        if not text:
            reply = "Qual produto/serviço você tem interesse?"
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        # incrementa o número do orçamento aqui (novo orçamento)
        quote_number = int(convo.get("quote_number") or 0) + 1

        convo = update_conversation(
            company_id, phone,
            produto=text,
            quote_number=quote_number,
            status="open",
        )

        # Se já tem CEP padrão, pergunta se usa ele
        if convo.get("cep_padrao"):
            convo = update_conversation(company_id, phone, step="cep_confirm", cep_atual="")
            reply = (
                f"Boa! Para este orçamento, você quer usar o CEP padrão ({convo.get('cep_padrao')})?\n"
                "Responda:\n"
                "1 = Sim, usar esse CEP\n"
                "2 = Não, informar outro CEP"
            )
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        # Se não tem CEP padrão, pede CEP e depois pergunta se salva como padrão
        convo = update_conversation(company_id, phone, step="cep")
        reply = "Agora me envie seu CEP (8 dígitos) pra eu preparar a oferta certinha."
        log_message(company_id, phone, "out", reply)
        return {"status": "ok", "reply": reply}

    # ---------------------------
    # Step: CEP_CONFIRM (1 usa padrão / 2 outro)
    # ---------------------------
    if step == "cep_confirm":
        if _is_yes(text):
            # usa o cep_padrao
            cep_usado = convo.get("cep_padrao") or ""
            convo = update_conversation(company_id, phone, cep_atual=cep_usado, step="finalize")
        elif _is_no(text):
            convo = update_conversation(company_id, phone, step="cep", cep_atual="")
            reply = "Beleza. Me envie o CEP (8 dígitos) que você quer usar neste orçamento."
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}
        else:
            reply = "Responda com 1 (usar CEP padrão) ou 2 (informar outro CEP)."
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        # caiu no finalize com CEP padrão
        convo = upsert_conversation(company_id, phone)  # pega estado atual
        quote_number = int(convo.get("quote_number") or 0)
        export_result = try_export_and_finalize(
            company, convo,
            is_returning=True,
            quote_number=quote_number,
            cep_usado=convo.get("cep_atual") or convo.get("cep_padrao") or "",
            cep_alterado=False,
            salvou_cep_padrao=False,
        )

        if export_result.get("exported") or export_result.get("reason") == "sheets_not_configured":
            reply = (
                f"Fechado, {convo.get('nome','')} ✅\n"
                f"Já registrei seu interesse em *{convo.get('produto','')}*.\n"
                "Um vendedor vai te chamar em breve com uma oferta preparada pra você."
            )
        else:
            reply = (
                f"Fechado, {convo.get('nome','')} ✅\n"
                "Consegui montar seu pedido, mas tive um problema ao registrar no sistema.\n"
                "Tente mandar qualquer mensagem (ex: 'ok') em 1 minuto que eu tento registrar novamente."
            )

        log_message(company_id, phone, "out", reply)
        return {"status": "ok", "reply": reply, "export": export_result}

    # ---------------------------
    # Step: CEP (informar CEP do orçamento)
    # ---------------------------
    if step == "cep":
        cep = _normalize_cep(text)
        if not cep:
            reply = "CEP inválido. Envie apenas números (8 dígitos)."
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        # usa CEP neste orçamento
        convo = update_conversation(company_id, phone, cep_atual=cep, step="cep_save", status="open")
        reply = (
            f"Show! Vou usar {cep} neste orçamento.\n"
            "Quer salvar esse CEP como seu padrão para próximos atendimentos?\n"
            "1 = Sim\n"
            "2 = Não"
        )
        log_message(company_id, phone, "out", reply)
        return {"status": "ok", "reply": reply}

    # ---------------------------
    # Step: CEP_SAVE (salvar como padrão ou não) -> exporta
    # ---------------------------
    if step == "cep_save":
        salvou = False
        if _is_yes(text):
            # salva como padrão
            convo = update_conversation(company_id, phone, cep_padrao=convo.get("cep_atual") or "")
            salvou = True
        elif _is_no(text):
            salvou = False
        else:
            reply = "Responda com 1 (sim) ou 2 (não). Quer salvar esse CEP como padrão?"
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        convo = upsert_conversation(company_id, phone)  # atualiza estado
        quote_number = int(convo.get("quote_number") or 0)
        cep_usado = convo.get("cep_atual") or ""
        cep_padrao = convo.get("cep_padrao") or ""
        cep_alterado = bool(cep_padrao) and (cep_usado != cep_padrao)

        export_result = try_export_and_finalize(
            company, convo,
            is_returning=is_returning_now or (quote_number > 1),
            quote_number=quote_number,
            cep_usado=cep_usado,
            cep_alterado=cep_alterado,
            salvou_cep_padrao=salvou,
        )

        if export_result.get("exported") or export_result.get("reason") == "sheets_not_configured":
            reply = (
                f"Fechado, {convo.get('nome','')} ✅\n"
                f"Já registrei seu interesse em *{convo.get('produto','')}*.\n"
                "Um vendedor vai te chamar em breve com uma oferta preparada pra você."
            )
        else:
            reply = (
                f"Fechado, {convo.get('nome','')} ✅\n"
                "Consegui montar seu pedido, mas tive um problema ao registrar no sistema.\n"
                "Tente mandar qualquer mensagem (ex: 'ok') em 1 minuto que eu tento registrar novamente."
            )

        log_message(company_id, phone, "out", reply)
        return {"status": "ok", "reply": reply, "export": export_result}

    # ---------------------------
    # Fallback: reset seguro
    # ---------------------------
    update_conversation(
        company_id, phone,
        step="nome",
        status="open",
        setor="",
        nome="",
        email="",
        produto="",
        cep_atual="",
    )
    reply = "Vamos recomeçar 🙂 Qual é o seu nome?"
    log_message(company_id, phone, "out", reply)
    return {"status": "ok", "reply": reply}


# ---------------------------
# Erro padrão (pra não ficar 500 “mudo”)
# ---------------------------
@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    logger.exception(f"Unhandled error: {exc}")
    return JSONResponse(status_code=500, content={"status": "error", "error": "Internal Server Error"})
