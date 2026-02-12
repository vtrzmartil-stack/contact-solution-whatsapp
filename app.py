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
# DB helpers
# ---------------------------
def db_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL ausente")
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def ensure_tables_and_migrate():
    """
    - Cria tabelas se não existirem
    - Adiciona colunas caso seu DB já tenha uma versão antiga
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
        # caso já exista conversations antiga
        "alter table conversations add column if not exists nome text default ''",
        "alter table conversations add column if not exists email text default ''",
        "alter table conversations add column if not exists cep_padrao text default ''",
        "alter table conversations add column if not exists step text not null default 'nome'",
        "alter table conversations add column if not exists status text not null default 'open'",
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
    Exporta para A:M (13 colunas), de acordo com sua planilha nova.
    """
    if not sheet_id:
        raise RuntimeError("sheet_id ausente para export")
    sheet_tab = sheet_tab or "Página1"
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
# Helpers - fluxo / validações
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
        # MVP: aberto se você não configurou.
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
    Lista orçamentos (quotes) exportáveis/registrados.
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
    now_iso = datetime.now(timezone.utc).isoformat()

    # garante que empresa existe
    company = get_company(company_id)

    # garante conversa no DB
    convo = upsert_conversation(company_id, phone)
    step = (convo.get("step") or "nome").strip()

    greetings = {"oi", "olá", "ola", "bom dia", "boa tarde", "boa noite", "hello", "hi"}

    logger.info(f"[FLOW] company={company_id} phone={phone} step={step} status={convo.get('status')} text='{text}'")
    log_message(company_id, phone, "in", text)

    # ---------------------------
    # Helpers de estado "in-memory" por request (sem quebrar o DB)
    # - produto/cep temporários em steps intermediários ficam no texto do usuário
    # - a persistência final é em QUOTES
    # ---------------------------

    # Se lead já é "completed" e mandar qualquer coisa que pareça novo contato, tratamos como retorno:
    is_completed = (convo.get("status") == "completed")
    has_profile = bool((convo.get("nome") or "").strip()) and bool((convo.get("email") or "").strip())
    cep_padrao = (convo.get("cep_padrao") or "").strip()

    # Atalho: se está completed e receber saudação ou mensagem curta, reinicia no modo retorno:
    if is_completed and step not in {"produto", "cep_confirm", "cep", "cep_save"}:
        # entra no fluxo de orçamento direto
        convo = update_conversation(company_id, phone, step="produto", status="open")
        step = "produto"

    # ---------------------------
    # Step: NOME (novo lead)
    # ---------------------------
    if step == "nome":
        if text.lower() in greetings:
            reply = "Olá! 👋 Tudo bem? Qual é o seu nome?"
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        if not text:
            reply = "Qual é o seu nome?"
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        convo = update_conversation(company_id, phone, nome=text, step="email", status="open")
        reply = f"Prazer, {convo.get('nome','')}! Qual é o seu e-mail?"
        log_message(company_id, phone, "out", reply)
        return {"status": "ok", "reply": reply}

    # ---------------------------
    # Step: EMAIL (novo lead)
    # ---------------------------
    if step == "email":
        if not _is_valid_email(text):
            reply = "Esse e-mail parece inválido 😅 Pode enviar novamente?"
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        convo = update_conversation(company_id, phone, email=text, step="produto", status="open")
        reply = "Perfeito! Qual serviço/produto você tem interesse?"
        log_message(company_id, phone, "out", reply)
        return {"status": "ok", "reply": reply}

    # ---------------------------
    # Step: PRODUTO (novo ou retornando)
    # ---------------------------
    if step == "produto":
        if not text or text.lower() in greetings:
            # se é retorno e ele só deu oi, pergunta direto o produto
            if is_completed and has_profile:
                reply = f"Olá, {convo.get('nome','')}! 😄 Qual serviço/produto você quer orçar agora?"
            else:
                reply = "Qual serviço/produto você tem interesse?"
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        # Se for retorno e já tiver cep_padrao, pergunta se quer usar
        # Se não tiver cep_padrao, pede CEP direto
        # Salvamos o "produto" no step seguinte via "pass-through" (você manda o texto e seguimos o fluxo)
        # Para não depender de estado externo, vamos guardar o produto num "marcador" no step (DB) usando step = "cep_confirm|<produto>" etc.
        produto = text.strip()

        if cep_padrao:
            convo = update_conversation(company_id, phone, step=f"cep_confirm::{produto}", status="open")
            reply = (
                f"Show! Vou preparar o orçamento de *{produto}*.\n"
                f"Quer usar o seu CEP padrão *{cep_padrao}*?\n"
                "Responda:\n"
                "1 = Sim (usar padrão)\n"
                "2 = Não (informar outro CEP)"
            )
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        # sem cep padrão -> pede cep direto
        convo = update_conversation(company_id, phone, step=f"cep::{produto}", status="open")
        reply = "Perfeito! Agora me envie seu CEP (apenas números) pra eu preparar a oferta certinha."
        log_message(company_id, phone, "out", reply)
        return {"status": "ok", "reply": reply}

    # ---------------------------
    # Step: CEP_CONFIRM (1 usar padrão, 2 informar outro)
    # step vem como: "cep_confirm::<produto>"
    # ---------------------------
    if step.startswith("cep_confirm::"):
        produto = step.split("::", 1)[1].strip()

        if text not in {"1", "2"}:
            reply = "Me responde com 1 (usar CEP padrão) ou 2 (informar outro CEP)."
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        if text == "1":
            # usa cep_padrao e vai perguntar se quer salvar (aqui não faz sentido salvar, então só finaliza)
            convo = update_conversation(company_id, phone, step=f"finalize::{produto}::PADRAO", status="open")
            # “PADRAO” indica que não alterou CEP
            # cai no finalize no próximo request? Melhor finalizar já aqui:
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

        # text == "2" -> pede novo CEP
        convo = update_conversation(company_id, phone, step=f"cep::{produto}", status="open")
        reply = "Beleza. Me envie o CEP (8 dígitos, só números)."
        log_message(company_id, phone, "out", reply)
        return {"status": "ok", "reply": reply}

    # ---------------------------
    # Step: CEP (recebe um CEP)
    # step vem como: "cep::<produto>"
    # ---------------------------
    if step.startswith("cep::"):
        produto = step.split("::", 1)[1].strip()

        cep_fmt = _normalize_cep(text)
        if not cep_fmt:
            reply = "CEP inválido. Envie apenas números (8 dígitos)."
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        # se já tinha cep_padrao e agora usou outro, pergunta se quer salvar como padrão
        if cep_padrao and cep_fmt != cep_padrao:
            convo = update_conversation(company_id, phone, step=f"cep_save::{produto}::{cep_fmt}", status="open")
            reply = (
                f"Entendi ✅ Vou usar o CEP *{cep_fmt}*.\n"
                "Quer salvar esse CEP como seu novo CEP padrão?\n"
                "1 = Sim\n"
                "2 = Não"
            )
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        # se não tinha cep_padrao (primeira vez), oferece salvar como padrão
        if not cep_padrao:
            convo = update_conversation(company_id, phone, step=f"cep_save::{produto}::{cep_fmt}", status="open")
            reply = (
                f"Perfeito ✅ Vou usar o CEP *{cep_fmt}*.\n"
                "Quer salvar esse CEP como padrão para próximos orçamentos?\n"
                "1 = Sim\n"
                "2 = Não"
            )
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        # cep igual ao padrão (ou padrão vazio tratado acima) -> finaliza
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
    # Step: CEP_SAVE (1 salva, 2 não)
    # step vem como: "cep_save::<produto>::<cep>"
    # ---------------------------
    if step.startswith("cep_save::"):
        try:
            rest = step.split("cep_save::", 1)[1]
            produto, cep_fmt = rest.split("::", 1)
            produto = produto.strip()
            cep_fmt = cep_fmt.strip()
        except Exception:
            # se corromper, volta pro produto
            convo = update_conversation(company_id, phone, step="produto", status="open")
            reply = "Vamos seguir 🙂 Qual serviço/produto você quer orçar?"
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        if text not in {"1", "2"}:
            reply = "Me responde com 1 (salvar como padrão) ou 2 (não salvar)."
            log_message(company_id, phone, "out", reply)
            return {"status": "ok", "reply": reply}

        salvou = (text == "1")
        cep_alterado = bool(cep_padrao) and (cep_fmt != cep_padrao)

        # se salvar, atualiza cep_padrao no perfil
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

    # fallback: reseta com segurança
    convo = update_conversation(company_id, phone, step="nome", status="open")
    reply = "Vamos recomeçar 🙂 Qual é o seu nome?"
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
    """
    Finaliza:
    1) Insere quote no DB (garante sucesso)
    2) Exporta pro Sheets (opcional)
    3) Marca convo como completed e step=produto (pronto pra novo orçamento)
    """
    # garante quote_number consistente
    quote_number = get_next_quote_number(company_id, phone)

    # 1) Persistir quote no DB (se falhar, NÃO exporta)
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

    # 2) Export opcional pro Sheets (só após DB OK)
    export_info = None
    export_error = None
    try:
        sheet_id = (company.get("sheet_id") or DEFAULT_SHEET_ID or "").strip()
        sheet_tab = (company.get("sheet_tab") or DEFAULT_SHEET_TAB or "Página1").strip()

        if sheet_id and GOOGLE_SA_B64:
            row = [
                now_iso,                         # created_at
                company_id,                      # company_id
                phone,                           # phone
                1 if is_returning else 0,        # is_returning
                int(quote_number),               # quote_number
                (convo.get("nome") or "").strip(),    # nome
                (convo.get("email") or "").strip(),   # email
                (produto or "").strip(),              # produto
                (cep_usado or "").strip(),            # cep_usado
                (convo.get("cep_padrao") or "").strip(),  # cep_padrao (pós save)
                1 if cep_alterado else 0,         # cep_alterado
                1 if salvou_cep_padrao else 0,    # salvou_cep_padrao
                "ok",                             # status
            ]
            export_info = append_to_sheets(sheet_id, sheet_tab, row)
    except Exception as e:
        export_error = str(e)
        logger.error(f"Falha no export pro Sheets (não bloqueia): {e}")

    # 3) Marcar conversa como completed e pronta pra novo orçamento (step produto)
    convo2 = update_conversation(company_id, phone, step="produto", status="completed")

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
