import os
import json
import base64
import logging
import uuid
import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
load_dotenv()

import psycopg
from psycopg.rows import dict_row

# ---------------------------
# Logging & Env
# ---------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("contact-solution")

VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")

app = FastAPI(title="Contact Solution (API V3 - Multi-Tenant + Funil + Admin)")

# ---------------------------
# CORS (Painel Admin React)
# ---------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # MVP: Libera para qualquer painel acessar
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

def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()

def ensure_tables_and_migrate():
    if not DATABASE_URL:
        logger.warning("DATABASE_URL ausente; pulando criação de tabelas.")
        return

    ddl = """
    create table if not exists companies (
      id text primary key,
      name text not null,
      email text unique,
      password text,
      phone text,
      sheet_id text,
      sheet_tab text default 'Página1',
      bot_enabled boolean not null default true,
      bot_mode text not null default 'active',
      settings jsonb not null default '{}'::jsonb,
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
      status text not null default 'open',
      status_funil text not null default 'negociacao', -- NOVO: Para o Kanban
      updated_at timestamptz not null default now(),
      created_at timestamptz not null default now(),
      unique(company_id, phone)
    );

    create table if not exists messages (
      id bigserial primary key,
      company_id text not null references companies(id) on delete cascade,
      phone text not null,
      direction text not null,
      text text not null,
      created_at timestamptz not null default now()
    );
    """

    migrations = [
        "alter table companies add column if not exists email text unique",
        "alter table companies add column if not exists password text",
        "alter table companies add column if not exists phone text",
        "alter table conversations add column if not exists status_funil text default 'negociacao'",
    ]

    try:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(ddl)
                for m in migrations:
                    try:
                        cur.execute(m)
                    except Exception as e:
                        pass # ignora se a coluna já existir e der erro
            conn.commit()
        logger.info("DB OK: Tabelas garantidas + migração do Kanban aplicada.")
    except Exception as e:
        logger.exception(f"Falha ao criar/verificar tabelas: {e}")

@app.on_event("startup")
def _startup():
    ensure_tables_and_migrate()

def _safe_settings(value: Any) -> Dict[str, Any]:
    if value is None: return {}
    if isinstance(value, dict): return value
    if isinstance(value, str):
        try: return json.loads(value)
        except: return {}
    return {}

# ==========================================
# ROTAS DA API FRONTEND
# ==========================================

@app.post("/api/auth/register")
async def api_register(request: Request):
    data = await request.json()
    name = data.get("companyName", "").strip()
    email = data.get("email", "").strip()
    phone = data.get("whatsapp", "").strip()
    password = data.get("password", "").strip()

    if not name or not email or not password:
        return JSONResponse(status_code=400, content={"error": "Preencha todos os campos."})

    company_id = f"NODE_{uuid.uuid4().hex[:8].upper()}"
    hashed_pw = hash_password(password)

    try:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "insert into companies (id, name, email, phone, password) values (%s, %s, %s, %s, %s)",
                    (company_id, name, email, phone, hashed_pw)
                )
            conn.commit()
        return {"status": "ok", "companyId": company_id}
    except psycopg.errors.UniqueViolation:
        return JSONResponse(status_code=400, content={"error": "Este e-mail já está registrado."})
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": "Erro no servidor."})

@app.post("/api/auth/login")
async def api_login(request: Request):
    data = await request.json()
    email = data.get("email", "").strip()
    password = data.get("password", "").strip()

    # Super Admin
    if email == "admin@solution.com" and password == "123":
        return {"companyId": "MASTER", "companyName": "Solution Admin", "role": "admin"}

    hashed_pw = hash_password(password)

    try:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("select id, name from companies where email = %s and password = %s", (email, hashed_pw))
                row = cur.fetchone()
                
        if row:
            return {"companyId": row["id"], "companyName": row["name"], "role": "client"}
        else:
            return JSONResponse(status_code=401, content={"error": "Credenciais inválidas."})
    except Exception:
        return JSONResponse(status_code=500, content={"error": "Erro no servidor."})

@app.post("/api/auth/change-password")
async def api_change_password(request: Request):
    data = await request.json()
    company_id = data.get("companyId")
    nova_senha = data.get("novaSenha")

    if company_id == "MASTER":
        return {"status": "ok"} # Simulação para o admin master

    if not company_id or not nova_senha:
        return JSONResponse(status_code=400, content={"error": "Dados inválidos"})

    hashed_pw = hash_password(nova_senha)

    try:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("update companies set password = %s where id = %s", (hashed_pw, company_id))
            conn.commit()
        return {"status": "ok"}
    except Exception as e:
        logger.error(f"Erro ao trocar senha: {e}")
        return JSONResponse(status_code=500, content={"error": "Erro no servidor"})

# ==========================================
# ROTAS DO SUPER ADMIN (INFRAESTRUTURA)
# ==========================================

@app.get("/api/admin/companies")
def api_admin_get_companies():
    try:
        with db_conn() as conn:
            with conn.cursor() as cur:
                # Retorna tudo, menos a senha (por segurança)
                cur.execute("select id, name, email, phone from companies order by created_at desc")
                rows = cur.fetchall()
        return {"companies": rows}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": "Erro ao buscar empresas."})

@app.delete("/api/admin/companies/{target_id}")
def api_admin_delete_company(target_id: str):
    try:
        with db_conn() as conn:
            with conn.cursor() as cur:
                # O ON DELETE CASCADE no banco vai apagar os leads dessa empresa automaticamente
                cur.execute("delete from companies where id = %s", (target_id,))
            conn.commit()
        return {"status": "ok"}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": "Erro ao apagar empresa."})


# ==========================================
# ROTAS DE DADOS (LEADS E FLUXO)
# ==========================================

@app.get("/api/leads/{company_id}")
def api_get_leads(company_id: str):
    if company_id == "MASTER":
        query = "select * from conversations order by updated_at desc limit 100"
        params = ()
    else:
        query = "select * from conversations where company_id = %s order by updated_at desc limit 100"
        params = (company_id,)

    try:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                rows = cur.fetchall()
        return rows
    except Exception:
        return []

@app.post("/api/config/flow")
async def api_config_flow(request: Request):
    data = await request.json()
    company_id = data.get("companyId")
    messages_array = data.get("flow_messages", [])

    if company_id == "MASTER":
        return {"status": "ok"}

    keys_map = [
        "ask_name", "ask_email", "ask_product_first", "ask_cep",
        "confirm_use_default_cep", "ask_other_cep", "ask_save_cep_as_default",
        "ask_replace_default_cep", "final_reply"
    ]
    
    new_messages = {}
    for i, msg in enumerate(messages_array):
        if i < len(keys_map):
            new_messages[keys_map[i]] = msg

    try:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("select settings from companies where id=%s", (company_id,))
                row = cur.fetchone()
                if not row: return JSONResponse(status_code=404, content={"error": "Empresa não encontrada"})
                
                settings = _safe_settings(row.get("settings"))
                settings["messages"] = new_messages

                cur.execute("update companies set settings = %s::jsonb where id=%s", (json.dumps(settings), company_id))
            conn.commit()
        return {"status": "ok"}
    except Exception:
        return JSONResponse(status_code=500, content={"error": "Erro ao salvar."})


# ==========================================
# CÓDIGO DO WEBHOOK (WHATSAPP)
# ==========================================
@app.get("/webhook")
async def webhook_verify(request: Request):
    qp = request.query_params
    if qp.get("hub.mode") == "subscribe" and qp.get("hub.verify_token") == VERIFY_TOKEN:
        return PlainTextResponse(qp.get("hub.challenge"))
    return JSONResponse(status_code=403, content={"error": "Verification failed"})

@app.post("/webhook/{company_id}")
async def webhook_receive(company_id: str, request: Request):
    # O código original de processamento de mensagens fica aqui
    return {"status": "ok"}

# ---------------------------
# Inicializador do Render
# ---------------------------
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 10000))
    uvicorn.run(app, host="0.0.0.0", port=port)