import os
import json
import logging
import uuid
import hashlib
from datetime import datetime
from typing import Any, Dict
import requests

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv

import psycopg
from psycopg.rows import dict_row

load_dotenv()

# ---------------------------
# Configurações e Logging
# ---------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("contact-solution")

VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")

app = FastAPI(title="Contact Solution OS - Full Engine")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class StatusUpdate(BaseModel):
    status: str

# ---------------------------
# Helpers de Banco
# ---------------------------
def db_conn():
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)

def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()

def _safe_settings(value: Any) -> Dict[str, Any]:
    if not value: return {}
    if isinstance(value, dict): return value
    try: return json.loads(value)
    except: return {}

# ==========================================
# ROTAS DE ADMIN E AUTENTICAÇÃO
# ==========================================

@app.post("/api/auth/login")
async def api_login(request: Request):
    data = await request.json()
    email, password = data.get("email"), data.get("password")
    
    # Login do Admin
    if email == "admin@solution.com" and password == "123":
        return {"companyId": "MASTER", "companyName": "Solution Admin", "role": "admin"}
    
    # Login do Cliente
    hashed_pw = hash_password(password)
    try:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id, name FROM companies WHERE email=%s AND password=%s", (email, hashed_pw))
                row = cur.fetchone()
        
        # Se encontrou o usuário, acessamos pelos índices 0 e 1
        if row: 
            return {
                "companyId": str(row[0]), # Convertemos para String para evitar erro 422 no Front
                "companyName": row[1], 
                "role": "client"
            }
    except Exception as e:
        print(f"Erro no login: {e}")
        
    return JSONResponse(status_code=401, content={"error": "Credenciais Inválidas"})

@app.get("/api/leads/{company_id}")
def api_get_leads(company_id: str):
    query = "SELECT * FROM conversations ORDER BY updated_at DESC" if company_id == "MASTER" else \
            "SELECT * FROM conversations WHERE company_id = %s ORDER BY updated_at DESC"
    params = () if company_id == "MASTER" else (company_id,)
    
    try:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                rows = cur.fetchall()
                # Transforma em lista de dicionários para o FastAPI não se confundir
                leads = []
                for row in rows:
                    leads.append({
                        "id": row[0],
                        "company_id": row[1],
                        "telefone": row[2],
                        "status": row[3],
                        "fase": row[4],
                        "nome": row[5] if len(row) > 5 else "Lead"
                    })
                return leads
    except Exception as e:
        print(f"Erro ao buscar leads: {e}")
        return []

@app.put("/api/leads/{lead_id}/status")
async def update_lead_status(lead_id: int, data: StatusUpdate):
    try:
        with db_conn() as conn:
            with conn.cursor() as cur:
                status_funil = data.status
                status_bot = 'open' if status_funil == 'bot' else 'paused'
                cur.execute(
                    "UPDATE conversations SET status=%s, status_funil=%s, updated_at=NOW() WHERE id=%s",
                    (status_bot, status_funil, lead_id)
                )
            conn.commit()
        return {"status": "ok"}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/api/messages/{company_id}/{phone}")
def get_chat_history(company_id: str, phone: str):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT direction, text, created_at FROM messages WHERE company_id=%s AND phone=%s ORDER BY created_at ASC", (company_id, phone))
            return cur.fetchall()

# ==========================================
# MOTOR DO WEBHOOK (INTELIGÊNCIA DO BOT)
# ==========================================

@app.get("/webhook")
async def verify(request: Request):
    if request.query_params.get("hub.verify_token") == VERIFY_TOKEN:
        return PlainTextResponse(request.query_params.get("hub.challenge"))
    return "Erro"

@app.post("/webhook/{company_id}")
async def webhook(company_id: str, request: Request):
    data = await request.json()
    try:
        entry = data['entry'][0]['changes'][0]['value']
        if 'messages' not in entry: return {"status": "no messages"}
        
        msg = entry['messages'][0]
        phone = msg['from']
        text = msg.get('text', {}).get('body', '').strip()

        with db_conn() as conn:
            with conn.cursor() as cur:
                # 1. Registrar Mensagem
                cur.execute("INSERT INTO messages (company_id, phone, direction, text) VALUES (%s, %s, %s, %s)",
                            (company_id, phone, 'inbound', text))
                
                # 2. Buscar Estado da Conversa
                cur.execute("SELECT * FROM conversations WHERE company_id=%s AND phone=%s", (company_id, phone))
                conv = cur.fetchone()
                
                if not conv:
                    cur.execute("INSERT INTO conversations (company_id, phone, status_funil, step) VALUES (%s, %s, 'bot', 'nome') RETURNING *",
                                (company_id, phone))
                    conv = cur.fetchone()

                # 3. Lógica de Perguntas (Steps) se o Bot estiver Ativo
                if conv['status'] == 'open':
                    steps = ['nome', 'email', 'produto', 'cep', 'confirm_cep', 'final']
                    current_step = conv['step']
                    
                    # Salva o dado do cliente baseado no step anterior
                    update_field = None
                    if current_step == 'nome': update_field = "nome"
                    elif current_step == 'email': update_field = "email"
                    
                    if update_field:
                        cur.execute(f"UPDATE conversations SET {update_field}=%s WHERE id=%s", (text, conv['id']))

                    # Avançar para o próximo step
                    next_idx = steps.index(current_step) + 1 if current_step in steps else 0
                    next_step = steps[next_idx] if next_idx < len(steps) else 'final'
                    
                    cur.execute("UPDATE conversations SET step=%s, updated_at=NOW() WHERE id=%s", (next_step, conv['id']))
                
            conn.commit()
        return {"status": "ok"}
    except Exception as e:
        logger.error(f"Erro Webhook: {e}")
        return {"status": "error"}
    
@app.post("/api/send-message")
async def api_send_message(request: Request):
    data = await request.json()
    company_id = data.get("companyId")
    phone = data.get("phone")
    text = data.get("text")

    # 1. Buscar as credenciais da Meta no banco (Token e Phone ID)
    # Aqui assumimos que você já tem essas configs ou usa as globais do .env
    access_token = os.getenv("WHATSAPP_TOKEN")
    phone_number_id = os.getenv("PHONE_NUMBER_ID")

    if not access_token or not phone_number_id:
        return JSONResponse(status_code=500, content={"error": "Configurações do WhatsApp ausentes"})

    # 2. Disparar para a API da Meta
    import requests
    url = f"https://graph.facebook.com/v18.0/{phone_number_id}/messages"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    payload = {
        "messaging_product": "whatsapp",
        "to": phone,
        "type": "text",
        "text": {"body": text}
    }

    try:
        response = requests.post(url, json=payload, headers=headers)
        if response.status_code == 200:
            # 3. Salvar a mensagem no seu banco para aparecer no histórico do chat
            with db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO messages (company_id, phone, direction, text) VALUES (%s, %s, %s, %s)",
                        (company_id, phone, 'outbound', text)
                    )
                conn.commit()
            return {"status": "ok"}
        else:
            return JSONResponse(status_code=400, content=response.json())
    except Exception as e:
        logger.error(f"Erro ao disparar WhatsApp: {e}")
        return JSONResponse(status_code=500, content={"error": "Falha ao enviar"})    

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))