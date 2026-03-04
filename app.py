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
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import json
from pydantic import BaseModel
import uuid

import random
import string
load_dotenv()

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import requests
import json

# CONFIGURAÇÕES DO WHATSAPP (Substitua pelos seus dados)
WA_TOKEN = "EAAL7kEUNnu0BQxBsXAzf2fZAx10X9t2YcB9cDJzR3F7VoSGAKoYTcpZCDPlzh73ViyZAfvQWxXPLQ4qjqPZAHhaLdCC2CMhyYKlW2ynvK4xIl6olTTcBpfXUjxOMbDtefJOiY8hdsmwgemXtk8I77vZBWvMEZBr4VdZBzacWYZBCeSMGTzyqyOpwoHTrlBAx9pUCiCbhll25LHGY3ekt1cKc1AhjpzACnniRJS3HSVB9z0clog1yXYDsSvv5vvHkiSYvuQZAGGl0ZCJaScj1EVHWiZAH1bn4zlZCGbyZA0QZDZD"
WA_PHONE_ID = "956946084171393"

# ---------------------------
# Configurações e Logging
# ---------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("contact-solution")

VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")
def get_db_connection():
    # Esta função abre a porta do banco de dados usando a URL do Render
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)
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

# Garanta que esta linha está no topo do seu app.py:
from fastapi.responses import JSONResponse 

@app.post("/api/auth/login")
async def api_login(request: Request):
    try:
        data = await request.json()
        email = data.get("email")
        password = data.get("password")

        # 1. ATALHO DE SEGURANÇA (MASTER/ADMIN)
        if email == "admin@master.com" and password == "suasenha":
            return {
                "status": "success",
                "companyId": "MASTER",
                "companyName": "Administrador Geral",
                "role": "admin",
                "email": "admin@master.com"
            }

        # 2. BUSCA NO BANCO APENAS COM AS COLUNAS QUE REALMENTE EXISTEM
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT company_id, role, email, password FROM users WHERE email = %s", 
                    (email,)
                )
                user = cur.fetchone()

        # 3. A BARREIRA DE AÇO (Validação rigorosa)
        # Se não achou o usuário (None) OU se a senha digitada for diferente do banco:
        if not user or user['password'] != password:
            # Mandamos o envelope VERMELHO (401) para o React bater a porta!
            return JSONResponse(
                status_code=401, 
                content={"status": "error", "message": "E-mail ou senha incorretos."}
            )

        # 4. SUCESSO! A senha está certa. (Envelope Verde automático)
        return {
            "status": "success",
            "companyId": user['company_id'],
            "companyName": f"Empresa {user['company_id']}", 
            "role": user['role'],
            "email": user['email'] 
        }

    except Exception as e:
        print(f"Erro no servidor: {e}")
        return JSONResponse(
            status_code=500, 
            content={"status": "error", "message": "Erro interno no servidor"}
        )
# ==========================================
# 2. BUSCA DE LEADS BLINDADA
# ==========================================
@app.get("/api/leads/{company_id}", response_model=None)
async def api_get_leads(company_id: str):
    cid = str(company_id)
    query = "SELECT id, company_id, phone, status, status_funil, nome FROM conversations ORDER BY updated_at DESC" if cid == "MASTER" else \
            "SELECT id, company_id, phone, status, status_funil, nome FROM conversations WHERE company_id = %s ORDER BY updated_at DESC"
    params = () if cid == "MASTER" else (cid,)
    
    try:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                rows = cur.fetchall()
                leads = []
                for row in rows:
                    # Verifica o formato de cada linha
                    is_dict = isinstance(row, dict) or hasattr(row, 'keys')
                    
                    leads.append({
                        "id": str(row["id"] if is_dict else row[0]),
                        "company_id": str(row["company_id"] if is_dict else row[1]),
                        "telefone": str(row["phone"] if is_dict else (row[2] or "")),
                        "status": str(row["status"] if is_dict else (row[3] or "open")),
                        "status_funil": str(row["status_funil"] if is_dict else (row[4] or "novo")), 
                        "nome": str(row["nome"] if is_dict else (row[5] or "Lead"))
                    })
                return JSONResponse(content=leads)
    except Exception as e:
        print(f"Erro ao buscar leads: {e}")
        return JSONResponse(content=[], status_code=500)

# ==========================================
# 2. ROTA DE ATUALIZAR STATUS (Apenas PUT, lead_id e data)
# ==========================================

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

# ==========================================
# 2. ROTA DE ENVIO MENSAGENS WPP
# ==========================================

def enviar_whatsapp(numero_destino, nome_cliente):
    url = f"https://graph.facebook.com/v19.0/{WA_PHONE_ID}/messages"
    headers = {
        "Authorization": f"Bearer {WA_TOKEN}",
        "Content-Type": "application/json"
    }
    
    # Payload usando o template hello_world (ajustaremos para o seu depois)
    payload = {
        "messaging_product": "whatsapp",
        "to": numero_destino,
        "type": "template",
        "template": {
            "name": "hello_world",
            "language": {"code": "en_US"}
        }
    }

    try:
        response = requests.post(url, headers=headers, json=payload)
        return response.json()
    except Exception as e:
        print(f"Erro ao disparar WhatsApp: {e}")
        return None
    
def disparar_zap(meu_numero, texto):
    """Função que limpa o número e envia a mensagem real"""
    # 1. Limpa o número (deixa só dígitos)
    num_limpo = "".join(filter(str.isdigit, meu_numero))
    if not num_limpo.startswith("55"):
        num_limpo = f"55{num_limpo}"
    
    # 2. A função que você me enviou (mantenha apenas a disparar_zap para limpar o código)
def disparar_zap(meu_numero, texto):
    num_limpo = "".join(filter(str.isdigit, meu_numero))
    if not num_limpo.startswith("55"):
        num_limpo = f"55{num_limpo}"
    
    url = f"https://graph.facebook.com/v19.0/{WA_PHONE_ID}/messages"
    headers = {"Authorization": f"Bearer {WA_TOKEN}", "Content-Type": "application/json"}
    
    payload = {
        "messaging_product": "whatsapp",
        "to": num_limpo,
        "type": "text",
        "text": {"body": texto}
    }
    try:
        res = requests.post(url, headers=headers, json=payload)
        return res.json()
    except Exception as e:
        print(f"Erro na API da Meta: {e}")
        return None

# 3. A ROTA que o seu site vai chamar
@app.post("/api/messages/send")
async def send_message(payload: MessageSendRequest):
    try:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO messages (company_id, phone, direction, text, created_at) VALUES (%s, %s, 'outbound', %s, NOW())",
                    (payload.company_id, payload.phone, payload.text)
                )
            conn.commit()

        resultado = disparar_zap(payload.phone, payload.text)
        return {"status": "success", "meta_response": resultado}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

# ==========================================
# ROTA DE MENSAGENS BLINDADA E COM ACESSO ADMIN
# ==========================================
@app.get("/api/messages/{company_id}/{phone}")
def get_chat_history(company_id: str, phone: str):
    try:
        with db_conn() as conn:
            with conn.cursor() as cur:
                # 🔥 Se for o Admin (MASTER), busca só pelo telefone
                if company_id == "MASTER":
                    cur.execute("SELECT direction, text, created_at FROM messages WHERE phone=%s ORDER BY created_at ASC", (phone,))
                # Se for um cliente normal, exige que a mensagem seja da empresa dele
                else:
                    cur.execute("SELECT direction, text, created_at FROM messages WHERE company_id=%s AND phone=%s ORDER BY created_at ASC", (company_id, phone))
                
                rows = cur.fetchall()
                
                if not rows:
                    return JSONResponse(content=[])

                colunas = [desc[0] for desc in cur.description]
                
                messages = []
                for row in rows:
                    d = dict(row) if isinstance(row, dict) or hasattr(row, 'keys') else dict(zip(colunas, row))
                    
                    messages.append({
                        "direction": str(d.get("direction", "")),
                        "text": str(d.get("text", "")),
                        "created_at": str(d.get("created_at", ""))
                    })
                return JSONResponse(content=messages)
    except Exception as e:
        print(f"Erro ao buscar mensagens: {e}")
        return JSONResponse(content=[], status_code=500)

# Criamos o modelo de dados que o Frontend vai enviar
class RegisterCompanyRequest(BaseModel):
    name: str
    email: str
    phone: str
    bot_whatsapp: str
    password: str

@app.post("/api/admin/companies")
async def register_company(data: RegisterCompanyRequest):
    hashed_pw = hash_password(data.password)
    
    # 🔥 A MÁGICA: Gerando um ID único no formato NODE_XXXX
    generated_id = f"NODE_{uuid.uuid4().hex[:8].upper()}"
    
    try:
        with db_conn() as conn:
            with conn.cursor() as cur:
                # Verifica se o e-mail já existe
                cur.execute("SELECT id FROM companies WHERE email = %s", (data.email,))
                if cur.fetchone():
                    return JSONResponse(status_code=400, content={"error": "E-mail já cadastrado!"})

                # 1. INSERE A EMPRESA (A casa)
                cur.execute(
                    """INSERT INTO companies 
                       (id, name, email, phone, bot_whatsapp, password, created_at, status) 
                       VALUES (%s, %s, %s, %s, %s, %s, NOW(), 'active')""",
                    (generated_id, data.name, data.email, data.phone, data.bot_whatsapp, hashed_pw)
                )

                # 2. INSERE O USUÁRIO (A chave da casa)
                # Usamos data.password (senha normal) para o seu login conseguir ler perfeitamente!
                cur.execute(
                    """INSERT INTO users 
                       (company_id, email, password, role) 
                       VALUES (%s, %s, %s, 'client')""",
                    (generated_id, data.email, data.password)
                )
                
            # O commit aqui salva as DUAS tabelas ao mesmo tempo!
            conn.commit() 
            
        return {"status": "success", "message": "Empresa e Acesso cadastrados com sucesso!", "id": generated_id}
        
    except Exception as e:
        print(f"Erro ao cadastrar empresa: {e}")
        return JSONResponse(status_code=500, content={"error": "Erro interno ao salvar no banco"})

    # Criamos um modelo para o corpo da requisição
class MessageSendRequest(BaseModel):
    company_id: str
    phone: str
    text: str

@app.post("/api/messages/send")
async def send_message(payload: MessageSendRequest):
    try:
        # 1. Salva a mensagem no banco de dados (Histórico)
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO messages (company_id, phone, direction, text, created_at) 
                       VALUES (%s, %s, 'outbound', %s, NOW())""",
                    (payload.company_id, payload.phone, payload.text)
                )
            conn.commit()
            
        # 🚀 2. Disparo real para o WhatsApp (O Motor)
        # Certifique-se de que a função disparar_zap existe no seu app.py
        print(f"🚀 Enviando Zap para: {payload.phone}")
        resultado_meta = disparar_zap(payload.phone, payload.text)
        print(f"📡 Resposta da Meta: {resultado_meta}")
            
        # 3. Retorno de sucesso para o Painel
        return {
            "status": "success", 
            "message": "Mensagem salva e enviada!",
            "meta_response": resultado_meta
        }

    except Exception as e:
        # 🛡️ O "ESCUDO": Isso aqui limpa o erro de 'Try statement'
        print(f"❌ Erro crítico em send_message: {e}")
        return JSONResponse(
            status_code=500, 
            content={"error": "Falha ao processar mensagem", "details": str(e)}
        )
            
       
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
    company_id = data.get("companyId") or data.get(company_id)
    phone = data.get("phone")
    text = data.get("text")

    # 1. Buscar as credenciais da Meta no banco (Token e Phone ID)
    # Aqui assumimos que você já tem essas configs ou usa as globais do .env
    access_token = "EAAL7kEUNnu0BQxBsXAzf2fZAx10X9t2YcB9cDJzR3F7VoSGAKoYTcpZCDPlzh73ViyZAfvQWxXPLQ4qjqPZAHhaLdCC2CMhyYKlW2ynvK4xIl6olTTcBpfXUjxOMbDtefJOiY8hdsmwgemXtk8I77vZBWvMEZBr4VdZBzacWYZBCeSMGTzyqyOpwoHTrlBAx9pUCiCbhll25LHGY3ekt1cKc1AhjpzACnniRJS3HSVB9z0clog1yXYDsSvv5vvHkiSYvuQZAGGl0ZCJaScj1EVHWiZAH1bn4zlZCGbyZA0QZDZD"
    phone_number_id = "956946084171393"

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
    
# ==========================================
# BUSCAR LISTA DE EMPRESAS (Painel Admin)
# ==========================================
@app.get("/api/admin/companies")
async def get_all_companies():
    try:
        with db_conn() as conn:
            with conn.cursor() as cur:
                # Busca as empresas (não trazemos a senha por segurança)
                cur.execute("SELECT id, name, email, phone, bot_whatsapp, created_at FROM companies ORDER BY created_at DESC")
                rows = cur.fetchall()
                
                if not rows:
                    return JSONResponse(content=[])

                colunas = [desc[0] for desc in cur.description]
                
                companies = []
                for row in rows:
                    d = dict(row) if isinstance(row, dict) or hasattr(row, 'keys') else dict(zip(colunas, row))
                    companies.append({
                        "id": str(d.get("id", "")),
                        "name": str(d.get("name", "")),
                        "email": str(d.get("email", "")),
                        "phone": str(d.get("phone", "")),
                        "bot_whatsapp": str(d.get("bot_whatsapp", "")),
                        "created_at": str(d.get("created_at", ""))
                    })
                return JSONResponse(content=companies)
    except Exception as e:
        print(f"Erro ao buscar empresas: {e}")
        return JSONResponse(content=[], status_code=500)    
    
# ==========================================
# ROTA SECRETA PARA ATUALIZAR O BANCO
# ==========================================
@app.get("/api/admin/fix-db")
def fix_database():
    try:
        with db_conn() as conn:
            with conn.cursor() as cur:
                # Colunas antigas que já adicionamos
                cur.execute("ALTER TABLE companies ADD COLUMN IF NOT EXISTS phone VARCHAR(50);")
                cur.execute("ALTER TABLE companies ADD COLUMN IF NOT EXISTS bot_whatsapp VARCHAR(50);")
                
                # 🔥 A NOVA GAVETA QUE ESTAVA FALTANDO:
                cur.execute("ALTER TABLE companies ADD COLUMN IF NOT EXISTS status VARCHAR(20) DEFAULT 'active';")
            conn.commit()
        return {"status": "Sucesso! O seu banco de dados foi atualizado com as novas colunas (incluindo o status)."}
    except Exception as e:
        return {"error": f"Erro ao atualizar banco: {str(e)}"}   
    
# ==========================================
# EXCLUIR EMPRESA
# ==========================================
@app.delete("/api/admin/companies/{company_id}")
def delete_company(company_id: str):
    try:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM companies WHERE id = %s", (company_id,))
            conn.commit()
        return {"status": "success", "message": "Empresa excluída!"}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

# ==========================================
# ATUALIZAR EMPRESA (EDITAR)
# ==========================================
@app.put("/api/admin/companies/{company_id}")
async def update_company(company_id: str, data: RegisterCompanyRequest):
    try:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """UPDATE companies SET name=%s, email=%s, phone=%s, bot_whatsapp=%s 
                       WHERE id=%s""",
                    (data.name, data.email, data.phone, data.bot_whatsapp, company_id)
                )
            conn.commit()
        return {"status": "success", "message": "Dados atualizados!"}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

# --- ROTA DE SEGURANÇA: TROCA DE SENHA ---
@app.post("/api/auth/change-password")
async def update_password(request: Request):
    try:
        # 1. Recebe os dados do React
        data = await request.json()
        new_password = data.get('password')
        user_email = data.get('email')

        # 2. Validação simples
        if not new_password or not user_email:
            return JSONResponse(
                status_code=400,
                content={"status": "error", "message": "Dados insuficientes para a troca."}
            )

        # Criptografa a senha para manter a tabela 'companies' segura também
        # (Usando a mesma função que você já tem no arquivo)
        hashed_pw = hash_password(new_password)

        # 3. Conecta no banco e atualiza as DUAS tabelas
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Atualiza a tabela USERS (Que usamos no login)
                cur.execute(
                    "UPDATE users SET password = %s WHERE email = %s",
                    (new_password, user_email)
                )
                linhas_alteradas = cur.rowcount
                
                # Atualiza a tabela COMPANIES (Para manter tudo sincronizado)
                cur.execute(
                    "UPDATE companies SET password = %s WHERE email = %s",
                    (hashed_pw, user_email)
                )

                # Se o e-mail não existia em lugar nenhum, barra aqui!
                if linhas_alteradas == 0:
                    return JSONResponse(
                        status_code=404,
                        content={"status": "error", "message": "Usuário não encontrado no banco."}
                    )
                    
            # O commit salva tudo de uma vez!
            conn.commit()

        # 4. RETORNO DE SUCESSO (A peça que faltava no seu código!)
        return {"status": "success", "message": "Senha alterada com sucesso!"}
        
    except Exception as e:
        print(f"Erro ao trocar senha: {e}")
        return JSONResponse(
            status_code=500, 
            content={"status": "error", "message": "Erro interno do servidor"}
        )

        # Retorno de sucesso (aqui pode ser direto, pois o padrão é 200)
        return {"status": "success", "message": "Senha atualizada com sucesso! ✅"}

    except Exception as e:
        print(f"❌ Erro na troca de senha: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": "Erro interno no servidor."}
        )

# --- ROTA PARA BUSCAR O FLUXO DO ROBÔ ---
@app.get("/api/config/flow/{flow_id}")
async def get_flow(flow_id: str):
    try:
        # Por enquanto, vamos retornar um fluxo vazio só para o React parar de dar erro
        # No futuro, aqui você vai buscar os dados reais do banco de dados
        return {
            "status": "success",
            "flow_id": flow_id,
            "nodes": [],  # Caixinhas do fluxo
            "edges": []   # Linhas conectando as caixinhas
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}, 500
    
@app.get("/api/descobrir-colunas")
def descobrir_colunas():
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'users';")
                colunas = cur.fetchall()
                
                # Como seu banco retorna Dicionários, vamos devolver direto para a tela!
                return {"status": "success", "colunas": colunas}
    except Exception as e:
        # Usando repr() para mostrar o nome exato do erro se acontecer de novo
        return {"status": "error", "erro_detalhado": repr(e)}

# --- CONFIGURAÇÕES DO GMAIL (Sempre ANTES da rota) ---
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 465
EMAIL_REMETENTE = "ctactsolution@gmail.com" 
# ⚠️ COLOQUE SUA NOVA SENHA AQUI EMBAIXO, TOTALMENTE JUNTA, SEM ESPAÇOS:
SENHA_APP_GOOGLE = "txaaarwiktppadai" 

def enviar_email_recuperacao(email_destino, nova_senha):
    try:
        msg = MIMEMultipart()
        msg['From'] = f"Contact Solution <{EMAIL_REMETENTE}>"
        msg['To'] = email_destino
        msg['Subject'] = "Sua Nova Senha Temporária - Contact Solution"

        corpo = f"""
        Olá,
        
        Recebemos uma solicitação de recuperação de senha para sua conta no Contact Solution.
        Sua nova senha temporária é: {nova_senha}
        
        Por favor, acesse o sistema com esta senha e altere-a imediatamente no seu perfil.
        
        Atenciosamente,
        Equipe Contact Solution.
        """
        msg.attach(MIMEText(corpo, 'plain'))

        with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as server:
            server.login(EMAIL_REMETENTE, SENHA_APP_GOOGLE)
            server.sendmail(EMAIL_REMETENTE, email_destino, msg.as_string())
            
        print(f"✅ E-mail enviado com sucesso para {email_destino}")
        return True
    except Exception as e:
        print(f"❌ Erro ao enviar e-mail: {e}")
        return False


# --- ROTA: ESQUECI MINHA SENHA (FASE 2 - COM GMAIL REAL) ---
@app.post("/api/auth/forgot-password")
async def forgot_password(request: Request):
    try:
        data = await request.json()
        email = data.get("email")

        if not email:
            return JSONResponse(status_code=400, content={"status": "error", "message": "E-mail não fornecido."})

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # 1. Verifica se o usuário existe
                cur.execute("SELECT email FROM users WHERE email = %s", (email,))
                user = cur.fetchone()

                if not user:
                    return JSONResponse(
                        status_code=404, 
                        content={"status": "error", "message": "E-mail não encontrado no sistema."}
                    )

                # 2. Gera a senha
                nova_senha = ''.join(random.choices(string.digits, k=6))
                senha_criptografada = hash_password(nova_senha)

                # 3. Atualiza o banco
                cur.execute("UPDATE users SET password = %s WHERE email = %s", (nova_senha, email))
                cur.execute("UPDATE companies SET password = %s WHERE email = %s", (senha_criptografada, email))
                conn.commit()

        # 4. O CARTEIRO REAL (Chama a função ali de cima)
        email_enviado = enviar_email_recuperacao(email, nova_senha)

        # 5. O Retorno correto (SEM MOSTRAR A SENHA NO POP-UP!)
        if email_enviado:
            return {"status": "success", "message": "✅ E-mail enviado! Verifique sua caixa de entrada e spam."}
        else:
            return JSONResponse(status_code=500, content={"status": "error", "message": "Senha alterada, mas falha ao enviar o e-mail."})

    except Exception as e:
        print(f"Erro no forgot-password: {e}")
        return JSONResponse(status_code=500, content={"status": "error", "message": "Erro interno no servidor"})
    
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))