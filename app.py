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

DB_URL = "postgresql://postgres:17121983casamento@db.tmpjydwfzrhcrquwamgx.supabase.co:5432/postgres"


# CONFIGURAÇÕES DO WHATSAPP (Substitua pelos seus dados)
WA_TOKEN = "EAAL7kEUNnu0BQ91oGZBGsJ7XqJnTfa5oOtveqZBMjZCKMcPGOvKiZAhh6tlvO74OrHo5D7Lwfo15ZCkyvFzlKin1m7FhjuF8N0FSWaT6p4rViZBz403emicPVXlLAzKJ6ZBkeywra3YjpZCYYLFZCiFNAN2NEDBTpVyqZBg9UZA2P2T8oyrRAZCZApMC60xuwNCCfMQxwJNGQgID8RSxPtHZAFT2ZCa4jRgbDJsdQAfiNUiN4qoLZCIIkd5YRY9koJqowMy3WkPQrZAofa6tpAmZCdAxmvKDdQHE5ebo9ikaNJBzcZD"
WA_PHONE_ID = "956946084171393"

import psycopg
from psycopg.rows import dict_row

def get_db_connection():
    # Abre a conexão com o Supabase usando a versão 3 (mais rápida e moderna)
    return psycopg.connect(DB_URL, row_factory=dict_row)


# Esta função vai criar as tabelas que estão faltando na imagem 049d22.png
def setup_database_tables():
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS flows (
                id TEXT PRIMARY KEY,
                company_id TEXT, 
                name TEXT NOT NULL,
                messages JSONB NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS conversations (
                id SERIAL PRIMARY KEY,
                phone TEXT UNIQUE,
                step TEXT DEFAULT '0',
                status_funil TEXT DEFAULT 'bot',
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()
        print("✅ Tabelas criadas no Supabase com sucesso!")
    finally:
        cur.close()
        conn.close()

# ---------------------------
# Configurações e Logging
# ---------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("contact-solution")

VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "")

# Agora o link está direto, sem o os.getenv atrapalhando!
DATABASE_URL = "postgresql://postgres:17121983casamento@db.tmpjydwfzrhcrquwamgx.supabase.co:5432/postgres"

def get_db_connection():
    # Esta função abre a porta do banco de dados usando a URL do Supabase
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
# 📱 MOTOR DE ENVIO (WHATSAPP CLOUD API)
# ==========================================
# Aqui vão as suas credenciais oficiais do painel da Meta:
WHATSAPP_TOKEN = "SEU_TOKEN_PERMANENTE_AQUI"
PHONE_NUMBER_ID = "SEU_ID_DO_NUMERO_AQUI"

def enviar_mensagem_wpp(numero_destino, texto_mensagem):
    # Usamos a versão 18.0 da API (pode ser 19.0 dependendo de quando você criou o app)
    url = f"https://graph.facebook.com/v18.0/{PHONE_NUMBER_ID}/messages"
    
    headers = {
        "Authorization": f"Bearer {WHATSAPP_TOKEN}",
        "Content-Type": "application/json"
    }
    
    # O pacote no formato exato que a Meta exige
    payload = {
        "messaging_product": "whatsapp",
        "to": numero_destino,
        "type": "text",
        "text": {"body": texto_mensagem}
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload)
        # Imprime no terminal se deu certo ou se a Meta bloqueou
        print(f"📡 Status Meta: {response.status_code} | Resposta: {response.text}")
        return response.status_code == 200
    except Exception as e:
        print(f"❌ Erro crítico ao conectar com a Meta: {e}")
        return False

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

# ==============================================================================
# 🗄️ GERENCIAMENTO DE TABELAS DO BANCO DE DADOS (POSTGRESQL)
# Este bloco garante que as tabelas necessárias existam sempre que o servidor ligar.
# Local: Geralmente logo após as configurações de conexão do DB.
# ==============================================================================
def setup_database_tables():
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # 1. Tabela de Empresas (A que está faltando no seu log!)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS companies (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT,
                phone TEXT,
                bot_whatsapp TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # 2. Criar a empresa MASTER automaticamente
        cur.execute("""
            INSERT INTO companies (id, name) 
            VALUES ('MASTER', 'Minha Empresa Principal') 
            ON CONFLICT (id) DO NOTHING;
        """)

        # 3. Tabela de Funis (Flows)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS flows (
                id TEXT PRIMARY KEY,
                company_id TEXT REFERENCES companies(id), 
                name TEXT NOT NULL,
                messages JSONB NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # 4. Tabela de Conversas (Leads)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS conversations (
                id SERIAL PRIMARY KEY,
                company_id TEXT REFERENCES companies(id),
                phone TEXT UNIQUE,
                step TEXT DEFAULT '0',
                status_funil TEXT DEFAULT 'bot',
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        conn.commit()
        print("✅ [FILTRO OK] Todas as tabelas e a empresa MASTER foram criadas!")
    except Exception as e:
        print(f"❌ Erro ao criar tabelas: {e}")
    finally:
        cur.close()
        conn.close()

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
                # 1. Registrar Mensagem do Cliente (Inbound)
                cur.execute("INSERT INTO messages (company_id, phone, direction, text) VALUES (%s, %s, %s, %s)",
                            (company_id, phone, 'inbound', text))
                
                # 2. Buscar Estado da Conversa
                cur.execute("SELECT * FROM conversations WHERE company_id=%s AND phone=%s", (company_id, phone))
                conv = cur.fetchone()
                
                # Se for um cliente novo, começa no passo '0' (Primeira mensagem do funil)
                if not conv:
                    cur.execute("INSERT INTO conversations (company_id, phone, status_funil, step) VALUES (%s, %s, 'bot', '0') RETURNING *",
                                (company_id, phone))
                    conv = cur.fetchone()

                # 3. Lógica do Funil Dinâmico (Se o Bot estiver Ativo)
                # Obs: Aqui assumimos que 'status' ou 'status_funil' controla se o bot fala. 
                # Adapte o nome da coluna se for diferente no seu banco.
                if conv.get('status_funil') == 'bot' or conv.get('status') == 'open':
                    
                    # A) Busca o funil que acabamos de salvar no React!
                    cur.execute("SELECT messages FROM flows WHERE company_id=%s LIMIT 1", (company_id,))
                    flow_row = cur.fetchone()
                    
                    if flow_row:
                        # Pega a lista de mensagens (O banco já devolve como lista graças ao JSONB)
                        flow_messages = flow_row['messages'] if isinstance(flow_row, dict) else flow_row[0]
                        
                        # Converte o passo atual para número (ex: '0' -> 0)
                        current_step = int(conv['step']) if str(conv['step']).isdigit() else 0
                        
                        # B) Verifica se ainda temos mensagens no funil para enviar
                        if current_step < len(flow_messages):
                            mensagem_do_robo = flow_messages[current_step]
                            
                            # Só envia se a caixa de texto não estiver vazia no React
                            if mensagem_do_robo.strip() != "":
                                
                                # ==========================================
                                cur.execute("UPDATE conversations SET status_funil='humano', updated_at=NOW() WHERE id=%s", (conv['id'],))
                                
                                
                                # ==========================================
                                
                                # Salva o que o robô falou no banco de dados (Outbound)
                                cur.execute("INSERT INTO messages (company_id, phone, direction, text) VALUES (%s, %s, %s, %s)",
                                            (company_id, phone, 'outbound', mensagem_do_robo))
                            
                            # C) Avança o cliente para o próximo passo da conversa
                            next_step = str(current_step + 1)
                            cur.execute("UPDATE conversations SET step=%s, updated_at=NOW() WHERE id=%s", (next_step, conv['id']))
                            
                        else:
                            # Se acabaram as mensagens do funil, podemos transferir para um humano
                            cur.execute("UPDATE conversations SET status_funil='humano', updated_at=NOW() WHERE id=%s", (conv['id'],))
                
            conn.commit()
        return {"status": "ok"}
    except Exception as e:
        logger.error(f"Erro Webhook: {e}")
        return {"status": "error"}
    
@app.post("/api/send-message")
async def api_send_message(request: Request):
    data = await request.json()
    # Pequeno ajuste na captura do ID para evitar erros
    company_id = data.get("companyId") or data.get("company_id") or "MASTER"
    phone = data.get("phone")
    text = data.get("text")

    # Credenciais da Meta (Obrigado por mascarar!)
    access_token = "EAAL7kEUNnu0BQ9z1vZC2FEgsrcQDx5doZApm7WRm9ZCTOZCMEm5fyZBusx1r972lRGdoxCC3ieEsPUdCiuwQU4abhdve33pi1ZAAJephw4Rg5zi96Pm1EVZBi4yCJp2slypvP6KOtFbGgddeCnCLy9ZBMVzhpGzkxrwWTPLibgiApF3eMU7Jw7H5QvFnByKEsbBRlTqI7bJ6Ew58Sh0Cx0NCHnzxrqEoK4oc1Yh1hAGhZAUcqBugqnY5ekfT84vanMNtithseLvPzZBj4aAcVsZCVOctiyAjFnfywkZCNAZDZD"
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
            # 3. SALVAMENTO CORRIGIDO PARA O SEU SUPABASE
            conn = get_db_connection() # Usando a função que já funciona!
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO messages (company_id, phone, direction, text) VALUES (%s, %s, %s, %s)",
                (company_id, phone, 'outbound', text)
            )
            conn.commit()
            cur.close()
            conn.close()
            return {"status": "ok"}
        else:
            return JSONResponse(status_code=400, content=response.json())
    except Exception as e:
        print(f"Erro ao disparar WhatsApp: {e}")
        return JSONResponse(status_code=500, content={"error": "Falha ao enviar"})    
    

@app.post("/webhook")
async def whatsapp_webhook(request: Request):
    data = await request.json()
    
    try:
        entry = data.get("entry", [{}])[0]
        changes = entry.get("changes", [{}])[0]
        value = changes.get("value", {})
        
        # Só processa se for uma MENSAGEM de um cliente (ignora confirmações de leitura/entrega)
        if "messages" in value:
            import json
            import requests # Necessário para enviar a mensagem real
            
            message = value["messages"][0]
            sender_phone = message["from"]
            text_received = message.get("text", {}).get("body", "").lower()
            
            print(f"📩 Cliente {sender_phone} enviou: {text_received}")

            # 1. BUSCAR A RESPOSTA NO SUPABASE
            conn = get_db_connection()
            cur = conn.cursor()
            
            cur.execute("SELECT messages FROM flows WHERE company_id = 'MASTER' LIMIT 1")
            row = cur.fetchone()
            
            if row:
                raw_data = row['messages'] if isinstance(row, dict) else row[0]
                funil_data = json.loads(raw_data) if isinstance(raw_data, str) else raw_data
                
                if isinstance(funil_data, list) and len(funil_data) > 0:
                    msg_obj = funil_data[0]
                    resposta_texto = msg_obj.get("text", "Olá! Como posso ajudar?") if isinstance(msg_obj, dict) else str(msg_obj)
                    
                    print(f"⚙️ Preparando para enviar: {resposta_texto}")

                    # ---------------------------------------------------------
                    # 2. A BOCA DO ROBÔ: ENVIAR PARA A META (WHATSAPP REAL)
                    # ---------------------------------------------------------
                    access_token = "EAAL7kEUNnu0BQ9z1vZC2FEgsrcQDx5doZApm7WRm9ZCTOZCMEm5fyZBusx1r972lRGdoxCC3ieEsPUdCiuwQU4abhdve33pi1ZAAJephw4Rg5zi96Pm1EVZBi4yCJp2slypvP6KOtFbGgddeCnCLy9ZBMVzhpGzkxrwWTPLibgiApF3eMU7Jw7H5QvFnByKEsbBRlTqI7bJ6Ew58Sh0Cx0NCHnzxrqEoK4oc1Yh1hAGhZAUcqBugqnY5ekfT84vanMNtithseLvPzZBj4aAcVsZCVOctiyAjFnfywkZCNAZDZD"  # <-- ATENÇÃO AQUI
                    phone_number_id = "956946084171393"  # <-- ATENÇÃO AQUI
                    
                    url = f"https://graph.facebook.com/v18.0/{phone_number_id}/messages"
                    headers = {
                        "Authorization": f"Bearer {access_token}",
                        "Content-Type": "application/json"
                    }
                    payload = {
                        "messaging_product": "whatsapp",
                        "to": sender_phone,
                        "type": "text",
                        "text": {"body": resposta_texto}
                    }

                    # Dispara a mensagem!
                    response = requests.post(url, json=payload, headers=headers)

                    if response.status_code == 200:
                        print("✅ Mensagem entregue com sucesso pela Meta!")
                        
                        # 3. SALVAR NO HISTÓRICO PARA APARECER NO SEU PAINEL REACT
                        cur.execute(
                            "INSERT INTO messages (company_id, phone, direction, text) VALUES (%s, %s, %s, %s)",
                            ('MASTER', sender_phone, 'outbound', resposta_texto)
                        )
                    else:
                        print(f"❌ Erro ao enviar pela Meta: {response.text}")
            
            conn.commit()
            cur.close()
            conn.close()

    except Exception as e:
        print(f"❌ Erro fatal no webhook: {e}")

    # A Meta exige que sempre retornemos 200 OK rapidamente
    return {"status": "ok"}

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

# ==========================================
# 1. ROTA PARA BUSCAR OS FUNIS (Resolve o 404)
# ==========================================
@app.get("/api/config/flow")
async def get_all_flows(companyId: str):
    conn = get_db_connection() 
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT id, name, messages FROM flows WHERE company_id = %s",
            (companyId,)
        )
        rows = cur.fetchall()
        
        flows = []
        for row in rows:
            # Se o seu banco devolve um Dicionário (o mais provável pelo erro "0")
            if isinstance(row, dict):
                flows.append({
                    "id": row.get("id"),
                    "nome": row.get("name"),
                    "messages": row.get("messages")
                })
            # Se o seu banco devolve uma Tupla (lista simples)
            else:
                flows.append({
                    "id": row[0],
                    "nome": row[1],
                    "messages": row[2] 
                })
                
        return flows
    except Exception as e:
        print(f"Erro ao buscar funis: {e}")
        return []
    finally:
        cur.close()
        conn.close()

# ==========================================
# 2. ROTA PARA SALVAR OS FUNIS (Resolve o 405)
# ==========================================
@app.post("/api/config/flow")
async def save_flow(request: Request):
    data = await request.json()
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        query = """
            INSERT INTO flows (id, company_id, name, messages)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id) 
            DO UPDATE SET 
                name = EXCLUDED.name,
                messages = EXCLUDED.messages,
                updated_at = CURRENT_TIMESTAMP
        """
        cur.execute(query, (
            data['flowId'],
            data['companyId'],
            data['flowName'],
            json.dumps(data['flow_messages']) 
        ))
        conn.commit()
        return {"status": "success"}
    except Exception as e:
        conn.rollback()
        print(f"Erro ao salvar no Postgres: {e}")
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e)})
    finally:
        cur.close()
        conn.close()
    
@app.get("/api/force-db")
async def force_db_creation():
    try:
        setup_database_tables() # Chama a nossa função de ontem
        return {"status": "sucesso", "mensagem": "Tabelas verificadas/criadas com sucesso no Postgres!"}
    except Exception as e:
        return {"status": "erro", "mensagem": str(e)}

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