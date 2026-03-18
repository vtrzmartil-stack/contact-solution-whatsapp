import requests
import json


# ==========================================
# 🔑 SUAS CHAVES MÁGICAS AQUI
# ==========================================
TOKEN = "EAAL7kEUNnu0BQ7E3oS0333DNnsgYofMwp84JSAKz7doHD7kkkR53GeuYVGfVrC7KAYfMHLaRNDq8HUNTlZBp1y3aoHzmdQULA7hcUsxWlcATZCtfXdMNCEmCePToIyf7IOktm9UBv0R5XxGECDQ95ZBDKlnfoMatLgz3TVzMtBaOLkd24q6dmSB8xu3HoAGVLzyHpmAgZAZCEymtAAu1toNPyVLZCGyZBr0ERlY3oAaTn29RQVZCFjwVge82d3kVFZBnug4Wt4GULytrKTwOTaYIOAReu1T4qViYun40ZD"
PHONE_NUMBER_ID = "956946084171393"

# Coloque o seu número que você verificou lá no painel. 
# Formato: Código do País (55) + DDD + Número. Tudo junto, sem + ou traços.
# Exemplo: "5511999998888"
NUMERO_DESTINO = "5511964816315" 
# ==========================================

# A URL oficial da API da Meta
url = f"https://graph.facebook.com/v19.0/{PHONE_NUMBER_ID}/messages"

# Os cabeçalhos de autorização (é aqui que o Token entra)
headers = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}

# O corpo da mensagem. Vamos usar o template padrão "hello_world" que a Meta 
# já deixa pré-aprovado para testes.
payload = {
    "messaging_product": "whatsapp",
    "to": NUMERO_DESTINO,
    "type": "template",
    "template": {
        "name": "hello_world",
        "language": {
            "code": "en_US" # O template padrão de teste deles é em inglês
        }
    }
}

print("Enviando mensagem para o WhatsApp...")

# O disparo do foguete! 🚀
response = requests.post(url, headers=headers, data=json.dumps(payload))

# Analisando a resposta da Meta
if response.status_code == 200:
    print("✅ MENSAGEM ACEITA PELA META! O recibo deles é:")
    print(response.json()) # <--- Adicionamos essa linha para ver o retorno!
else:
    print("❌ ERRO AO ENVIAR:")
    print(response.json())