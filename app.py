from flask import Flask, request, jsonify
import os
import requests

app = Flask(__name__)

VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "contact-solution-token")
WHATSAPP_TOKEN = os.getenv("WHATSAPP_TOKEN")

@app.get("/")
def home():
    return "ok", 200

@app.get("/webhook")
def verify():
    mode = request.args.get("hub.mode")
    token = request.args.get("hub.verify_token")
    challenge = request.args.get("hub.challenge")

    if mode == "subscribe" and token == VERIFY_TOKEN:
        return challenge, 200

    return "", 403


@app.post("/webhook")
def webhook():
    data = request.get_json(silent=True) or {}

    phone = "desconhecido"
    text = ""

    try:
        message = (
            data.get("entry", [{}])[0]
                .get("changes", [{}])[0]
                .get("value", {})
                .get("messages", [{}])[0]
        )

        phone = message.get("from", "desconhecido")
        text = (
            message.get("text", {})
                   .get("body", "")
                   .strip()
                   .lower()
        )

        print("Telefone:", phone)
        print("Mensagem:", text)

        # LÓGICA DE ATENDIMENTO
        if "oi" in text or "olá" in text:
            resposta = (
                "Olá! 👋\n"
                "Sou o atendimento automático 🤖\n\n"
                "Digite:\n"
                "1️⃣ para Vendas\n"
                "2️⃣ para Suporte"
            )

        elif text == "1":
            resposta = "Perfeito! 📦 Vou te encaminhar para Vendas."

        elif text == "2":
            resposta = "Certo! 🛠️ Vou te encaminhar para o Suporte."

        else:
            resposta = (
                "Não entendi sua mensagem 😅\n"
                "Digite *oi* para começar o atendimento."
            )

        print("Resposta gerada:", resposta)

    except Exception as e:
        print("Erro ao processar mensagem:", e)

    return jsonify(status="ok"), 200

