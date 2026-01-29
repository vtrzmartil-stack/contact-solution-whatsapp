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

    return "Unauthorized", 403

@app.post("/webhook")
def webhook():
    data = request.get_json(silent=True) or {}
    print(data)

    # por enquanto só responde ok pro WhatsApp não tentar reenviar
    return jsonify(status="ok"), 200
