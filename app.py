from flask import Flask, request, jsonify
import requests
import os

app = Flask(__name__)

VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "contact-solution-token")
WHATSAPP_TOKEN = os.getenv("WHATSAPP_TOKEN")

@app.route("/webhook", methods=["GET"])
def verify():
    mode = request.args.get("hub.mode")
    token = request.args.get("hub.verify_token")
    challenge = request.args.get("hub.challenge")

    if mode == "subscribe" and token == VERIFY_TOKEN:
        return challenge, 200
    return "Unauthorized", 403

@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json()
    print(data)

    try:
        message = data["entry"][0]["changes"][0]["value"]["messages"][0]
        phone = message["from"]

        send_message(phone, "Mensagem recebida com sucesso ✅")
    except Exception as e:
        print("Erro:", e)

    return jsonify(status="ok"), 200


def send_message(phone, text):
    url = "https://graph.facebook.com/v22.0/me/messages"
    headers = {
        "Authorization": f"Bearer {WHATSAPP_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "messaging_product": "whatsapp",
        "to": phone,
        "text": {"body": text}
    }
    requests.post(url, json=payload, headers=headers)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
