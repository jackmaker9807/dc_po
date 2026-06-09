import json
import time
import requests
import websocket
import threading
import sys
import random

# Load config
try:
    with open('config.json', 'r') as f:
        config = json.load(f)
except FileNotFoundError:
    print("config.json tidak ditemukan!")
    sys.exit(1)

TOKEN = config['token']
GUILD_ID = config['guild_id']
CHANNEL_ID = config['channel_id']
STATUS = config.get('status', 'online')
SELF_MUTE = config.get('self_mute', True)
SELF_DEAF = config.get('self_deaf', True)

HEADERS = {
    "Authorization": TOKEN,
    "Content-Type": "application/json"
}

def validate_token():
    r = requests.get("https://discord.com/api/v9/users/@me", headers=HEADERS)
    if r.status_code != 200:
        print("Token invalid atau expired!")
        sys.exit(1)
    user = r.json()
    print(f"Logged in as {user['username']}#{user['discriminator']}")

def get_gateway():
    r = requests.get("https://discord.com/api/v9/gateway/bot", headers=HEADERS)
    if r.status_code != 200:
        print("Gagal ambil gateway!")
        return None
    return r.json()['url'] + "?v=9&encoding=json"

def on_message(ws, message):
    data = json.loads(message)
    op = data.get('op')

    if op == 0:  # Dispatch
        if data['t'] == 'READY':
            print("Ready! Joining voice...")
            send_voice_state(ws)

    elif op == 10:  # Hello
        heartbeat_interval = data['d']['heartbeat_interval'] / 1000
        threading.Thread(target=heartbeat, args=(ws, heartbeat_interval), daemon=True).start()
        identify(ws)

    elif op == 11:  # Heartbeat ACK
        pass  # OK

    elif op == 7 or op == 9:  # Reconnect / Invalid session
        print("Reconnecting...")
        ws.close()

def heartbeat(ws, interval):
    while True:
        time.sleep(interval * random.uniform(0.9, 1.1))
        ws.send(json.dumps({"op": 1, "d": None}))

def identify(ws):
    payload = {
        "op": 2,
        "d": {
            "token": TOKEN,
            "properties": {
                "$os": "windows",
                "$browser": "chrome",
                "$device": "pc"
            },
            "presence": {
                "status": STATUS,
                "since": 0,
                "afk": False
            }
        }
    }
    ws.send(json.dumps(payload))

def send_voice_state(ws):
    payload = {
        "op": 4,
        "d": {
            "guild_id": GUILD_ID,
            "channel_id": CHANNEL_ID,
            "self_mute": SELF_MUTE,
            "self_deaf": SELF_DEAF,
            "self_stream": False,
            "self_video": False
        }
    }
    ws.send(json.dumps(payload))
    print(f"Joined voice channel {CHANNEL_ID} in guild {GUILD_ID}")

def on_error(ws, error):
    print(f"Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("Connection closed, reconnecting in 5s...")
    time.sleep(5)
    run()

def run():
    gateway = get_gateway()
    if not gateway:
        time.sleep(10)
        run()
        return

    ws = websocket.WebSocketApp(
        gateway,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        header=HEADERS
    )
    ws.run_forever(ping_interval=30, ping_timeout=10)

if __name__ == "__main__":
    validate_token()
    print("Starting selfbot voice 24/7...")
    run()