"""
Discord Voice Channel Presence Bot
-----------------------------------
Connects to the Discord Gateway and joins a voice channel, keeping the
bot present indefinitely.  All credentials and settings are read from
config.json at startup.

config.json fields
------------------
token       : Bot token (from Discord Developer Portal)
guild_id    : ID of the server (guild) to join
channel_id  : ID of the voice channel to join
status      : Presence status shown for the bot ("online", "idle", "dnd", "invisible")
self_mute   : Whether the bot joins muted  (true / false)
self_deaf   : Whether the bot joins deafened (true / false)
"""

import json
import time
import threading
import logging
import sys

import requests
import websocket

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
CONFIG_FILE = "config.json"

def load_config() -> dict:
    with open(CONFIG_FILE, "r") as f:
        return json.load(f)

# ---------------------------------------------------------------------------
# Discord Gateway constants
# ---------------------------------------------------------------------------
GATEWAY_VERSION = 10
GATEWAY_URL = f"wss://gateway.discord.gg/?v={GATEWAY_VERSION}&encoding=json"

# Opcodes
OP_DISPATCH          = 0
OP_HEARTBEAT         = 1
OP_IDENTIFY          = 2
OP_PRESENCE_UPDATE   = 3
OP_VOICE_STATE_UPDATE = 4
OP_RESUME            = 6
OP_RECONNECT         = 7
OP_REQUEST_GUILD_MEMBERS = 8
OP_INVALID_SESSION   = 9
OP_HELLO             = 10
OP_HEARTBEAT_ACK     = 11

# ---------------------------------------------------------------------------
# Bot
# ---------------------------------------------------------------------------
class DiscordVoiceBot:
    def __init__(self, config: dict):
        self.token      = config["token"]
        self.guild_id   = str(config["guild_id"])
        self.channel_id = str(config["channel_id"])
        self.status     = config.get("status", "online")
        self.self_mute  = config.get("self_mute", True)
        self.self_deaf  = config.get("self_deaf", True)

        self.ws: websocket.WebSocketApp | None = None
        self.heartbeat_interval: float = 0
        self.last_sequence: int | None = None
        self.session_id: str | None = None
        self.resume_gateway_url: str | None = None
        self._heartbeat_thread: threading.Thread | None = None
        self._stop_heartbeat = threading.Event()
        self._ack_received = True  # assume ack on first beat

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------
    def run(self):
        while True:
            try:
                url = self.resume_gateway_url or GATEWAY_URL
                log.info("Connecting to Gateway: %s", url)
                self.ws = websocket.WebSocketApp(
                    url,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                self.ws.run_forever(ping_interval=0)
            except Exception as exc:
                log.error("WebSocket crashed: %s", exc)
            log.info("Reconnecting in 5 seconds …")
            time.sleep(5)

    # ------------------------------------------------------------------
    # WebSocket callbacks
    # ------------------------------------------------------------------
    def _on_open(self, ws):
        log.info("WebSocket connection opened.")

    def _on_message(self, ws, raw: str):
        data = json.loads(raw)
        op  = data.get("op")
        seq = data.get("s")
        evt = data.get("t")
        d   = data.get("d")

        if seq is not None:
            self.last_sequence = seq

        if op == OP_HELLO:
            self.heartbeat_interval = d["heartbeat_interval"] / 1000.0
            self._start_heartbeat()
            if self.session_id:
                self._resume()
            else:
                self._identify()

        elif op == OP_HEARTBEAT_ACK:
            self._ack_received = True
            log.debug("Heartbeat ACK received.")

        elif op == OP_HEARTBEAT:
            # Server requested an immediate heartbeat
            self._send_heartbeat()

        elif op == OP_RECONNECT:
            log.info("Server requested reconnect.")
            self._close()

        elif op == OP_INVALID_SESSION:
            resumable = bool(d)
            log.warning("Invalid session (resumable=%s).", resumable)
            if not resumable:
                self.session_id = None
                self.resume_gateway_url = None
                self.last_sequence = None
            time.sleep(2)
            self._close()

        elif op == OP_DISPATCH:
            self._handle_dispatch(evt, d)

    def _on_error(self, ws, error):
        log.error("WebSocket error: %s", error)

    def _on_close(self, ws, code, reason):
        log.info("WebSocket closed (code=%s reason=%s).", code, reason)
        self._stop_heartbeat.set()

    # ------------------------------------------------------------------
    # Dispatch events
    # ------------------------------------------------------------------
    def _handle_dispatch(self, event: str, data: dict):
        if event == "READY":
            self.session_id = data["session_id"]
            self.resume_gateway_url = data.get("resume_gateway_url", GATEWAY_URL)
            user = data.get("user", {})
            log.info(
                "Logged in as %s#%s (id=%s)",
                user.get("username"), user.get("discriminator"), user.get("id"),
            )
            self._join_voice_channel()
            self._update_presence()

        elif event == "RESUMED":
            log.info("Session resumed successfully.")
            self._join_voice_channel()

        elif event == "VOICE_STATE_UPDATE":
            uid = (data.get("member") or {}).get("user", {}).get("id") or data.get("user_id")
            log.debug("VOICE_STATE_UPDATE user_id=%s channel_id=%s", uid, data.get("channel_id"))

    # ------------------------------------------------------------------
    # Gateway payloads
    # ------------------------------------------------------------------
    def _identify(self):
        payload = {
            "op": OP_IDENTIFY,
            "d": {
                "token": self.token,
                "intents": 0,          # no privileged intents needed for voice presence
                "properties": {
                    "os":      "linux",
                    "browser": "discord-voice-bot",
                    "device":  "discord-voice-bot",
                },
                "presence": self._presence_payload(),
            },
        }
        self._send(payload)
        log.info("Sent IDENTIFY.")

    def _resume(self):
        payload = {
            "op": OP_RESUME,
            "d": {
                "token":      self.token,
                "session_id": self.session_id,
                "seq":        self.last_sequence,
            },
        }
        self._send(payload)
        log.info("Sent RESUME (session=%s seq=%s).", self.session_id, self.last_sequence)

    def _join_voice_channel(self):
        payload = {
            "op": OP_VOICE_STATE_UPDATE,
            "d": {
                "guild_id":   self.guild_id,
                "channel_id": self.channel_id,
                "self_mute":  self.self_mute,
                "self_deaf":  self.self_deaf,
            },
        }
        self._send(payload)
        log.info(
            "Joining voice channel %s in guild %s (mute=%s deaf=%s).",
            self.channel_id, self.guild_id, self.self_mute, self.self_deaf,
        )

    def _update_presence(self):
        payload = {
            "op": OP_PRESENCE_UPDATE,
            "d": self._presence_payload(),
        }
        self._send(payload)

    def _presence_payload(self) -> dict:
        return {
            "since":      None,
            "activities": [],
            "status":     self.status,
            "afk":        False,
        }

    # ------------------------------------------------------------------
    # Heartbeat
    # ------------------------------------------------------------------
    def _start_heartbeat(self):
        self._stop_heartbeat.clear()
        self._ack_received = True
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop, daemon=True
        )
        self._heartbeat_thread.start()
        log.info("Heartbeat started (interval=%.2fs).", self.heartbeat_interval)

    def _heartbeat_loop(self):
        # Jitter: wait a random fraction of the interval before the first beat
        import random
        time.sleep(self.heartbeat_interval * random.random())
        while not self._stop_heartbeat.is_set():
            if not self._ack_received:
                log.warning("No heartbeat ACK — connection may be zombied. Reconnecting.")
                self._close()
                return
            self._send_heartbeat()
            self._ack_received = False
            self._stop_heartbeat.wait(self.heartbeat_interval)

    def _send_heartbeat(self):
        self._send({"op": OP_HEARTBEAT, "d": self.last_sequence})
        log.debug("Heartbeat sent (seq=%s).", self.last_sequence)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _send(self, payload: dict):
        if self.ws:
            self.ws.send(json.dumps(payload))

    def _close(self):
        if self.ws:
            self.ws.close()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    config = load_config()
    log.info("Loaded config from %s.", CONFIG_FILE)
    bot = DiscordVoiceBot(config)
    bot.run()
