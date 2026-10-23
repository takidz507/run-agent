#!/usr/bin/env python3
# Run Agent Bridge — Muse's permanent member inside a private Run room.
# Hand-rolled MQTT 3.1.1 over WebSocket (WSS), same brokers/topics/protocol as Run.
import json, os, random, ssl, struct, sys, threading, time, hashlib
from datetime import datetime, timezone
import websocket

BASE = os.path.dirname(os.path.abspath(__file__))
CFG = json.load(open(os.path.join(BASE, "config.json"), encoding="utf-8"))
ROOM = CFG["room_code"].strip().upper()
BROKERS = CFG["brokers"]
AGENT_ID = CFG.get("agent_user_id", "agent-mous")
AGENT_NAME = CFG.get("agent_name", "موس")
DEVICE_HASH = hashlib.sha256(("run-agent-device-" + AGENT_ID).encode()).hexdigest()[:32]
STATE_F = os.path.join(BASE, "state.json")
INBOX_F = os.path.join(BASE, "inbox.jsonl")
OUTBOX_F = os.path.join(BASE, "outbox.jsonl")
OUTBOX_SENT_F = os.path.join(BASE, "outbox.sent")  # persistent cursor: how many outbox lines were already sent
TRANSCRIPT_F = os.path.join(BASE, "transcript.txt")
LOG_F = os.path.join(BASE, "bridge.log")

TOPIC_ROOM = f"runapp/v1/room/{ROOM}"
TOPIC_MY_PRESENCE = f"runapp/v1/presence/{AGENT_ID}"
TOPIC_MY_ROOM_PRESENCE = f"{TOPIC_ROOM}/presence/{AGENT_ID}"
TOPIC_ACKS = f"{TOPIC_ROOM}/acks"
TOPIC_TYPING = f"{TOPIC_ROOM}/typing"

lock = threading.Lock()
state = {"room": None, "seen": [], "read_acked": [], "delivered_acked": []}
if os.path.exists(STATE_F):
    try: state.update(json.load(open(STATE_F, encoding="utf-8")))
    except Exception: pass
seen = set(state["seen"]); read_acked = set(state["read_acked"]); deliv_acked = set(state["delivered_acked"])

def log(*a):
    line = time.strftime("%Y-%m-%d %H:%M:%S") + " " + " ".join(str(x) for x in a)
    print(line, flush=True)
    try:
        with open(LOG_F, "a", encoding="utf-8") as f: f.write(line + "\n")
    except Exception: pass

def now_iso():
    n = datetime.now(timezone.utc)
    return n.strftime("%Y-%m-%dT%H:%M:%S.") + f"{n.microsecond // 1000:03d}Z"

def new_id(prefix="m"):
    t = format(int(time.time() * 1000), "x")
    r = "".join(random.choice("0123456789abcdefghijklmnopqrstuvwxyz") for _ in range(6))
    return f"{prefix}-{t}{r}"

def save_state():
    with lock:
        state["seen"] = sorted(seen); state["read_acked"] = sorted(read_acked); state["delivered_acked"] = sorted(deliv_acked)
        tmp = STATE_F + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f: json.dump(state, f, ensure_ascii=False)
        os.replace(tmp, STATE_F)

def transcript(sym, who, text, ts=""):
    with open(TRANSCRIPT_F, "a", encoding="utf-8") as f:
        f.write(f"{sym} {who} {ts} {text}\n".replace("\n ", " "))

# ---------------- MQTT over WebSocket ----------------
class MqttWs:
    def __init__(self, url):
        self.url = url
        self.ws = None
        self.pkt_id = 0
        self.connected = threading.Event()
        self.on_message = None
    def _send(self, data): self.ws.send(data, opcode=websocket.ABNF.OPCODE_BINARY)
    @staticmethod
    def _enc_len(n):
        out = b""
        while True:
            d = n % 128; n //= 128
            out += bytes([d | (0x80 if n else 0)])
            if not n: return out
    def connect(self):
        self.ws = websocket.create_connection(self.url, subprotocols=["mqtt"], timeout=20,
                                              sslopt={"cert_reqs": ssl.CERT_REQUIRED})
        self.ws.settimeout(None)
        cid = "run-agent-" + new_id("b")[2:]
        body = b"\x00\x04MQTT\x04\x02\x00\x3c"  # protocol, clean session, keepalive 60
        payload = struct.pack("!H", len(cid)) + cid.encode()
        vh = body
        self._send(b"\x10" + self._enc_len(len(vh) + len(payload)) + vh + payload)
    def subscribe(self, topic, qos=0):
        self.pkt_id += 1
        pid = self.pkt_id
        body = struct.pack("!H", pid) + struct.pack("!H", len(topic)) + topic.encode() + bytes([qos])
        self._send(b"\x82" + self._enc_len(len(body)) + body)
    def publish(self, topic, payload, qos=0, retain=False):
        if isinstance(payload, str): payload = payload.encode()
        flags = (1 if retain else 0) | (qos << 1)
        vh = struct.pack("!H", len(topic)) + topic.encode()
        if qos > 0:
            self.pkt_id += 1; vh += struct.pack("!H", self.pkt_id)
        self._send(bytes([0x30 | flags]) + self._enc_len(len(vh) + len(payload)) + vh + payload)
    def ping(self): self._send(b"\xc0\x00")
    @staticmethod
    def _read_len(data, i):
        mult = 1; val = 0
        while True:
            d = data[i]; i += 1
            val += (d & 0x7f) * mult
            if not (d & 0x80): return val, i
            mult *= 128
    def loop(self):
        buf = b""
        try:
            self._loop_inner(buf)
        except Exception as e:
            log("LOOP END:", type(e).__name__, e)
            self.connected.clear()
    def _loop_inner(self, buf):
        while True:
            frame = self.ws.recv()
            if isinstance(frame, str): frame = frame.encode()
            buf += frame
            while len(buf) >= 2:
                try: rlen, hlen = self._read_len(buf, 1)
                except Exception: break
                if len(buf) < hlen + rlen: break
                packet, buf = buf[:hlen + rlen], buf[hlen + rlen:]
                ptype = packet[0] >> 4
                if ptype == 2:  # CONNACK
                    rc = packet[-1]
                    log("CONNACK rc=", rc)
                    if rc == 0: self.connected.set()
                elif ptype == 14:
                    log("SERVER SENT DISCONNECT")
                elif ptype == 9:
                    log("SUBACK received")
                elif ptype == 3:  # PUBLISH
                    qos = (packet[0] >> 1) & 0x3
                    i = hlen
                    tlen = struct.unpack("!H", packet[i:i+2])[0]; i += 2
                    topic = packet[i:i+tlen].decode(); i += tlen
                    if qos > 0:
                        pid = packet[i:i+2]; i += 2
                        if qos == 1: self._send(b"\x40\x02" + pid)
                    payload = packet[i:]
                    if self.on_message: self.on_message(topic, payload)

mqtt = None

def publish_presence():
    now = int(time.time() * 1000)
    obj = {"userId": AGENT_ID, "deviceHash": DEVICE_HASH, "name": AGENT_NAME, "avatarUrl": "",
           "avatar": "", "bio": "وكيل تقي الذكي — Run Agent", "lastSeenTs": now, "at": now,
           "ts": now, "platform": "Agent"}
    data = json.dumps(obj, ensure_ascii=False)
    mqtt.publish(TOPIC_MY_PRESENCE, data, qos=0, retain=True)
    mqtt.publish(TOPIC_MY_ROOM_PRESENCE, data, qos=0, retain=True)

def publish_ack(status, ids):
    if not ids: return
    obj = {"kind": "run-ack", "version": 1, "fromUserId": AGENT_ID, "fromDeviceHash": DEVICE_HASH,
           "status": status, "messageIds": list(ids), "ts": int(time.time() * 1000)}
    mqtt.publish(TOPIC_ACKS, json.dumps(obj, ensure_ascii=False), qos=0, retain=False)

def make_message(text):
    return {"id": new_id("m"), "type": "text", "text": text, "author": AGENT_NAME,
            "userId": AGENT_ID, "createdAt": now_iso(), "replyTo": None, "caption": None,
            "file": None, "reactions": {}, "deliveredTo": [], "readBy": []}

def initial_room():
    return {"kind": "run-room", "version": 1,
            "meta": {"code": ROOM, "name": "Run Agent — موس", "createdAt": now_iso(),
                     "dirId": None, "ownerId": None, "accentColor": None, "bgStyle": None,
                     "identityVersion": 0},
            "messages": [dict(make_message("سلام تقي، أنا موس — وصلت. هذي غرفة Run Agent: كلّمني من هنا مباشرة، والجسر يسمع على مدار الساعة ويوقظني. جرّب تبعث أي رسالة."),
                              id=new_id("m"))],
            "tasks": [], "avatars": {AGENT_ID: ""},
            "note": {"content": "مذكرة غرفة Run Agent المشتركة — موس وتقي.", "lastEditedBy": AGENT_NAME,
                     "lastEditedAt": now_iso(), "version": 1},
            "identity": None}

def publish_room():
    with lock: room = state["room"]
    if room is not None:
        mqtt.publish(TOPIC_ROOM, json.dumps(room, ensure_ascii=False), qos=1, retain=True)

def on_message(topic, payload):
    if topic != TOPIC_ROOM: return
    if not payload: return
    try: room = json.loads(payload.decode("utf-8"))
    except Exception: return
    if not isinstance(room, dict) or room.get("kind") != "run-room": return
    fresh_delivered = []
    with lock:
        state["room"] = room
        for m in room.get("messages", []):
            mid = m.get("id")
            if not mid or mid in seen: continue
            seen.add(mid)
            if m.get("userId") == AGENT_ID: continue
            rec = {"id": mid, "author": m.get("author", ""), "userId": m.get("userId", ""),
                   "type": m.get("type", "text"), "text": m.get("text", "") or "",
                   "createdAt": m.get("createdAt", ""), "arrivedTs": int(time.time() * 1000),
                   "file": m.get("file")}
            txt = rec["text"].strip()
            if txt.startswith("/حالة") or txt.startswith("/كتم"):
                rec["cmd"] = True
            with open(INBOX_F, "a", encoding="utf-8") as f: f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            transcript("◀", rec["author"] or rec["userId"], rec["text"] or f"[{rec['type']}]", rec["createdAt"])
            log("INBOX new message from", rec["userId"])
            if mid not in deliv_acked:
                deliv_acked.add(mid); fresh_delivered.append(mid)
    save_state()
    if fresh_delivered: publish_ack("delivered", fresh_delivered)
    # Local commands answered by the bridge itself, without waking the agent
    try:
        import re as _re
        for m in room.get("messages", []):
            if m.get("id") not in fresh_delivered: continue
            t = (m.get("text") or "").strip()
            auto = None
            if t.startswith("/كتم"):
                dur = 3600
                mm = _re.search(r"(\d+)\s*(h|m|ساعة|د)", t)
                if mm: dur = int(mm.group(1)) * (3600 if mm.group(2) in ("h", "ساعة") else 60)
                until = int(time.time()) + dur
                with open(os.path.join(BASE, "mute_until.txt"), "w") as f: f.write(str(until))
                auto = f"تم تفعيل الكتم — الجسر يسجل رسائلك ولا يوقظ موس حتى {time.strftime('%H:%M', time.localtime(until))}. كي يرجع قبل الوقت، ابعث /حالة ثم رسالة عادية بعد انتهاء الكتم."
            elif t.startswith("/حالة"):
                auto = "الجسر خدام ويسمع الآن على مدار الساعة، وحضور موس في الغرفة حقيقي. ابعث رسالة عادية وستوقظه."
            if auto:
                with lock:
                    state["room"].setdefault("messages", []).append(make_message(auto))
                publish_room(); save_state()
                transcript("▶", AGENT_NAME + " (جسر)", auto, now_iso())
    except Exception as e: log("cmd err", e)

def heartbeat_loop():
    while True:
        time.sleep(25)
        try:
            with open(os.path.join(BASE, "heartbeat.txt"), "w") as f: f.write(str(int(time.time())))
            if mqtt and mqtt.connected.is_set(): publish_presence(); mqtt.ping()
        except Exception as e: log("heartbeat err", e)

def _load_outbox_pos():
    try: return int(open(OUTBOX_SENT_F, encoding="utf-8").read().strip())
    except Exception: return 0

def _save_outbox_pos(p):
    try:
        tmp = OUTBOX_SENT_F + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f: f.write(str(p))
        os.replace(tmp, OUTBOX_SENT_F)
    except Exception as e: log("outbox.pos err", e)

def outbox_loop():
    pos = _load_outbox_pos()
    while True:
        time.sleep(2)
        try:
            if not (mqtt and mqtt.connected.is_set()): continue
            if not os.path.exists(OUTBOX_F): continue
            with open(OUTBOX_F, encoding="utf-8") as f: lines = f.readlines()
            if pos > len(lines): pos = 0
            while pos < len(lines):
                line = lines[pos].strip(); pos += 1
                if not line: continue
                try: item = json.loads(line)
                except Exception: continue
                text = (item.get("text") or "").strip()
                voice_url = (item.get("voice_url") or "").strip()
                if not text and not voice_url: continue
                # replying means I have read everything pending -> honest read acks
                pending_read = [i for i in deliv_acked if i not in read_acked]
                if pending_read:
                    read_acked.update(pending_read); publish_ack("read", pending_read); save_state()
                mqtt.publish(TOPIC_TYPING, json.dumps({"userId": AGENT_ID, "name": AGENT_NAME,
                    "typing": True, "at": int(time.time()*1000), "ts": int(time.time()*1000)},
                    ensure_ascii=False), qos=0, retain=False)
                with lock:
                    if state["room"] is None: state["room"] = initial_room()
                    if voice_url:
                        msg = make_message("")
                        msg["type"] = "voice"
                        msg["caption"] = text or None
                        msg["file"] = {"name": item.get("voice_name", "voice.mp3"),
                                       "size": int(item.get("voice_size", 0)),
                                       "mime": item.get("voice_mime", "audio/mpeg"),
                                       "url": voice_url, "dataUrl": None, "voice": True,
                                       "caption": text or None,
                                       "duration": int(item.get("voice_duration", 0))}
                        state["room"].setdefault("messages", []).append(msg)
                    else:
                        state["room"].setdefault("messages", []).append(make_message(text))
                    state["room"].setdefault("avatars", {})[AGENT_ID] = state["room"].get("avatars", {}).get(AGENT_ID, "")
                publish_room(); save_state()
                transcript("▶", AGENT_NAME, ("[صوتية] " + text) if voice_url else text, now_iso())
                log("OUTBOX sent:", ("voice " + voice_url)[:60] if voice_url else text[:40])
        except Exception as e: log("outbox err", e)

def main():
    global mqtt
    backoff = 5
    threading.Thread(target=heartbeat_loop, daemon=True).start()
    threading.Thread(target=outbox_loop, daemon=True).start()
    while True:
        for url in BROKERS:
            try:
                log("connecting", url)
                mqtt = MqttWs(url); mqtt.on_message = on_message
                mqtt.connect()
                threading.Thread(target=mqtt.loop, daemon=True).start()
                if not mqtt.connected.wait(timeout=15): raise RuntimeError("no CONNACK")
                backoff = 5
                mqtt.subscribe(TOPIC_ROOM, qos=1)
                mqtt.subscribe(TOPIC_ROOM + "/presence/+", qos=0)
                publish_presence()
                log("subscribed; room state known:", state["room"] is not None)
                time.sleep(6)
                with lock: need_init = state["room"] is None
                if need_init:
                    with lock: state["room"] = initial_room()
                    publish_room(); save_state()
                    transcript("▶", AGENT_NAME, state["room"]["messages"][0]["text"], now_iso())
                    log("room created:", ROOM)
                while mqtt.connected.is_set():
                    time.sleep(5)
                    # liveness: websocket raises in loop thread on close; check socket
                    try: mqtt.ws.sock.getpeername()
                    except Exception: break
            except Exception as e:
                log("conn err", url, e)
            try:
                if mqtt and mqtt.ws: mqtt.ws.close()
            except Exception: pass
            if mqtt: mqtt.connected.clear()
        log("reconnect in", backoff); time.sleep(backoff); backoff = min(backoff * 2, 60)

if __name__ == "__main__":
    main()
