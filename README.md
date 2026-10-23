# Run Agent

**Run Agent** is a service that lets an AI agent live as a permanent member inside a private [Run](https://github.com/takidz507/run) room.

The owner talks to the agent from the normal Run app — no other app needed. A small bridge program runs in the agent's own environment, listens to the room around the clock, wakes the agent when a message arrives, and posts the agent's replies back into the same room with the same protocol Run already uses.

## Status — LIVE (built 2026-10-23)

- **Bridge: built and running.** A Python daemon (hand-rolled MQTT over WebSocket) runs as a systemd service in the agent's environment, joined to a private room as `agent-mous` («موس»), with honest retained presence (25s heartbeat), instant "delivered to bridge" acks, and "read" acks that fire only when the agent actually replies.
- Owner messages wake the agent in bundled batches; a safety net re-wakes the agent for any message left unanswered over **1 minute**.
- Local bridge commands answered without waking the agent: `/حالة` (status) and `/كتم <مدة>` (mute).
- A health watcher checks the bridge every 2 minutes and wakes the agent to self-repair after host replacements.
- App side: Run Android **v3.3.0** (Agent room surfaces) is built and audited — see `radar/`. The bridge works with the already-released v3.2 app too; v3.3 is comfort, not a requirement.
- Specification (frozen): [SPEC.md](SPEC.md) · Operator's guide: [docs/OPERATION.md](docs/OPERATION.md)

## Design in one paragraph

Same MQTT brokers, same topics (`runapp/v1/room/<CODE>`), same message protocol as Run today — the bridge is just another room member with a stable identity and honest presence. The room code is the only secret (it is **not** in this repo). Highlights: bundled wake-ups, a read anchor, the room's built-in Note as shared memory, honest delivery/read receipts, an optional terminal text view for the agent, an in-room **Live Edit** fix loop, and a multi-agent design — any agent with a permanent home can join the same room.

## Known limits

Replies take seconds to minutes (agent wake latency). Public brokers are best-effort. File uploads from the bridge's network to Catbox/Litterbox currently fail, so in-room voice messages await a hosting path (this repo is the candidate). Reply-chain rough edges observed on day one are listed in `docs/OPERATION.md` for a later update.

## Related

- Web app: [takidz507/run](https://github.com/takidz507/run) — live at https://takidz507.github.io/run/
- Android app: [takidz507/run-android](https://github.com/takidz507/run-android)
