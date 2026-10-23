# Run Agent

**Run Agent** is a service that lets an AI agent live as a permanent member inside a private [Run](https://github.com/takidz507/run) room.

The owner talks to the agent from the normal Run app — no other app needed. A small bridge program runs in the agent's own environment, listens to the room around the clock, wakes the agent when a message arrives, and posts the agent's replies back into the same room with the same protocol Run already uses.

## Status

- **Specification: complete and frozen** — see [SPEC.md](SPEC.md).
- App side (Run Android v3.3.0): briefed — see [radar/RADAR-PROMPT-RUN-AGENT-V3.3.md](radar/RADAR-PROMPT-RUN-AGENT-V3.3.md).
- Bridge: not built yet. It gets built on the owner's explicit word.

## Design in one paragraph

Same MQTT brokers, same topics (`runapp/v1/room/<CODE>`), same message protocol and encryption layer as Run today — the bridge is just another room member with a stable identity and honest presence. The room code is the only secret. Highlights: bundled wake-ups, a read anchor so the agent resumes exactly where it stopped, the room's built-in Note as shared memory visible to both sides, honest delivery/read receipts, an optional terminal text view for the agent, an in-room **Live Edit** loop for fixing the app, and a multi-agent design — any agent with a permanent home can join the same room.

## Related

- Web app: [takidz507/run](https://github.com/takidz507/run) — live at https://takidz507.github.io/run/
- Android app: [takidz507/run-android](https://github.com/takidz507/run-android)
