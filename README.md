# 夜话 / treehole

An anonymous "tree hole" chat backend that pairs two random strangers for an ephemeral conversation.

## What it does

- Two users connect and are randomly matched together
- They can chat anonymously — no accounts, no history
- The server **never stores messages**: each message is relayed once between the two paired connections and dropped
- Either user can `/skip` to leave and be re-queued for a new match
- No database, no Redis — all pairing state lives in memory

## Architecture

The whole backend is a single file ([main.go](main.go), ~590 lines) with one dependency: `github.com/gorilla/websocket`.

The design uses an **actor model** to avoid locks on hot paths:

- **Per connection**: three goroutines (`readPump`, `writePump`, `loop`) + two channels (`in`, `out`) + a priority control channel (`ctrl`). `loop` is the sole owner of connection state — no locks needed.
- **One global matchmaker goroutine**: the single owner of all pairing topology (`waiting`, `partners` map, stats). All connections talk to it only via the `matchbox` channel. Lock-free by construction.

### Wire protocol (prefix-based, not JSON)

| Direction | Message | Meaning |
|-----------|---------|---------|
| Client → server | plain text | chat message |
| Client → server | `/skip` (exact) | leave current partner and re-queue |
| Server → client | `S:matched` | you've been paired |
| Server → client | `S:partner_left` | your partner disconnected |
| Server → client | `S:rate_limited` | too many requests |
| Server → client | `M:<text>` | message from your partner |

## Running locally

```bash
go run .        # listens on :8888
```

Open `http://localhost:8888` in two browser tabs and click "开始 / 换一个" in each — they pair up.

The WebSocket endpoint requires `?secret=Mingda2026` (the fallback page appends this automatically).

## Other commands

```bash
go build .          # build ./treehole binary
go vet ./...        # static analysis
go run -race .      # run under the race detector
go mod tidy         # sync dependencies
```

## Rate limiting

Two per-IP token buckets guard match/skip requests and chat messages. Over-limit sends `S:rate_limited` and does not disconnect. Idle IP buckets are evicted after 5 minutes.

## Deployment

Multi-stage [Dockerfile](Dockerfile) (Go 1.23-alpine → alpine:3.20 runtime, static binary) and [fly.toml](fly.toml) targeting Fly.io (`app: yehua-treehole`, region `nrt`).

**In-memory pairing does not survive horizontal scaling** — keep it single-machine.

## Security notes

- The WebSocket handshake is gated on a shared static secret (`secret=Mingda2026`) — a public-scan deterrent only, not real auth.
- `checkOrigin` allows same-host, `*.fly.dev`, localhost, and entries from `ALLOWED_ORIGIN` env var (comma-separated).
- IP-based rate limiting is easily bypassed via NAT/IP rotation. Abuse governance (reporting, ban lists, device fingerprinting) is an open problem.
