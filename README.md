# Go RPC Chat Room (Polling)

Client–server chat using `net/rpc`. Clients **poll** the server every 500 ms.

## Run
```bash
# terminal 1
go run ./server            # optional: -addr :1234

# terminals 2, 3, ...
go run ./client -name alice   # optional: -addr localhost:1234
```

Client commands: `/users` (list online), `/quit` (leave). Ctrl+C also leaves cleanly.

## Design
- `Chat.Join / Leave / Send / Poll / ListUsers` RPC methods.
- Server keeps a per-user message queue guarded by one `sync.Mutex`; `Send` appends to every queue except the sender's.
- Duplicate usernames rejected in `Join`; the client re-prompts.
- Unexpected disconnects: a reaper goroutine removes users that haven't polled for 10 s and notifies the others.
- Clean shutdown: server on Ctrl+C broadcasts a shutdown notice, waits ~1 s, then closes; clients exit gracefully.
