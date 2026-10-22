# ratelimit

Centralized rate limiter in the style of Uber GRL. Design doc:
[Thiết kế Centralized Rate Limiter (kiểu Uber GRL)](https://claude.ai/artifact/BjMti3PajUPSq3976BDMEW).

Docs (tiếng Việt):

- [docs/design.md](docs/design.md): thiết kế, chuyển từ tài liệu gốc
- [docs/implementation.md](docs/implementation.md): thiết kế ánh xạ vào code, trạng thái, kết quả đo
- [docs/operations.md](docs/operations.md): build, chạy thử, cờ dòng lệnh, viết policy

Sidecars decide locally by comparing a random number with a per-bucket drop
ratio. The control plane computes those ratios once per second from global
offered load and is never on the request path. Losing the control plane means
fail-open.

```
sidecar/agent ──usage──▶ aggregator (per zone) ──usage──▶ controller (regional / global)
              ◀─ drop ratio ─                  ◀─ drop ratio ─
```

## Layout

| Path | What |
| --- | --- |
| `api/ratelimit/v1` | `RateLimitControl.Sync` protobuf, the same at every tier |
| `gen/` | generated Go code (`make proto`), committed |
| `internal/policy` | YAML policy schema, validation, compiled match table |
| `internal/filter` | hot path: match, count offered load, drop by ratio, TTL fail-open |
| `internal/counter` | sharded lock-free counter |
| `internal/controller` | EWMA + hysteresis + slew limit + clamp, priority-tier allocation, control loop |
| `internal/hub` | downstream stream server shared by aggregator and controller |
| `internal/upstream` | upstream stream client with reconnect, shared by agent and aggregator |
| `cmd/agent` | per-host agent (phase 1); currently drives the in-process filter with synthetic load |
| `cmd/aggregator` | per-zone, stateless |
| `cmd/controller` | per-region drop-ratio computation |
| `cmd/configurator` | `validate` policies (CI) |
| `policies/` | bucket policies |

## Quick start

Requirements: Go 1.24+ (uses `go tool`), `buf` only for regenerating protobuf.

```sh
make test        # unit tests with -race
make bench       # Decide() hot path benchmark
make validate    # check policies/*.yaml
make run-local   # controller + aggregator + agent at 30k RPS against a 20k limit
```

`run-local` should settle at about 20,000 allowed and 10,000 rejected per second
within about 5 seconds. Kill the controller or the aggregator, and the agent
fails open after the 5 s directive TTL.

## Not built yet

- Envoy integration (phase 1: read Envoy stats, push ratios via RTDS to the fault filter)
- xDS config delivery and the global kill-switch channel
- Regional → global hop for `scope: global`, leader election, bucket sharding
- Priority tiers wired into the filter (`AllocateByPriority` exists, unused)
- Configurator traffic simulation, metrics (OpenTelemetry / Prometheus), mTLS between tiers
