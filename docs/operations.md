# Vận hành & phát triển

Cách build, chạy thử và cấu hình các thành phần hiện có. Thiết kế ở [design.md](design.md), trạng thái triển khai ở [implementation.md](implementation.md).

## Yêu cầu

- Go 1.27 trở lên (dự án dùng `go tool` cho plugin protobuf).
- `buf`, chỉ khi sửa `.proto`. Nếu `buf` không nằm trong `PATH`: `make proto BUF=~/go/bin/buf`.

## Lệnh thường dùng

| Lệnh | Việc làm |
| --- | --- |
| `make test` | Unit test với `-race` |
| `make bench` | Benchmark hot path `Decide()` |
| `make lint` | `go vet` và `buf lint` |
| `make proto` | Sinh lại `gen/` từ `api/` |
| `make validate` | Kiểm tra mọi file trong `policies/` |
| `make build` | Build bốn binary vào `bin/` |
| `make run-local` | Chạy controller, aggregator, agent trên máy, load 30k RPS vào bucket limit 20k |

## Chạy thử cả vòng phản hồi

```sh
make run-local
```

Agent in mỗi giây một dòng `synthetic load allowed=… rejected=…`. Sau khoảng 5 giây, số allowed ổn định quanh 20.000 và rejected quanh 10.000. Thử fail-open bằng cách tắt riêng controller hoặc aggregator: khoảng 5 giây sau agent log `directive TTL expired, failing open` và rejected về 0.

## Cờ dòng lệnh

| Binary | Cờ | Mặc định | Ý nghĩa |
| --- | --- | --- | --- |
| `controller` | `-listen` | `:9091` | Địa chỉ gRPC cho aggregator |
| | `-policy` | `policies/example.yaml` | File policy |
| | `-interval` | `1s` | Chu kỳ tính ratio |
| | `-ttl` | `5s` | Hạn của chỉ thị ở tầng dưới |
| `aggregator` | `-listen` | `:9090` | Địa chỉ gRPC cho sidecar/agent |
| | `-controller` | `localhost:9091` | Regional controller |
| | `-node-id` | hostname | Định danh aggregator |
| | `-zone` | `local` | Tên zone |
| | `-interval` | `1s` | Chu kỳ gửi usage lên |
| `agent` | `-aggregator` | `localhost:9090` | Aggregator của zone |
| | `-policy` | `policies/example.yaml` | File policy (sau này nhận qua xDS) |
| | `-node-id` | hostname | Định danh host |
| | `-zone` | `local` | Tên zone |
| | `-interval` | `1s` | Chu kỳ báo cáo |
| | `-load-rps` | `0` | RPS giả lập vào bucket đầu tiên; 0 là tắt |
| `configurator` | `validate <file>...` | | Validate và in bảng bucket đã biên dịch |

## Viết policy

Policy nằm trong `policies/*.yaml`. Schema đầy đủ và ý nghĩa từng trường ở [design.md, mục Mô hình cấu hình & policy](design.md#mô-hình-cấu-hình--policy).

| Trường | Bắt buộc | Mặc định |
| --- | --- | --- |
| `name` | Có, duy nhất | |
| `match.callee` | Có | |
| `match.caller`, `match.endpoint` | Không | rỗng = mọi giá trị |
| `limit_rps` | Có, > 0 | |
| `scope` | Không | `global` |
| `priority` | Không | `medium` (`low` / `medium` / `high`) |
| `mode` | Không | `shadow` |

Validate từ chối: trường lạ, tên trùng, hai bucket cùng `match`, giá trị `scope`/`mode`/`priority` không hợp lệ, trùng bucket ID. Chạy `make validate` trước khi mở merge request.

## Tinh chỉnh vòng phản hồi

Tham số mặc định nằm ở `controller.DefaultParams` trong [`internal/controller/ratio.go`](../internal/controller/ratio.go): α = 0,5, hysteresis 5%, slew 0,2 mỗi chu kỳ, clamp 0,9. Đây là giả định khởi đầu của thiết kế, cần chỉnh bằng load test trước giai đoạn 2 của rollout. Hiện chưa có cờ dòng lệnh cho chúng.
