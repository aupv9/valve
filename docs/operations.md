# Vận hành & phát triển

Cách build, chạy thử, triển khai và vận hành các thành phần. Thiết kế ở [design.md](design.md), trạng thái triển khai ở [implementation.md](implementation.md).

## Yêu cầu

- Go 1.27 trở lên (dự án dùng `go tool` cho plugin protobuf).
- Docker với Compose v2, cho demo và image.
- `buf`, chỉ khi sửa `.proto`. Nếu `buf` không nằm trong `PATH`: `make proto BUF=~/go/bin/buf`.

## Lệnh thường dùng

| Lệnh | Việc làm |
| --- | --- |
| `make test` | Unit test với `-race` |
| `make bench` | Benchmark hot path `Decide()` |
| `make lint` | `go vet`, `golangci-lint` và `buf lint` |
| `make proto` | Sinh lại `gen/` từ `api/` |
| `make validate` | Kiểm tra mọi file trong `policies/` |
| `make build` | Build mọi binary vào `bin/` |
| `make run-local` | Chạy controller, aggregator, agent in-process trên máy, load 30k RPS vào limit 20k |
| `make demo-up` / `make demo-down` | Demo với Envoy thật trong Docker |
| `make docker` | Build image `valve:dev` chứa mọi binary |
| `make certs` | Sinh CA và cert mTLS dùng thử vào `certs/` |

## Demo với Envoy thật

```sh
make demo-up
docker compose -f deploy/demo/compose.yaml logs -f load-web load-mobile
```

Demo chạy configurator, controller, aggregator, một Envoy sidecar kèm agent trước service `checkout`, và hai bộ sinh tải:

- `web-gateway` gửi 300 RPS `CreateOrder` vào bucket enforce 200 RPS: khoảng một phần ba nhận 429.
- `mobile` gửi 150 RPS vào bucket shadow 100 RPS: không request nào bị từ chối, số "lẽ ra bị drop" nằm trong metric.

| Cổng trên máy | Dịch vụ |
| --- | --- |
| 28080 | Configurator: kill switch, `/metrics` |
| 29104 | Controller `/metrics` |
| 29102 | Agent `/metrics` |
| 29901 | Envoy admin |
| 20000 | Service `checkout` qua sidecar |

Thử kill switch: `curl -X PUT localhost:28080/kill-switch -d '{"enabled":true}'`. Thử đổi limit: sửa `deploy/demo/policies/demo.yaml`; configurator đọc lại sau 2 giây.

## Các binary

| Binary | Chạy ở đâu | Việc làm |
| --- | --- | --- |
| `configurator serve` | Vài replica, cạnh kho policy (Git checkout) | Phục vụ policy và kill switch qua xDS |
| `controller -tier global` | Một nhóm cho toàn cầu, leader election | Tính bucket scope `global` |
| `controller` (regional) | Mỗi region, leader election, có thể chia shard | Tính bucket zonal và regional, chuyển tiếp global |
| `aggregator` | Nhiều replica mỗi zone | Cộng usage, chuyển chỉ thị xuống |
| `agent -mode envoy` | Cạnh Envoy sidecar trong mỗi pod | Đẩy ratio vào Envoy, đọc counter của Envoy |
| `agent -mode inprocess` | Thử nghiệm, phát triển | Filter trong agent, có tải giả lập |
| `configurator validate` | CI | Validate policy |
| `configurator envoy` | Pipeline cấu hình mesh | Sinh fault filter và virtual cluster cho Envoy |
| `loadgen` | Demo, thử nghiệm | Gửi HTTP với tốc độ cố định, in mã trạng thái mỗi giây |

## Cờ dòng lệnh

Cờ chung của mọi binary chạy lâu:

| Cờ | Ý nghĩa |
| --- | --- |
| `-admin` | Địa chỉ HTTP cho `/metrics`, `/healthz`, `/readyz`; rỗng là tắt |
| `-tls-cert`, `-tls-key`, `-tls-ca` | Bật mTLS; phải đủ cả ba |
| `-tls-server-name` | Tên kiểm tra trong cert của server, khi khác host đang gọi |
| `-xds` | Địa chỉ configurator; khi có, policy và kill switch lấy từ đây thay cho `-policy` |
| `-interval` | Chu kỳ báo cáo/tính, mặc định `1s` |

| Binary | Cờ | Mặc định | Ý nghĩa |
| --- | --- | --- | --- |
| `controller` | `-listen` | `:9091` | gRPC cho tầng dưới |
| | `-tier` | `regional` | `regional` hoặc `global` |
| | `-region` | `local` | Tên region, gửi lên tầng global |
| | `-global` | | Địa chỉ tầng global, phân cách bằng dấu phẩy, mỗi shard một địa chỉ |
| | `-shard`, `-shards` | `0`, `1` | Chia bucket theo `id % shards` |
| | `-leader-elect` | `false` | Bầu leader bằng Kubernetes Lease |
| | `-lease-namespace`, `-lease-name`, `-identity` | `$POD_NAMESPACE`, `valve-controller-<tier>-<shard>`, `$POD_NAME` | Cấu hình Lease |
| | `-ttl` | `5s` | Hạn của chỉ thị ở tầng dưới |
| | `-admin` | `:9104` | |
| `aggregator` | `-listen` | `:9090` | gRPC cho agent |
| | `-controller` | `localhost:9091` | Controller regional, mỗi shard một địa chỉ |
| | `-zone`, `-node-id` | `local`, hostname | |
| | `-admin` | `:9103` | |
| `agent` | `-mode` | `inprocess` | `envoy` hoặc `inprocess` |
| | `-aggregator` | `localhost:9090` | |
| | `-service` | | Callee mà sidecar đứng trước (bắt buộc với `-mode envoy`) |
| | `-envoy-admin` | `http://127.0.0.1:9901` | |
| | `-rtds-listen` | `127.0.0.1:18001` | Địa chỉ RTDS cho Envoy |
| | `-zone`, `-node-id` | `local`, hostname | |
| | `-load-rps`, `-load-request` | | Tải giả lập (`-mode inprocess`), `caller/callee/endpoint[/priority]` |
| | `-admin` | `:9102` | |
| `configurator serve` | `-listen` | `:18000` | xDS (ADS) |
| | `-admin` | `:8080` | Kill switch và metric |
| | `-admin-token` | `$VALVE_ADMIN_TOKEN` | Bearer token để đổi kill switch; rỗng là không xác thực |
| | `-policy`, `-poll` | `policies`, `5s` | Thư mục policy và chu kỳ đọc lại |
| | `-state` | | File lưu kill switch qua restart |
| `configurator envoy` | `-service` | | Callee cần sinh cấu hình (bắt buộc) |
| | `-caller-header` | `x-valve-caller` | Header chứa tên caller |
| | `-caller-spiffe` | `false` | Nhận caller theo service account SPIFFE trong `x-forwarded-client-cert` |
| | `-retry-after` | `1` | Giá trị `Retry-After` (giây) trên 429 |

## Viết policy

Policy nằm trong một thư mục `*.yaml`, được validate như một tập: tên và `match` phải duy nhất trên mọi file. Schema và ý nghĩa từng trường ở [design.md](design.md#mô-hình-cấu-hình--policy).

| Trường | Bắt buộc | Mặc định |
| --- | --- | --- |
| `name` | Có, duy nhất, chỉ `[A-Za-z0-9_-]` | |
| `match.callee` | Có | |
| `match.caller`, `match.endpoint` | Không | Rỗng là mọi giá trị |
| `limit_rps` | Có, > 0 | |
| `scope` | Không | `global` |
| `priority` | Không | `medium` (`low` / `medium` / `high`); tier mặc định của traffic thuộc bucket |
| `mode` | Không | `shadow` |

`endpoint` bắt đầu bằng `/` khớp tiền tố path HTTP; không bắt đầu bằng `/` thì khớp tên method gRPC ở cuối path.

## Tích hợp Envoy

1. Sinh cấu hình cho service: `configurator envoy -policy policies -service checkout`.
2. Thêm `http_filters` sinh ra vào trước `envoy.filters.http.router`, `virtual_clusters` vào virtual host của service, và `local_reply_config` vào HTTP connection manager (nó thêm `Retry-After` cho 429 của Valve). Trong Istio, áp bằng `EnvoyFilter`.
3. Cho Envoy nạp runtime layer `valve` qua RTDS từ agent, và đặt `node.id`/`node.cluster`. Mẫu đầy đủ: [`deploy/demo/checkout-envoy.yaml`](../deploy/demo/checkout-envoy.yaml).
4. Chạy agent cạnh Envoy: `agent -mode envoy -service checkout -aggregator <aggregator> -xds <configurator>`.

Khi thêm, xoá hay đổi `match` của bucket, chạy lại bước 1–2. Đổi `limit_rps`, `mode`, `scope` không cần đổi cấu hình Envoy.

## Kill switch

```sh
curl localhost:8080/kill-switch
curl -X PUT -H "Authorization: Bearer $VALVE_ADMIN_TOKEN" localhost:8080/kill-switch \
  -d '{"enabled":true,"reason":"incident 1234"}'
```

Khi bật, mọi bucket chạy như shadow: vẫn tính ratio và ghi metric nhưng không từ chối request. Kill switch đi trên kiểu tài nguyên xDS riêng tới thẳng agent, và còn đi kèm mọi chỉ thị của controller. Đặt `-state` để giữ trạng thái qua restart. Cổng admin của configurator phải được bảo vệ (network policy, `-admin-token`).

## Triển khai trên Kubernetes

- Controller: chạy 2 replica cho mỗi (tầng, shard) với `-leader-elect`, đặt `POD_NAME` và `POD_NAMESPACE` qua downward API, readiness probe vào `/readyz`. Chỉ leader ready, nên Service chỉ trỏ vào leader.
- Phía dưới gọi controller qua headless Service: `dns:///valve-controller-0.valve.svc:9091`. Client cân bằng round robin và thử địa chỉ khác khi gặp standby.
- Service account của controller cần quyền `get`, `create`, `update` trên `leases.coordination.k8s.io` trong namespace của Lease.
- Chia shard: chạy N nhóm controller với `-shard i -shards N`. Aggregator nhận N địa chỉ qua `-controller a,b,c`, theo đúng thứ tự shard.

## mTLS

`make certs` sinh CA và cert dùng thử. Mỗi tiến trình dùng một cert cho cả vai server và client; peer phải có cert cùng CA. Cert và key được đọc lại khi file thay đổi (kiểm tra mỗi 10 giây), nên xoay vòng bằng cert-manager hay SPIRE không cần restart. Kênh RTDS giữa agent và Envoy là plaintext trên loopback của pod.

## Metric

Mọi binary phục vụ `/metrics` trên `-admin`. Theo quy ước Prometheus, các "RPS" trong thiết kế được xuất dưới dạng counter; dùng `rate()` khi truy vấn.

| Metric | Thành phần | Nhãn | Ý nghĩa |
| --- | --- | --- | --- |
| `valve_requests_offered_total` | agent | bucket, caller, callee, priority | Offered load, gồm cả request bị drop |
| `valve_requests_rejected_total` | agent | như trên | Request bị trả 429 |
| `valve_requests_shadow_would_drop_total` | agent | như trên | Lẽ ra bị drop (shadow hoặc kill switch) |
| `valve_drop_ratio` | agent | như trên | Ratio đang áp dụng |
| `valve_control_loop_lag_seconds` | agent | | Tuổi của chỉ thị mới nhất; cảnh báo khi vượt TTL |
| `valve_fail_open_total` | agent | | Số lần bucket fail-open do chỉ thị hết hạn |
| `valve_config_info` | agent, controller, configurator | version | Phiên bản policy; so sánh giữa các host |
| `valve_kill_switch` | agent, controller, configurator | | 1 khi kill switch đang bật |
| `valve_upstream_connected` | agent | | Stream tới aggregator |
| `valve_controller_offered_rps` | controller | bucket, zone | Offered RPS đã làm mịn trên scope |
| `valve_controller_drop_ratio` | controller | bucket, zone | Ratio gửi xuống |
| `valve_controller_tier_drop_ratio` | controller | bucket, zone, priority | Ratio từng tier |
| `valve_controller_is_leader`, `valve_controller_peers` | controller | | |
| `valve_controller_tick_seconds` | controller | | Thời gian một chu kỳ |
| `valve_controller_global_connected` | controller | target | Stream tới tầng global |
| `valve_aggregator_peers`, `valve_aggregator_upstream_connected` | aggregator | target | |
| `valve_configurator_published_total`, `valve_configurator_reload_errors_total` | configurator | | Phiên bản đã phát, thay đổi bị từ chối |

Cảnh báo nên có: `valve_control_loop_lag_seconds > 5`, `increase(valve_fail_open_total[5m]) > 0`, `count(count by (version) (valve_config_info)) > 1` kéo dài, `valve_kill_switch == 1`, `increase(valve_configurator_reload_errors_total[10m]) > 0`.

## Tinh chỉnh vòng phản hồi

Tham số mặc định nằm ở `controller.DefaultParams` trong [`internal/controller/ratio.go`](../internal/controller/ratio.go): α = 0,5, hysteresis 5%, slew 0,2 mỗi chu kỳ, clamp 0,9. Đây là giả định khởi đầu của thiết kế, cần chỉnh bằng load test trước giai đoạn 2 của rollout. Hiện chưa có cờ dòng lệnh cho chúng.
