# Thiết kế: Veritas Live Demo (mô phỏng + cụm Docker) và dashboard kết quả

- **Ngày:** 2026-11-02
- **Bối cảnh:** Trình diễn trực tiếp trong buổi bảo vệ luận văn (máy chiếu nối MacBook M3 Pro, có thể không có mạng). Phát triển tiếp từ `2026-10-29-pluggable-consensus-experiments-design.md`.
- **Trạng thái:** Đã duyệt kiến trúc và UI trong chat; mockup UI: canvas "Veritas Live Demo UI" (3 màn: Live cluster, After fault, Merkle proof).

## 1. Mục tiêu

Cho hội đồng **thấy tận mắt** cơ chế của một hybrid blockchain database và các kết quả RQ1–RQ3, bằng:

1. **Live Demo** (`cmd/demo`): một web app chạy trên máy trình bày, có hai chế độ dùng chung một UI:
   - **Simulated**: N node `core.Server` chạy trong cùng tiến trình trên `consensus/memnet`, không cần Docker. Đây là chế độ an toàn, luôn chạy được.
   - **Docker cluster**: nối vào cụm thật đã dựng sẵn (một backend: kafka, raft, tendermint hoặc bft2), gửi giao dịch, sinh tải, tiêm lỗi.
2. **Dashboard kết quả** (`results/dashboard.html`): một file HTML tĩnh, offline, trình bày E1–E6 và bảng RQ3. Dùng làm dự phòng khi demo trực tiếp gặp sự cố.

### Tiêu chí thành công

Trong khoảng 5 phút, người trình bày làm được các việc sau trên **cả hai chế độ**:

1. Gửi một `Set` và thấy nó được commit trên mọi replica (cùng height, cùng root digest, cùng giá trị).
2. Chạy tải YCSB và thấy TPS/latency cập nhật theo giây cùng dải block có thứ tự.
3. Verify một key: xem kết luận hợp lệ và đường đi Merkle từ lá lên root, rồi so sánh cùng key trên mọi replica.
4. Tiêm lỗi (crash, silent, slow, corrupt): thấy banner chuyển sang DIVERGENT hoặc thấy node down, TPS giảm, và (khi gỡ lỗi được) TPS hồi phục.
5. Mọi thứ chạy **không cần Internet**, UI **hoàn toàn bằng tiếng Anh**, đọc rõ trên máy chiếu.

### Không làm (non-goals)

- Web tự dựng/tắt cụm Docker (cụm dựng trước bằng script, xem mục 5).
- Chạy nhiều cụm song song để so sánh trực tiếp.
- Khôi phục node đã crash hoặc đã corrupt **trong chế độ Docker**: không có chuyển trạng thái. Chế độ Docker chỉ gỡ được `silent` và `slow`; muốn làm lại thì dựng lại cụm.
- Xác thực/đa người dùng. Server chỉ lắng nghe trên `127.0.0.1`.
- Đổi hành vi của E1–E6. Mọi thay đổi trên đường đo đều mặc định tắt (mục 4).

## 2. Kiến trúc

```
Browser (UI tiếng Anh, offline, vanilla JS + SVG)
   │ REST  /api/*            │ SSE /api/events
┌──▼─────────────── cmd/demo (một binary) ─────────────────┐
│ demo/web      : HTML/CSS/JS/fonts nhúng bằng go:embed      │
│ demo/api      : HTTP handlers, SSE fan-out                 │
│ demo/hub      : trạng thái tổng hợp, metrics, event log,   │
│                 phán quyết consistency (đa số root)        │
│ demo/load     : bộ sinh tải YCSB-A/B/C (closed loop)       │
│ demo/verify   : dựng proof path + verdict từng replica     │
│ demo/cluster  : interface Cluster                           │
│   ├─ sim.go    : N × core.Server trên memnet + faultyNet   │
│   └─ docker.go : gRPC + observer SSE + demo_fault.sh       │
└────────────────────────────────────────────────────────────┘
         │ (docker mode)
   127.0.0.1:1990i (gRPC Veritas)   127.0.0.1:1980i (observer HTTP)
   127.0.0.1:19870 (TSO, nếu backend dùng TSO)
         │
   Cụm hbdb (experiments/cluster, PUBLISH_PORTS=1, DEMO_EVENTS=1)
```

Mỗi package có một trách nhiệm rõ ràng, giao tiếp qua interface nhỏ và test được độc lập. `demo/hub` không biết đang ở chế độ nào; nó chỉ tiêu thụ `cluster.Event`.

### 2.1 Interface `Cluster` (`demo/cluster`)

```go
type NodeInfo struct {
    Name   string // "node1".."nodeN"
    Status string // "up" | "down"  (divergent do hub suy ra từ root)
}

type BlockEvent struct {
    Node     string    // replica báo sự kiện
    Height   int64
    Proposer string    // signature của block ("node2"); "" nếu không rõ
    Txs      int       // số Set trong block
    Aborted  int
    Root     []byte    // ledger root sau khi apply
    At       time.Time
}

type KV struct { Value string; Version int64 }

type Proof struct {
    Root      []byte
    SideNodes [][]byte
    NonMembershipLeafData []byte
}

type Cluster interface {
    Info() ClusterInfo                    // mode, backend, N, f, fault kinds hỗ trợ
    Nodes() []NodeInfo
    Set(ctx context.Context, node, key, value string) error   // trả khi commit (sync)
    Get(ctx context.Context, node, key string) (KV, error)    // value + version từ store của node
    Prove(ctx context.Context, node, key string) (Proof, error)
    Events() <-chan BlockEvent            // gộp từ mọi node
    Inject(ctx context.Context, node string, f Fault) error   // crash|silent|slow|corrupt
    Heal(ctx context.Context, node string) error               // gỡ lỗi gỡ được
    Reset(ctx context.Context) error                           // sim: dựng lại cụm; docker: ErrUnsupported
    Close() error
}
```

### 2.2 Chế độ Simulated (`demo/cluster/sim.go`)

- N node (mặc định 4, cờ `--n`). Mỗi node gồm `core.Executor` với `core.NewMapStore()` và ledger SMT trong thư mục tạm (xoá khi `Close`/`Reset`), `core.Server` (`CommitSync`, `VersionOrdered`, `BlockSize` 100, `FlushInterval` 200 ms để block ra nhanh khi gõ tay), và `ServerConfig.OnApplied` đẩy `BlockEvent` (mục 4.1).
- Mỗi node dùng `faultyNet`, một `consensus.Consensus` bọc quanh `memnet` node, có bốn trạng thái:
  - **normal**: chuyển thẳng.
  - **crashed**: một goroutine **luôn rút** kênh Deliver của memnet vào hàng đợi không giới hạn (để không chặn hub, vì `NodeBuffer` = 64). Khi đang crashed thì không chuyển block cho core, `Submit` trả lỗi, và lớp demo từ chối mọi RPC tới node đó (`Unavailable`). `Heal` phát lại hàng đợi, nên node **bắt kịp** và người xem thấy height tăng vọt.
  - **silent**: `Submit` bị nuốt (trả `nil` nhưng không gửi), node vẫn nhận block. Client gửi vào node này sẽ hết thời gian chờ `SyncTimeout`, mô phỏng F2 ở mức mạng.
  - **slow**: trễ thêm 200 ms (cờ `--slow-ms`) trên cả `Submit` và Deliver.
- **corrupt**: `core.ParseFault("corrupt", …)` được cài sẵn cho mọi node với `after` rất lớn; `Inject(corrupt)` gọi `Activator.Activate()`. Lỗi này không gỡ được (trạng thái đã sai), nên chỉ `Reset` mới xoá.
- `Reset` đóng mọi node rồi dựng cụm mới; UI xoá dải block và metrics.

### 2.3 Chế độ Docker (`demo/cluster/docker.go`)

- Cấu hình bằng cờ:
  - `--backend` (để hiển thị nhãn và biết loại lỗi hỗ trợ);
  - `--n`;
  - `--grpc-base 19900` (node i → `127.0.0.1:(19900+i)`);
  - `--events-base 19800`;
  - `--tso-addr 127.0.0.1:19870` (chỉ kafka/raft dùng TSO; tendermint/bft2 dùng `ordered`).
- `Set`/`Get` dùng `veritas/driver` cho backend `client` version mode (có TSO), hoặc client gRPC `pbv.NodeClient` trực tiếp cho `ordered`, giống cách `cmd/hbdbprobe` làm.
- `Get` và `Prove` đọc qua observer HTTP của node (`GET /kv?key=`, `GET /prove?key=`, mục 4.2) để có cả `version`, vì leaf của SMT là `value + "-" + version`.
- `Events()` mở một SSE tới `GET /events` của mỗi node. Khi mất kết nối thì đánh dấu node `down` và thử lại sau mỗi 1 s.
- `Inject`/`Heal` gọi `experiments/demo_fault.sh <action> <node>` (mục 5.2), với timeout 30 s, stdout/stderr đưa vào event log.

### 2.4 `demo/hub`

- Giữ snapshot: danh sách node (status, height, root, giá trị của key đang theo dõi), 30 block gần nhất (tính theo node tham chiếu, là node `up` có số nhỏ nhất), metrics, event log (200 dòng).
- **Phán quyết consistency** mỗi 1 s, cùng quy tắc với `cmd/auditor`:
  - lấy root mới nhất của các node `up`;
  - nhóm theo root, gán chữ A, B, C theo kích thước nhóm giảm dần;
  - có nhóm chiếm đa số tuyệt đối thì mọi node khác nhóm đó là **divergent**;
  - không có đa số thì mọi node đều divergent.
  - Node `down` không bao giờ bị tính là divergent.
  - Vì các replica apply lệch nhau vài ms, chỉ so root **ở cùng height**: hub giữ `root[node][height]` (vòng 64 height gần nhất) và so tại height lớn nhất mà mọi node `up` đều đã đạt. Không có height chung thì giữ phán quyết cũ.
- **Metrics**: TPS (số thao tác đọc + ghi thành công mỗi giây, cùng định nghĩa với `ycsbbench`; gồm cả Set từ form), p50/p99 latency trong cửa sổ 10 s, số lỗi, chuỗi 60 điểm TPS cho sparkline, mốc thời gian tiêm lỗi để vẽ vạch.

### 2.5 `demo/load`

Closed loop với C client (1–256), workload YCSB A (50% đọc, 50% ghi), B (95/5) hoặc C (100% đọc), trên keyspace 1.000 key `ycsb0..ycsb999` (tiền tố riêng để tải không ghi đè khoá kịch bản `user42`), giá trị 100 byte, phân bố uniform. Client i gửi tới node `(i mod N)+1` như harness. Pha nạp ban đầu ghi 1.000 key (không đo) với 128 writer song song. Trạng thái tải báo cho hub có `phase`: `preloading` trong pha nạp (UI hiện "Preloading 1,000 keys…"), `running` khi C client bắt đầu chạy. Dừng khi hết thời lượng hoặc khi `DELETE /api/load`.

## 3. HTTP API (`demo/api`)

Tất cả JSON. Server lắng nghe `127.0.0.1:8080` (cờ `--listen`).

| Method & path | Body | Trả về |
|---|---|---|
| `GET /api/state` | — | snapshot đầy đủ: `info`, `nodes`, `consistency`, `blocks`, `metrics`, `log`, `load`, `scenario_step` |
| `GET /api/events` | — | SSE, các event: `nodes`, `consistency`, `block`, `metrics` (1 s), `log`, `load` |
| `POST /api/tx` | `{key, value, node}` | `{ok, latency_ms, height, error}`; key được đặt làm *tracked key* hiển thị trên thẻ replica |
| `POST /api/load` | `{clients, workload, duration_s}` | `202`; `409` nếu đang chạy |
| `DELETE /api/load` | — | `204` |
| `POST /api/verify` | `{key, node}` | `{valid, value, version, root, leaf_hash, steps:[{level, hash, sibling, sibling_side, empty}], replicas:[{node, value, root, verdict}]}` |
| `POST /api/fault` | `{kind, node}` | `{ok, error}`; `kind` ∈ `crash, silent, slow, corrupt` |
| `POST /api/heal` | `{node}` | `{ok, error}` |
| `POST /api/reset` | — | sim: `204`; docker: `501` |
| `POST /api/scenario` | `{step}` | đặt bước kịch bản đang làm nổi bật (1–5), không tự chạy hành động |

Lỗi trả `{error}` với mã 4xx/5xx. Tham số được validate (node phải có thật, key không rỗng và ≤ 128 byte, value ≤ 4 KB, clients 1–256, duration 5–300 s).

**Verify** (`demo/verify`, hàm thuần, test được):

1. `Get(node,key)` → `value, version`; `Prove(node,key)` → `root, side nodes`.
2. Gọi `merkletree.VerifyProofSteps(proof, root, key, []byte(value+"-"+version), sha256.New())` (hàm mới, mục 4.3), nhận `ok` và các hash trung gian.
3. Dựng `steps` từ lá lên root. `sibling_side` suy từ bit của `sha256(key)` tại level đó. Level không có side node, tức subtree rỗng, được gộp thành một dòng `empty: true` kèm số level.
4. `replicas`: làm bước 1–2 trên mọi node, hỏi song song. Khi có replica báo root khác root đa số (dưới tải, các replica lệch nhau một block trong chốc lát), hỏi lại toàn bộ, tối đa 5 lượt cách nhau ~100 ms. `verdict` nhận một trong các giá trị `valid · majority`, `valid · lagging` (proof hợp lệ, cùng value với đa số nhưng root vẫn khác sau các lượt hỏi lại: replica đang chậm, không phải gian lận), `valid · minority root` (proof hợp lệ nhưng value và root khác đa số), `invalid`, `unreachable`, `not found`.

## 4. Thay đổi trong code hiện có (mặc định tắt)

### 4.1 Hook observer trong `veritas/core`

- `ServerConfig.OnApplied func(ev AppliedEvent)` là tuỳ chọn, nil theo mặc định. `AppliedEvent{Height, Proposer string, Txs, Aborted int, Root []byte}`.
- Hook được gọi trong `applyLoop` **sau** một `Apply` thành công, trước `resolveBlock`. `Root` là bản sao `ex.RootDigest()`.
- Hook phải không chặn. `applyLoop` gọi đồng bộ nên phía cài đặt chịu trách nhiệm không chặn; tài liệu ghi rõ điều này. Khi nil thì không tốn gì thêm, nên đường đo E1–E6 không đổi.

### 4.2 Observer HTTP trong node (`veritas/observe` + `cmd/veritas`)

- Package `veritas/observe`:
  - `Broadcaster`: `Publish(ev)` không chặn. Mỗi subscriber có kênh đệm 256; đầy thì bỏ sự kiện và tăng bộ đếm `dropped`.
  - `Handler(b, lookup, prove)` phục vụ:
    - `GET /events`: SSE, mỗi event là JSON của `AppliedEvent` kèm `node` và `at`.
    - `GET /kv?key=`: `{value, version}` hoặc 404.
    - `GET /prove?key=`: `{root, side_nodes, non_membership_leaf_data}`, dạng hex.
    - `GET /healthz`.
- `cmd/veritas`: cờ `--demo-events-addr` (rỗng = tắt). Khi bật, core path đặt `OnApplied = b.Publish` và chạy HTTP server trên địa chỉ đó. `lookup` dùng `store.Get` + `veritas.Decode`, `prove` dùng `Executor.Prove`. Không áp dụng cho `kafka-orig`/`tm-orig`.

### 4.3 `merkletree.VerifyProofSteps`

Hàm export mới bọc `verifyProofWithUpdates`, trả `(ok bool, steps [][]byte)`: các hash từ leaf lên root. `VerifyProof` giữ nguyên.

### 4.4 Cụm Docker (`experiments/cluster`)

- `gen_compose.py --publish-ports`: publish `1990` của node i ra `127.0.0.1:(19900+i)`, cổng observer `7300` ra `127.0.0.1:(19800+i)`, TSO `7070` ra `127.0.0.1:19870`. Cờ này cũng thêm `--demo-events-addr=:7300` vào lệnh veritas của các backend core.
- `up.sh`: `PUBLISH_PORTS=1` truyền `--publish-ports`. Mặc định tắt, nên E1–E6 sinh ra compose giống hệt trước (có test khẳng định).

### 4.5 `go.mod`

`go 1.15` đổi thành `go 1.16` để dùng `go:embed`. Image build bằng golang:1.20 và máy dev dùng go1.24, nên kiểm tra bằng `go build ./... && go test ./veritas/... ./consensus/memnet/...`.

## 5. Kịch bản vận hành

### 5.1 `experiments/demo_up.sh`

Script bọc `up.sh`:

```bash
BACKEND=bft2 N=4 experiments/demo_up.sh
```

- Đặt `PUBLISH_PORTS=1`, `COMMIT_MODE=sync`, `BATCH_FLUSH=200ms`, `CPUS`/`MEM` như thí nghiệm.
- "Arm" sẵn lỗi FaultPolicy: `NODE_FAULTS="node3=corrupt"`, thêm `node4=silent` nếu backend là kafka.
- Chờ health (`health.sh`), rồi in lệnh chạy demo:

```bash
go run ./cmd/demo --mode=docker --backend=bft2 --n=4
```

### 5.2 `experiments/demo_fault.sh <action> <node>`

Tái dùng `lib.sh`, `faults.sh` và `net.sh`. `AT=0` (ngay lập tức).

| Action | Hiện thực | Gỡ được? |
|---|---|---|
| `crash` | `fault_crash` | Không (docker mode) |
| `silent` | kafka: `fault_signal` (FaultPolicy silent, node phải được arm); raft/tendermint/bft2: `fault_silent_net` với `hbdb_consensus_ports` | Có, trừ kafka |
| `slow` | `fault_slow` 200 ms | Có |
| `corrupt` | `fault_signal` (node phải được arm) | Không |
| `heal` | xoá luật iptables DROP do `silent` thêm (`-D` theo đúng luật đã thêm) và `tc qdisc del` / đặt lại delay nền | — |

Hành động không hỗ trợ (ví dụ corrupt trên node chưa arm) thoát mã 4 kèm thông báo; UI hiện thông báo đó và khoá nút tương ứng (`ClusterInfo.Faults`).

### 5.3 Kịch bản 5 bước trên UI

1. **Write a transaction**: điền sẵn `user42 = hello-committee`, gửi tới node2.
2. **Replicate & commit**: tự bật tải 64 client YCSB-A trong 60 s.
3. **Verify a proof**: mở màn Merkle proof với `user42`.
4. **Inject a fault**: corrupt node3, rồi gửi lại `user42` (tới node2) để giá trị trên node3 lệch thấy rõ (và crash node4 nếu bật tuỳ chọn).
5. **Detect & recover**: trỏ vào banner DIVERGENT. Sim: Heal node4 cho thấy catch-up, Reset để làm lại. Docker: nêu rằng corrupt/crash không gỡ được.

Mỗi bước là một nút. Bấm nút thì gọi đúng các API ở trên (không có đường tắt riêng), và người trình bày vẫn thao tác tay được.

## 6. UI (`demo/web`)

- Theo mockup trên canvas: header (công tắc chế độ chỉ để hiển thị, vì chế độ chọn bằng cờ lúc khởi động; công tắc mờ khi không khả dụng), thanh kịch bản, cột trái (New transaction, Load generator, Fault injection), giữa (banner, thẻ replica, dải block), phải (Throughput, Event log). Màn **Verify** là một view thứ hai trong cùng trang (`#verify`), có nút quay lại.
- Vanilla JS (ES2017, không bundler), một `app.js` chia thành các module nhỏ bằng IIFE: `api.js`, `state.js`, `render-*.js`. Sparkline và sơ đồ proof dùng SVG. Toàn bộ chữ tiếng Anh.
- Font: nhúng IBM Plex Sans/Mono dạng `woff2` (giấy phép OFL) vào `demo/web/fonts` nếu tải được lúc phát triển; nếu không thì dùng system font stack. Không phụ thuộc mạng lúc chạy.
- Màu như mockup (ink `#16181D`, nền `#F5F5F2`, accent `#1D4ED8`, ok `#15803D`, divergent `#C2410C`, down `#9AA0A6`); mọi cặp chữ/nền ≥ 4.5:1. Trạng thái luôn có nhãn chữ ("Up", "Divergent", "Down") và chữ nhóm root, không chỉ dựa vào màu.
- Kích thước tối thiểu: chữ thân 15 px, nút 44 px trở lên. Bố cục co được tới 1024 px (màn chiếu), xếp chồng ở chiều hẹp.
- Mất SSE thì hiện "Reconnecting…" và tự nối lại; khi nối lại thì gọi `/api/state` để đồng bộ.

## 7. Xử lý lỗi

- Mọi lời gọi cluster có timeout: Set 10 s, Get/Prove 3 s, fault script 30 s. Lỗi được ghi vào event log và trả về cho UI, không làm sập server.
- Node down trong docker mode: hub đánh dấu sau 2 lần nối SSE thất bại liên tiếp hoặc khi gRPC trả `Unavailable`.
- Bộ sinh tải đếm lỗi theo loại (`Aborted`, timeout, unavailable) và tiếp tục chạy.
- `cmd/demo` khởi động ở docker mode mà không nối được node nào trong 10 s thì in hướng dẫn chạy `demo_up.sh` và thoát mã 2.

## 8. Dashboard kết quả (`results/dashboard.html`)

- `experiments/plot/build_dashboard.py`:
  - đọc `results/final/e1.csv`, `e1-bf100ms.csv`, `e2.csv`, …, `e6.csv`, `results/figs/rq3_summary.csv`;
  - gom theo cấu hình (backend, n, clients, block_size, workload, rtt_ms, fault): mean, std mẫu, số rep cho `tps`, `lat_mean_ms`, `lat_p99_ms`;
  - ghi **một file HTML** với dữ liệu JSON nhúng trong `<script type="application/json">`, CSS và JS inline. Không CDN, mở bằng double-click.
- Tab:
  - **Overview**: thẻ kết luận cho RQ1–RQ3 kèm con số chính lấy từ dữ liệu, không viết tay.
  - **RQ1 HotStuff vs Tendermint**: E1 (tps/latency theo clients), E2 (theo N).
  - **RQ2 Raft vs Kafka**: E1, E2 và E1-bf100ms.
  - **Sensitivity**: E3 block size, E4 workload, E5 RTT.
  - **RQ3 Faults**: E6 cột tps theo loại lỗi ở N=4 và N=7; bảng recovery/detect/stall từ `rq3_summary.csv`.
- Có công tắc bật/tắt từng backend, tooltip mean ± std (n reps). Màu mỗi backend cố định trên mọi biểu đồ, kèm hình dạng marker khác nhau. Có theme sáng/tối.
- Biểu đồ SVG viết tay: line chart (trục log2 cho clients), grouped bar, bảng. Hỗ trợ bàn phím (tab qua điểm dữ liệu).

## 9. Kiểm thử

- `veritas/core`: test hook `OnApplied` (gọi đúng height, root khớp `RootDigest`, nil không panic), dùng `-race`.
- `veritas/ledger/merkletree`: `VerifyProofSteps` cho ra `ok` giống `VerifyProof`; bước cuối bằng root; proof bị sửa trả `false`.
- `veritas/observe`: `httptest` cho `/events` (nhận đúng event, subscriber chậm không chặn `Publish`), `/kv`, `/prove`.
- `demo/cluster` (sim):
  - Set trên node1 thì mọi node có cùng giá trị và root;
  - corrupt node3 thì root node3 khác, hub báo divergent node3;
  - crash node4, sau đó Heal thì height node4 bắt kịp và root khớp;
  - silent node2 thì Set qua node2 hết thời gian chờ, còn Set qua node khác vẫn commit.
- `demo/hub`: phán quyết consistency (đa số, hoà, node down, so tại height chung) dạng table test.
- `demo/verify`: steps và verdict trên cụm sim.
- `demo/api`: `httptest` end-to-end trên cụm sim (tx → verify → fault → state).
- `experiments/cluster/test_gen_compose.py`: không có `--publish-ports` thì output y hệt trước; có cờ thì đúng cổng `127.0.0.1` và đúng cờ `--demo-events-addr`.
- `experiments/plot/test_build_dashboard.py`: gom số trên fixture CSV, HTML chứa JSON hợp lệ.
- Smoke test Playwright (chromium có sẵn), không đưa vào CI:
  - chạy `cmd/demo --mode=sim`, bấm qua 5 bước kịch bản, xác nhận banner đổi sang DIVERGENT và không có lỗi console;
  - mở `results/dashboard.html`, chuyển qua mọi tab, không có lỗi console.
- Chế độ Docker không test tự động được trong môi trường này (không có Docker daemon). Có checklist kiểm tra tay trong `docs/demo.md`, chạy trên MacBook trước buổi bảo vệ.

## 10. Tài liệu

`docs/demo.md` (tiếng Việt):

- chuẩn bị máy (build image, `demo_up.sh`);
- chạy sim và docker;
- kịch bản 5 bước kèm lời nói gợi ý;
- checklist kiểm tra trước buổi bảo vệ;
- phương án dự phòng: sim → dashboard → hình trong `results/figs`.
