# Thiết kế: Đọc có kiểm chứng, truy vết node lỗi và bộ gom block thích ứng

- **Ngày:** 2026-11-09
- **Bối cảnh:** Giai đoạn tiếp theo của luận văn, xây trên baseline `docs/superpowers/specs/2026-10-29-pluggable-consensus-experiments-design.md` (E1–E6, dữ liệu cuối ở `results/final/`, commit đo `e7baa60`).
- **Trạng thái:** Được thay bằng `2026-11-09-phase3-accountability-design.md` (bản này viết khi chưa biết giai đoạn 2 trên nhánh `claude/charming-hopper-hy6x4e` đã có attestation, certifier và đọc có xác nhận).

## 1. Mục tiêu

Baseline cho thấy hai vấn đề mà luận văn chưa giải quyết:

1. **Đọc không được bảo vệ.** Ở F4 và F5-F4, đồng thuận vẫn chạy nhưng client đọc từ node lỗi nhận 2–10 nghìn giá trị sai mỗi lần chạy (`wrong_gets`). Auditor phát hiện sau 0,7–1,5 s nhưng không ngăn được. `Get` chỉ trả `value`; `Verify` trả proof nhưng root không được ai chứng nhận.
2. **Bộ hẹn giờ flush 1 s quyết định thông lượng** (tps ≈ CLIENTS / 0,5 s), che mất khác biệt giữa các backend ở E1, E2, E3, E5.

Giai đoạn này thêm:

- **A. Đọc có kiểm chứng:** client chỉ chấp nhận giá trị kèm proof SMT khớp một root được một quorum node ký.
- **E. Truy vết và cô lập:** bằng chứng mật mã chỉ ra node lỗi; cụm và client loại node đó.
- **B. Bộ gom block thích ứng:** thay bộ hẹn giờ cố định bằng cửa sổ số block đang chờ.
- **F (tùy chọn). Kafka approve theo quorum.**

### Câu hỏi nghiên cứu

- **RQ4 (mới):** Đọc có kiểm chứng tốn bao nhiêu (độ trễ, CPU, byte) trên từng backend, và nó cùng cơ chế truy vết giảm số lần đọc sai được chấp nhận xuống bao nhiêu dưới F4, F5-F4 và F6? Ngưỡng chứng chỉ f+1 hay 2f+1 đánh đổi an toàn và sẵn sàng ra sao?
- **RQ1/RQ2 (trả lời lại, nếu làm B):** Khi bỏ trần bộ hẹn giờ, so sánh Tendermint/HotStuff và Raft/Kafka thay đổi thế nào?

### Ưu tiên

**A + E là lõi, không cắt.** Khi trễ tiến độ, cắt theo thứ tự: F → E3′/E5′ → toàn bộ B.

### Tiêu chí thành công

1. Khi không có lỗi, chế độ `verified`: 0 lần đọc bị từ chối, < 0,1% lần chờ chứng chỉ hết giờ.
2. Dưới F4 và F6 với chế độ `verified`: `wrong_gets` (giá trị sai được chấp nhận) = 0.
3. Đo được chuỗi thời gian lỗi → bằng chứng → loại node → client chuyển hướng cho F4 và F6.
4. Chế độ `plain` + `timer` tái lập E1 c256 của baseline trong ±10% cho cả 4 backend (mã mới không làm đổi baseline).
5. Mỗi điểm dữ liệu 3 lần chạy, ghi commit hash và tham số, theo phương pháp baseline.

### Ràng buộc

- Phần cứng như baseline: MacBook M3 Pro 18 GB, Docker VM 11 CPU / 7,65 GiB, mỗi node `cpus 0.5`, `mem 768m`, N ≤ 7.
- Thời gian 12 tuần, một người, Go (module chính giữ golang:1.20; không cần Rust).

### Không làm

- Đổi thành viên đồng thuận (reconfiguration) khi loại node: phải sửa sâu trong từng thư viện.
- Gắn root vào block đồng thuận (QC của BFT, app hash Tendermint): mỗi backend một kiểu, mất tính công bằng.
- Gộp chữ ký BLS (N ≤ 7 dùng danh sách chữ ký ed25519 là đủ).
- Giữ snapshot cây SMT cũ (cây đã cắt tỉa; thiết kế không cần root cũ).
- Bộ điều khiển batch kiểu AIMD hay extremum-seeking.

## 2. Kiến trúc

```
Client (ycsbbench + veritas/verify)
   │ Get(read_mode, signed) / Set / ReportEvidence
┌──▼──────────────────────────────────────────────┐
│ core.Server                                      │
│  Batcher (timer | window) ──Submit──► Consensus  │
│  applyLoop ◄──Deliver── Consensus                │
│    └─ Executor.Apply → (h, root_h)               │
│         └─ attest.Node.OnApplied(h, root_h) ─────┼──Attest──► peers
│  Get(verified): Executor.ReadProved → chờ cert   │◄─Attest─── peers
│  ReportEvidence → attest.Node (evicted set)      │
└──────────────────────────────────────────────────┘
```

### Thành phần mới

| Thành phần | Nhiệm vụ | Phụ thuộc |
|---|---|---|
| `veritas/attest` | **Signer**: khóa ed25519 suy ra tất định từ seed chung + `node-id` (chỉ cho testbed, như bft2). **CertStore**: theo height, gom chữ ký theo root; đủ quorum cùng root thì có chứng chỉ; giữ K = 1024 height gần nhất; đánh thức `Get` đang chờ; sinh bằng chứng khi có chữ ký mâu thuẫn. **Gossip**: gửi attestation tới peer. **Evicted set** có epoch. | proto, `--peers` |
| `veritas/verify` | Thư viện client: kiểm chứng chỉ (≥ quorum chữ ký hợp lệ của các node khác nhau, không bị loại) và proof SMT của leaf `value-version` theo root. Đệm chứng chỉ đã kiểm theo `(h, root)`. Kiểm lại bằng chứng. | `merkletree`, khóa công khai |
| `core.Executor.ReadProved(key)` | Trong một lần giữ khóa: giá trị + version từ store, height h, root_h, proof. | executor hiện có |
| `core.Executor.Apply` | Trả thêm root sau khi áp dụng, trong cùng lần giữ khóa. | |
| `FaultPolicy.OnRead` | Hook mới cho F6. | `core/fault.go` |

### Thay đổi proto (tương thích ngược)

- `service Node` thêm `rpc Attest(Attestation) returns (AttestAck)` và `rpc ReportEvidence(Evidence) returns (EvidenceAck)`.
- `Attestation { int64 height; bytes root; int32 node_id; bytes sig }`, chữ ký trên `("veritas-attest", height, root)`.
- `Evidence { kind (divergent_root | bad_read); Attestation culprit; repeated Attestation cert; SignedRead read }`.
- `GetRequest` thêm `read_mode` (`plain` mặc định | `verified`) và `bool signed`.
- `GetResponse` thêm `int64 version, height; bytes root; repeated bytes side_nodes; bytes non_membership_leaf_data; repeated Attestation cert; int64 evicted_epoch; bytes node_sig`.
- Client cũ (`plain`) chạy như hiện nay.

### Cờ mới của `cmd/veritas` (chỉ backend core)

| Cờ | Ý nghĩa | Mặc định |
|---|---|---|
| `--peers` | địa chỉ gRPC của mọi node, theo thứ tự node-id | rỗng = tắt attestation |
| `--attest-seed` | seed suy khóa | hằng số của testbed |
| `--attest-f` | f | (N−1)/3 |
| `--attest-quorum` | `f+1` hoặc `2f+1` | `f+1` |
| `--cert-wait` | thời gian `Get(verified)` chờ chứng chỉ | 2 s |
| `--batch-mode` | `timer` hoặc `window` | `timer` |
| `--window` | W | 2 |
| `--max-delay` | lệnh chờ lâu nhất trước khi buộc flush (window) | 1 s |
| `--inflight-timeout` | giải phóng chỗ của block không được giao | 2 s |
| `--kafka-quorum` | `all` hoặc `2f+1` (F) | `all` |
| `--fault=readlie` | F6 | |

`gen_compose.py` sinh `--peers`; `run_one.sh` nhận `READ_MODE`, `ATTEST_QUORUM`, `SIGN_READS`, `BATCH_MODE`, `WINDOW`, `KAFKA_QUORUM`.

## 3. Đọc có kiểm chứng (A)

### Luồng

1. **Sau khi áp dụng block h**, `applyLoop` gọi `attest.OnApplied(h, root_h)`: ký, lưu vào CertStore của chính node, gửi tới N−1 peer (N² thông điệp ~100 B mỗi block, ngoài đồng thuận, như nhau cho 4 backend). Gửi bất đồng bộ, lỗi gửi thì bỏ (không thử lại; chứng chỉ chỉ cần quorum).
2. **`Get(verified)`**: `ReadProved(key)` → `(value, version, h, root_h, proof)`; chờ chứng chỉ của `(h, root_h)` tối đa `--cert-wait`. Node đã sang h+1 trong lúc chờ thì proof vẫn đúng với root_h. Hết giờ → `Unavailable`. Key không có → `NotFound` kèm proof không tồn tại và chứng chỉ (client kiểm như thường). Node chạy không có `--peers` (attestation tắt) → `FailedPrecondition`.
3. **Client** (`verify`): kiểm chứng chỉ (đệm theo `(h, root)`), rồi proof của leaf `value + "-" + version` theo root. Đạt → chấp nhận. Không đạt → `rejected_gets`, chuyển sang E2 (mục 4), rồi thử node kế tiếp theo vòng tròn.
4. **A3 (đối chứng)**: `ycsbbench --read-mode=quorum` đọc `plain` từ f+1 node (node của client + f node kế tiếp), chấp nhận khi mọi giá trị bằng nhau; khác nhau → `rejected_gets`, thử bộ node kế tiếp.

### Bất biến

- Giá trị, proof và root của một `Get(verified)` lấy trong một lần giữ khóa executor, nên luôn khớp nhau.
- Một chứng chỉ chỉ chứa chữ ký của các node khác nhau, không bị loại, trên cùng `(h, root)`.
- `wrong_gets` từ nay = giá trị sai **được chấp nhận** (ở `plain` vẫn là mọi giá trị sai trả về).

### Hành vi dưới lỗi

- **F4**: root của node lỗi khác các node khác nên không đủ quorum; `Get(verified)` trên node đó hết giờ và client chuyển node. Node lỗi gửi kèm chứng chỉ của root đúng với proof của nó thì client từ chối.
- **F5-F4 (f+1 node lỗi giống nhau)**: f+1 node cùng ra một root sai. Với `f+1`, có chứng chỉ cho root sai (vượt ngưỡng chịu lỗi); hai chứng chỉ mâu thuẫn ở cùng height là bằng chứng có hơn f node lỗi. Với `2f+1`, không phía nào đủ chữ ký: đọc mất sẵn sàng nhưng không sai.
- **F1/F2**: N − f ≥ 2f + 1 node còn sống, nên cả hai ngưỡng vẫn tạo được chứng chỉ.

## 4. Truy vết và cô lập (E)

### Giả định

Mọi node trung thực áp dụng cùng dãy block và thực thi tất định, nên cùng height thì cùng root. Height do `applyLoop` đếm, thống nhất giữa các node; block giao trùng (raft) vẫn được giao cho mọi node theo cùng thứ tự. Nếu test ở tuần 2 cho thấy height lệch, khóa attestation đổi từ height sang hash của block.

### E1. Bằng chứng từ attestation (F4, F5-F4)

CertStore đã có chứng chỉ `(h, R)` và nhận attestation có chữ ký hợp lệ của node X cho `(h, R′)`, R′ ≠ R → bằng chứng `divergent_root {attestation của X, chứng chỉ (h, R)}`. Node chậm (chưa ký h) không bị coi là lỗi. Hai chứng chỉ mâu thuẫn ở cùng h được ghi log là "vượt ngưỡng", không loại node nào.

### E2. Bằng chứng từ lần đọc (F6)

Khi `Get(verified)` không qua kiểm tra, client đọc lại cùng key từ cùng node với `signed=true`; node ký `("veritas-read", key, value, version, h, root, hash(proof))`. Phản hồi có chữ ký mà không qua kiểm tra → bằng chứng `bad_read`. Node từ chối ký hoặc lần này trả đúng → client chỉ coi là **nghi ngờ** (tự tránh node, không công bố). `--sign-reads=always` (client gửi `signed=true` mọi lần) đóng lỗ hổng này, đổi lấy CPU ký.

### E3. Công bố và cô lập

- Bên phát hiện (node hoặc client) gửi `ReportEvidence` tới mọi node. Node kiểm lại bằng chứng bằng `verify`, thêm node lỗi vào `evicted`, tăng `evicted_epoch`, chuyển tiếp cho peer (mỗi bằng chứng một lần).
- Node bị loại: attestation không còn được tính; `GetResponse.evicted_epoch` tăng → client lấy danh sách mới và chuyển các client gắn với node đó sang node còn lại theo vòng tròn.
- Không đổi thành viên đồng thuận.

### Mốc thời gian (ghi vào `<tag>.evidence`)

`t_fault` (từ `.faults`) → `t_first_evidence` → `t_evicted_all_honest` (mọi node trung thực đã loại) → `t_clients_rerouted`; cùng `wrong_gets` được chấp nhận sau `t_first_evidence`. So với thời gian phát hiện của auditor hiện có (vẫn chạy song song).

## 5. Lỗi F6: nói dối khi đọc

`FaultPolicy.OnRead(key, val) string`; `NoFault` trả nguyên. `--fault=readlie`: `OnApply` trung thực, `OnRead` trả `val + CorruptMarker`, kích hoạt bằng `--fault-after` hoặc SIGUSR1 như các lỗi khác. Trạng thái (Redis, SMT) đúng nên root khớp: auditor và E1 không thấy; chỉ E2 thấy.

Dự đoán: `plain` → chấp nhận giá trị sai; `verified` → từ chối, có thể không có bằng chứng; `verified` + `sign-reads=always` → có bằng chứng, node bị loại.

## 6. Bộ gom block thích ứng (B)

`--batch-mode=window`: Batcher flush khi (a) block đủ `size`; hoặc (b) số block của chính node đang trong đồng thuận < W và có lệnh chờ; hoặc (c) lệnh chờ lâu nhất quá `--max-delay`.

Một chỗ trong cửa sổ được giải phóng khi: `applyLoop` giao block của chính node (`own`); block bị Abort (`AbortBlock`); `Submit` lỗi; hoặc quá `--inflight-timeout` (block mất khi raft đổi leader).

W chọn bằng thử trên raft (N=4, c16 và c256, W ∈ {1, 2, 4}), sau đó cố định cho mọi backend. `timer` (1 s, 100 ms) là đối chứng.

## 7. Kafka approve theo quorum (F, tùy chọn)

`--kafka-quorum=2f+1`: block commit khi đủ 2f+1 phiếu approve thay vì N. Thứ tự commit vẫn tất định (phiếu được đếm khi đọc lại từ topic). Chạy lại F1, F2.

## 8. Thí nghiệm

Phương pháp như baseline: `COMMIT_MODE=sync`, khởi động 10 s, đo 60 s (E8: 180 s, lỗi ở giây 60), 3 lần chạy, quy tắc CV > 10%, cổng kiểm tra hợp lệ, `host.log`, mỗi `EXP` một bộ tham số. Thêm ghi `vm_stat` theo lần chạy (khuyến nghị của baseline).

| Mã | Biến thay đổi | Cố định | Số lần chạy |
|---|---|---|---|
| **R** hồi quy | 4 backend, `plain` + `timer` | E1 c256, N=4 | 12 |
| **E7** chi phí | workload {b, c} × read_mode {plain, verified, quorum, verified + sign-always} × N {4, 7} × 4 backend | c256, block 100, `f+1`, timer 1 s | 192 |
| **E8** lỗi | F4, F6 (f node) × {plain, verified, verified + sign-always}; F5-F4 (f+1 node) × {plain, verified f+1, verified 2f+1}; N {4, 7}; 4 backend | workload A, c256, block 100 | 216 |
| **B′** | thử W; rồi E1 (client), E2 (N), E3 (block), E5 (RTT) với `window` | như baseline | ~150 |
| **F** | Kafka F1, F2 × quorum {all, 2f+1} × N {4, 7} | như E6 | 12 |

Tổng ~580 lần chạy, ~30 giờ máy.

### Chỉ số và định dạng

- CSV thêm 4 cột cấu hình: `read_mode`, `attest_quorum`, `batch_mode`, `window` (khóa chạy tiếp của driver gồm các cột này). Driver và `plot.py` chấp nhận CSV cũ (thiếu cột = `plain`, `f+1`, `timer`, rỗng).
- `<tag>.reads.csv`: accepted, rejected, wrong_accepted, failover, đọc p50/p99, chờ chứng chỉ p50/p99, kích thước proof trung bình.
- `<tag>.evidence`: mốc thời gian mục 4.
- `net_bytes_per_block` (đã có) gồm cả byte attestation; tách thêm byte của RPC `Attest` từ bộ đếm của node.
- CPU mỗi node từ `<tag>.stats` (đã có).

### Cổng kiểm tra hợp lệ mở rộng (FAULT=none)

`verified`: `rejected_gets` = 0 và chờ chứng chỉ hết giờ < 0,1% số đọc; ngược lại rc=3 như cổng hiện có.

## 9. Kiểm thử

- **Unit (`-race`)**: CertStore (ngưỡng f+1/2f+1, chữ ký trùng node, root mâu thuẫn → bằng chứng, xóa height cũ, chờ hết giờ, node bị loại); `verify` (chữ ký sai, proof sai, proof không tồn tại, bằng chứng giả bị từ chối); `ReadProved` song song với `Apply`; Batcher window (4 đường giải phóng, `max-delay`, đóng khi đang chờ); `OnRead`; Kafka quorum (nếu làm F).
- **Tích hợp (`memnet`, 4 node trong một tiến trình)**: F4 → bằng chứng và loại node; F6 → từ chối, và có bằng chứng khi sign-always; F5-F4 với f+1 (chứng chỉ sai, log vượt ngưỡng) và 2f+1 (hết giờ, không đọc sai).
- **Raft**: kiểm tra height thống nhất khi đổi leader và giao trùng.
- **Hồi quy**: thí nghiệm R (mục 8).

## 10. Lộ trình (12 tuần)

| Tuần | Việc | Điều kiện xong |
|---|---|---|
| 1 | proto, `attest`, `verify` | unit test qua |
| 2 | `ReadProved`, `Get(verified)`, gossip, cờ, `gen_compose`, test `memnet`, test height raft | 4 backend trả `Get(verified)` qua kiểm chứng |
| 3 | `ycsbbench` read modes, chuyển node, file phụ, cổng mở rộng | thí nghiệm R trong ±10% |
| 4 | Bằng chứng, `ReportEvidence`, loại node, chuyển hướng client, F6, `attest-quorum` | test tích hợp F4/F6/F5-F4 qua |
| 5 | Mốc thời gian trong harness, chạy thử E7/E8 | smoke mỗi backend chạy được |
| 6 | Chạy E7, E8 | **Mốc lõi:** RQ4 đủ số liệu |
| 7–8 | B (window) + test + thử W | W được chọn |
| 9 | Chạy B′ | |
| 10 | F, hoặc chạy lại điểm CV > 10% | |
| 11–12 | Phân tích, hình, viết chương | bản nháp chương |

Mỗi mốc kết thúc bằng tag: `p5-attest` (tuần 2), `p6-evidence` (tuần 4), `p7-window` (tuần 8).

## 11. Rủi ro

| Rủi ro | Phát hiện ở | Dự phòng |
|---|---|---|
| `ReadProved` giữ khóa độc quyền, đọc tuần tự hóa với `Apply` | E7 workload C | Mỗi goroutine một hasher, đổi sang `RLock`; báo cả số trước và sau |
| CPU client kiểm chữ ký | E7 | Đệm chứng chỉ theo `(h, root)` (đã có trong thiết kế) |
| Height lệch giữa node (raft đổi leader, giao trùng) | Tuần 2 | Khóa attestation theo hash block |
| Chờ chứng chỉ lâu khi block đến dày | E7 | Báo p99 chờ chứng chỉ; tăng `--cert-wait` chỉ nếu hết giờ > 0,1% |
| Trễ tiến độ | Cuối mỗi mốc | Cắt F → E3′/E5′ → B |

## 12. Giới hạn cần nêu

- Khóa suy ra từ seed chung: chỉ cho testbed, không phải quản lý khóa thật.
- Loại node chỉ ở tầng đọc và attestation; node lỗi vẫn tham gia đồng thuận.
- F6 không có bằng chứng nếu node chỉ nói dối khi không bị yêu cầu ký (trừ `sign-always`).
- Ngưỡng `f+1` không an toàn khi có hơn f node lỗi thông đồng; `2f+1` an toàn nhưng mất sẵn sàng đọc.
- Mọi giới hạn của baseline vẫn áp dụng (một laptop, N ≤ 7, `tc netem`, version TSO và `(height, index)` khác nhau giữa CFT và BFT).
