# Thiết kế giai đoạn 3: truy vết node lỗi, đọc sai không đổi trạng thái và ngưỡng xác nhận

- **Ngày:** 2026-11-09
- **Bối cảnh:** xây trên giai đoạn 2 (`docs/superpowers/specs/2026-11-03-phase2-fair-comparison-and-verifiability-design.md`, nhánh `claude/charming-hopper-hy6x4e`, commit `0cc4828`). Thay cho bản `2026-11-09-verifiable-reads-accountability-design.md` (viết khi chưa biết đã có giai đoạn 2).
- **Trạng thái:** chờ duyệt.

## 1. Mục tiêu

Giai đoạn 2 đã có (mã và unit test, **chưa chạy trên cụm**): chữ ký Ed25519 của client, `GetVerified` kèm proof, attestation root đi qua `Consensus.Submit`, `Certifier` (quorum mặc định 2f+1, cờ `--attest-quorum`), node tự chuyển `diverged` (trả `codes.DataLoss`) và đọc có xác nhận (`--read-mode=certified`). Khi đó F4 bị phát hiện và client không chấp nhận giá trị sai.

Còn thiếu ba điều mà giai đoạn 3 bổ sung:

1. **Truy vết:** giai đoạn 2 chỉ để node tự biết mình lệch. Không có bằng chứng chỉ ra node lỗi cho phần còn lại của cụm, không loại node, và client vẫn gắn cứng vào node lỗi (chỉ nhận lỗi).
2. **Lỗi chỉ ở đường đọc (F6):** node giữ trạng thái đúng nhưng trả `Get` sai. Root vẫn khớp nên auditor và `Certifier` không thấy; đọc có xác nhận từ chối giá trị sai nhưng không ai biết node nào lỗi.
3. **Ngưỡng xác nhận:** giai đoạn 2 cố định 2f+1. Với f+1 node lỗi thông đồng (F5-F4), quorum f+1 có thể xác nhận một root sai, còn 2f+1 thì đọc có xác nhận dừng. Đánh đổi này chưa được đo.

### Câu hỏi nghiên cứu

- **RQ6:** Truy vết trong giao thức (bằng chứng được sắp thứ tự qua đồng thuận) chỉ ra và loại node lỗi nhanh tới đâu dưới F4 và F6, và client mất bao nhiêu lần đọc trước khi được chuyển sang node trung thực?
- **RQ7:** Với f+1 node lỗi thông đồng, quorum f+1 và 2f+1 đánh đổi an toàn (đọc sai được chấp nhận, node trung thực bị loại oan) và sẵn sàng (đọc có xác nhận hết giờ) ra sao? Đọc từ quorum node (không proof) so với đọc có xác nhận về chi phí và mức đảm bảo ra sao?

### Ưu tiên và thứ tự cắt

1. **Chạy giai đoạn 2 trên cụm** (điều kiện tiên quyết: mọi số liệu của giai đoạn 3 so với E9 của giai đoạn 2).
2. **Truy vết + F6 + chuyển node phía client** (lõi, không cắt).
3. Ngưỡng f+1/2f+1 và đọc từ quorum.
4. Kafka approve theo quorum (tùy chọn, cắt đầu tiên).

### Bỏ so với bản thiết kế trước

- **Kênh attestation riêng:** giai đoạn 2 đã gửi attestation qua đồng thuận, cũng như nhau cho mọi backend.
- **Bộ gom theo cửa sổ (B):** tải vòng mở của giai đoạn 2 (`--rate`) đã tách thông lượng khỏi bộ hẹn giờ; NOTES (đợt cloud) cho thấy vấn đề còn lại là block không đầy ở vòng kín, mà vòng mở giải quyết. Ghi là hướng mở rộng.

### Tiêu chí thành công

1. Giai đoạn 2 chạy được trên máy này: `check_consistency.sh` đạt cho 4 backend; có E7 (thông lượng bão hòa) cho raft và bft2 và E9.
2. F4 và F6 (f node), `certified` + chuyển node: `wrong_gets` = 0; mọi node trung thực loại node lỗi tại **cùng một height**; client của node lỗi được chuyển sang node khác.
3. F5-F4: đo được, với mỗi quorum, số đọc sai được chấp nhận, số node trung thực bị loại oan và tỉ lệ đọc có xác nhận hết giờ.
4. Không lỗi: bật truy vết không làm thông lượng ghi giảm quá 5% so với E9 cùng cấu hình (attestation bật, `certified`).
5. 3 lần chạy mỗi điểm, quy tắc CV như giai đoạn 1–2.

### Ràng buộc

- Máy: MacBook M3 Pro 18 GB (Docker VM 11 CPU / 7,65 GiB), giống giai đoạn 1. Giai đoạn 2 đặt `MEM=1g`: N=4 dùng ~4,5 GiB; **N=7 cần nâng Docker VM lên ≥ 10 GiB**, nếu không thì mọi thí nghiệm N=7 bị bỏ và F5-F4 chỉ đo ở N=4 (ghi trong giới hạn).
- 12 tuần, một người, Go (module chính golang:1.20, `bft2node/` golang:1.23).

### Không làm

- Đổi thành viên đồng thuận khi loại node (node bị loại vẫn bỏ phiếu và đề xuất block).
- Chữ ký gộp, quản lý khóa thật (khóa vẫn suy từ seed như giai đoạn 2).
- Cây Merkle nhiều phiên bản: bằng chứng đọc sai chỉ kiểm được khi height của nó còn trong cửa sổ 1024 height của `Certifier`.

## 2. Kiến trúc

Mọi thay đổi nằm trong core, client và harness; adapter đồng thuận không đổi.

```
Client (ycsbbench)
  GetVerified(certified, signed) ──► node i ──── lỗi proof/cert/DataLoss/evicted ──► chuyển node
  ReportEvidence(bằng chứng đọc) ──► node j ──► pending evidence ──► Block.evidence ──► Consensus
                                                                                         │
mọi node: applyLoop ── Certifier (attestation + bằng chứng, theo thứ tự block) ──► evicted set
```

Nguyên tắc: **mọi quyết định loại node đều tất định**, được tính trong vòng apply từ dữ liệu đã sắp thứ tự (attestation, bằng chứng trong block). Vì vậy mọi node trung thực loại cùng node tại cùng height mà không cần thêm giao thức trao đổi.

### Thành phần và thay đổi

| Thành phần | Thay đổi |
|---|---|
| `veritas/core/certifier.go` | Ghi **attestation mâu thuẫn**: chữ ký hợp lệ của node X cho `(h, R′)` khi h đã được xác nhận với R ≠ R′ (kể cả các attestation đến trước lúc xác nhận) và báo cho `Accountant`. |
| `veritas/core/accountability.go` (mới) | `Accountant`: tập `evicted` (theo thứ tự loại, kèm height và lý do), `epoch`; nhận sự kiện từ `Certifier` và từ bằng chứng trong block; kiểm bằng chứng đọc. Hàng đợi bằng chứng chờ gửi (giống `attestor.pending`). |
| `veritas/core/attest.go` | Bỏ qua attestation của node đã bị loại; khi certifier báo mâu thuẫn thì chuyển cho `Accountant`. |
| `veritas/core/server.go` | `GetVerified` hỗ trợ `signed`; `ReportEvidence`; `Evicted`; bằng chứng chờ gửi đi kèm block kế tiếp như attestation; node nằm trong `evicted` trả `codes.PermissionDenied` cho mọi đọc. |
| `veritas/core/fault.go` | Hook `OnRead(key, val string) string`; `--fault=readlie` (F6). |
| `veritas/sigs` | `SignRead`/`VerifyRead` trên bytes chuẩn hóa của phản hồi đọc. |
| `veritas/benchmark/ycsbbench` | Chuyển node (`--failover`), đọc có chữ ký (`--sign-reads=off|on-failure|always`), báo cáo bằng chứng, `--read-mode=quorum`. |
| `consensus/kafka` (tùy chọn) | `--kafka-quorum=all|2f+1`. |
| `experiments/exp/e10_*.sh`, `e11_*.sh`, `rq6.py` | Driver và phân tích (mục 6). |

### Proto (tương thích ngược)

- `Block` thêm `repeated Evidence evidence = 5;` (cùng cơ chế với `attestations`).
- `message Evidence { int32 culprit = 1; int64 height = 2; SignedRead read = 3; }`: chỉ dùng cho bằng chứng đọc. Bằng chứng root mâu thuẫn đã nằm sẵn trong log (các attestation), không cần gửi lại.
- `message SignedRead { string key = 1; string value = 2; int64 version = 3; int64 height = 4; bytes root = 5; repeated bytes side_nodes = 6; bytes non_membership_leaf_data = 7; int32 node_id = 8; bytes sig = 9; }`. Chữ ký trên `"veritas-read" 0x00 key 0x00 value 0x00 version(8B) height(8B) root sha256(side_nodes ‖ non_membership_leaf_data)`.
- `GetVerifiedRequest` thêm `bool signed = 3;`; `GetVerifiedResponse` thêm `int64 evicted_epoch = 8; bytes node_sig = 9; int32 node_id = 10;`.
- `rpc ReportEvidence(Evidence) returns (EvidenceAck)`, `rpc Evicted(EvictedRequest) returns (EvictedResponse { int64 epoch; repeated int32 nodes; })`.

## 3. Truy vết và loại node

### 3.1 Root mâu thuẫn (F4, F5-F4)

- Giả định của giai đoạn 2: node trung thực cùng height thì cùng root.
- Khi h được xác nhận với root R, mọi attestation hợp lệ cho h với root ≠ R (đã nhận hoặc nhận sau, trong cửa sổ 1024 height) làm node ký nó bị loại với lý do `divergent_root`, tại height của block chứa attestation đó (hoặc của block làm h được xác nhận, nếu attestation đến trước).
- Node trung thực bị loại oan khi một root sai được xác nhận trước (chỉ có thể khi số node lỗi ≥ quorum). Đây là lượng cần đo ở RQ7.
- Root được xác nhận đầu tiên là root xác nhận (giữ hành vi giai đoạn 2). Vì attestation mâu thuẫn làm người ký bị loại ngay và attestation của node bị loại không được đếm, root thứ hai không bao giờ đạt quorum: với quorum f+1 và f+1 node lỗi thông đồng, kết quả phụ thuộc thứ tự block — root đúng được xác nhận trước thì node lỗi bị loại, root sai được xác nhận trước thì node trung thực bị loại oan. Với 2f+1 (N=4: 2 lỗi, 2 trung thực) không root nào được xác nhận và không ai bị loại.

### 3.2 Đọc sai có chữ ký (F6)

- Client nhận `GetVerified` không qua kiểm tra (proof hoặc chứng chỉ sai) thì, nếu `--sign-reads=on-failure`, đọc lại cùng key từ cùng node với `signed=true`; ở `always` mọi lần đọc đều có chữ ký.
- Phản hồi có chữ ký mà proof không khớp root được xác nhận của height đó → client gửi `ReportEvidence(Evidence{culprit, height, read})` tới một node khác (node kế tiếp theo vòng tròn).
- Node nhận kiểm sơ bộ (chữ ký hợp lệ) rồi đưa vào `Block.evidence` của block kế tiếp (hoặc block rỗng sau 100 ms, như attestation).
- Trong vòng apply, mọi node kiểm lại: chữ ký đúng của `culprit`; height còn trong cửa sổ và đã được xác nhận; proof **không** khớp root xác nhận của height đó, hoặc root đã ký ≠ root xác nhận. Đạt → loại `culprit` với lý do `bad_read`. Không đạt (giả mạo, height quá cũ, chưa xác nhận) → bỏ và đếm. Height chưa xác nhận: giữ bằng chứng tới khi xác nhận hoặc ra khỏi cửa sổ.
- Node trả đúng khi bị yêu cầu ký hoặc từ chối ký: client chỉ tự tránh node đó (nghi ngờ), không có bằng chứng. Đo tỉ lệ này ở F6 với `on-failure`.

### 3.3 Hệ quả của việc bị loại

- Attestation của node bị loại không còn được đếm: mọi attestation của nó được xử lý sau khi bị loại đều bỏ, kể cả attestation đến trễ của các height trước đó (cắt theo thứ tự block, không theo height được ký; nếu cắt theo height, với quorum f+1 phía còn lại vẫn có thể được xác nhận ở một height khác và cả hai phía bị loại).
- Node bị loại (nếu trung thực về mã, tức F4) trả `codes.PermissionDenied` cho mọi đọc. Node độc hại có thể vẫn trả lời; client không tin nó nữa (dưới).
- Mỗi lần loại tăng `epoch`. `GetVerifiedResponse.evicted_epoch` cho client biết có thay đổi; client gọi `Evicted` trên node đang dùng và bỏ các node trong danh sách.

### 3.4 Chuyển node phía client (`--failover`)

- Mỗi worker có node chính (như cũ) và danh sách node theo vòng tròn.
- Chuyển sang node kế tiếp khi gặp: `DataLoss`, `PermissionDenied`, lỗi proof/cert, node nằm trong `evicted`, hoặc bị nghi ngờ (3.2). Không chuyển khi `DeadlineExceeded`/`Unavailable` (giữ hành vi backoff của giai đoạn 1 cho F1/F2).
- Ghi: thời điểm chuyển đầu tiên của mỗi worker, số đọc sai được chấp nhận trước đó.
- `--failover` chỉ áp dụng cho đọc; ghi vẫn gửi node chính (ghi qua node lỗi F4/F6 vẫn được đồng thuận áp dụng đúng trên node trung thực).

## 4. Lỗi F6: nói dối khi đọc

- `FaultPolicy.OnRead(key, val string) string`; `NoFault` và các lỗi hiện có trả `val`.
- `--fault=readlie`: `OnApply` trung thực; `OnRead` trả `val + benchmark.CorruptMarker` trong `Get` và `GetVerified` (proof, root, chứng chỉ vẫn của giá trị thật). Kích hoạt bằng `--fault-after` hoặc SIGUSR1 như các lỗi khác.
- Ở `GetVerified` với `signed=true`, node F6 ký đúng phản hồi sai của nó (mô hình node "ngây thơ", để có bằng chứng). Biến thể "né ký" (trả đúng khi bị yêu cầu ký) là `--fault=readlie-evasive`: chỉ đo ở `on-failure` để chỉ ra giới hạn.
- `e6_faults.sh` và driver mới nhận `F6`.

## 5. Ngưỡng xác nhận và đọc từ quorum

- **Ngưỡng:** dùng cờ có sẵn `--attest-quorum`. Harness thêm `ATTEST_QUORUM=f+1|2f+1` (đổi ra số theo N). Phần mới là quy tắc loại node (3.1) và các chỉ số RQ7.
- **Đọc từ quorum (`--read-mode=quorum`, chỉ client):** đọc `Get` từ f+1 node (node chính và f node kế tiếp, song song), chấp nhận khi mọi giá trị bằng nhau; khác nhau → lỗi `quorum`, thử bộ node kế tiếp. Không có proof và không tạo bằng chứng. Là đối chứng chi phí cho `certified`.

## 6. Kafka approve theo quorum (tùy chọn)

`--kafka-quorum=2f+1`: block commit khi đủ 2f+1 phiếu approve (đếm khi đọc lại từ topic, nên vẫn tất định); phiếu đến sau khi đã commit thì bỏ. Abort khi số Abort vượt N − (2f+1). Chạy lại F1, F2 của kafka.

## 7. Thí nghiệm

Phương pháp như giai đoạn 2: vòng mở ở 80% thông lượng bão hòa của E7 trên **máy này**, flush 100 ms, `MEM=1g`, attestation K=1 trừ khi ghi khác, 3 lần chạy, `.params` bảo vệ tham số. Backend: **raft và bft2** (như E8/E9 của giai đoạn 2: đường đọc và truy vết không phụ thuộc backend; một CFT, một BFT). Kafka và tendermint chỉ chạy `check_consistency.sh` với truy vết bật.

| Mã | Câu hỏi | Biến | Lượt (×3) |
|---|---|---|---|
| P2 | điều kiện tiên quyết | `check_consistency.sh` 4 backend; E7 (raft, bft2), E9 của giai đoạn 2 | theo plan giai đoạn 2 (~45 + 75) |
| **E10** chi phí | RQ6/RQ7 không lỗi | read_mode {plain, certified, certified + sign-always, quorum} × workload {a, c} × backend {raft, bft2}, N=4 | 16 × 3 = 48 |
| **E11** truy vết | RQ6 | {F4, F6} (f=1) × {plain, certified + failover, certified + failover + sign on-failure} × backend {raft, bft2}, N=4; thêm F6-evasive × sign on-failure × 2 backend | (12 + 2) × 3 = 42 |
| **E12** ngưỡng | RQ7 | F5-F4 (f+1 node) × quorum {f+1, 2f+1} × read_mode {certified, quorum} × backend {raft, bft2}, N=4 (và N=7 nếu Docker VM ≥ 10 GiB) | 8 × 3 = 24 (48 với N=7) |
| **E13** (tùy chọn) | liveness kafka | kafka F1, F2 × `--kafka-quorum` {all, 2f+1}, N=4 | 4 × 3 = 12 |

Tổng giai đoạn 3 (không kể P2): 126–150 lượt, mỗi lượt 2,5–4 phút: ~8 giờ.

### Chỉ số và định dạng

- CSV giữ 23 cột của giai đoạn 2; như E8/E9, mỗi tổ hợp tham số không có cột riêng chạy thành một EXP riêng (`e10-<read>`, `e11-<fault>-<mode>`, `e12-q<quorum>-<read>`).
- `.ops.csv` thêm: `rejected_reads`, `quorum_errors`, `failovers`, `wrong_before_failover`.
- Sidecar mới `<tag>.evict`: mỗi dòng `node, ts, height, culprit, reason, epoch` lấy từ log `core: evicted node <c> at height <h> (<reason>)` của mọi node; `<tag>.certs` từ log `core: certified height <h> root <r>` (chỉ ghi các height có attestation mâu thuẫn, để biết root nào được xác nhận).
- `rq6.py`: thời gian từ lúc bật lỗi tới bằng chứng đầu tiên, tới khi mọi node trung thực đã loại (cùng height), tới khi mọi worker đã chuyển; `wrong_gets`; với E12 thêm số height xác nhận root sai, số node trung thực bị loại oan và tỉ lệ đọc có xác nhận hết giờ.
- Cổng hợp lệ (FAULT=none): `rejected_reads` = 0 và không có dòng `evicted`.

## 8. Kiểm thử

- **Unit (`-race`)**: certifier ghi attestation mâu thuẫn trước và sau xác nhận, node bị loại không được đếm (root thứ hai không đạt quorum), quorum f+1 với root sai đến trước; `Accountant` kiểm bằng chứng đọc (hợp lệ, giả mạo, height quá cũ, height chưa xác nhận rồi xác nhận); `SignRead`/`VerifyRead`; `OnRead` và `readlie`/`readlie-evasive`; bộ chọn node của client; đọc từ quorum.
- **Tích hợp (`memnet`, 4 node)**: F4 → mọi node loại node lỗi tại cùng height, đọc trên node đó trả `PermissionDenied`; F6 + sign on-failure → bằng chứng đi qua block, mọi node loại cùng height; F5-F4 với quorum f+1 → kết quả theo thứ tự block như 3.1 (hai thứ tự đều được test), với 2f+1 → không xác nhận, đọc hết giờ, không đọc sai.
- **Cụm**: `check_consistency.sh` cho 4 backend với `ATTEST_EVERY=1` và truy vết bật.

## 9. Lộ trình (12 tuần)

| Tuần | Việc | Điều kiện xong |
|---|---|---|
| 1 | Build ảnh giai đoạn 2 trên M3; chạy toàn bộ test trong `hbdb-build`/`hbdb-bft2-build`; `check_consistency.sh` 4 backend; sửa lỗi | consistency đạt |
| 2–3 | E7 (raft, bft2, để có tốc độ 80%), E9; ghi NOTES | có E9 trên M3; tag `p7-openloop` … `p10-attest` của giai đoạn 2 |
| 4 | F6 (`OnRead`, `readlie`, `readlie-evasive`), client failover, `--read-mode=quorum` | unit test qua |
| 5–6 | Certifier mâu thuẫn, `Accountant`, `Block.evidence`, `SignedRead`, `ReportEvidence`, `Evicted`, sign-reads | test tích hợp qua; tag `p11-accountability` |
| 7 | Harness: `.evict`, `.certs`, `rq6.py`, driver E10–E12, smoke | smoke mỗi backend |
| 8–9 | Chạy E10, E11, E12 | **Mốc lõi**: RQ6, RQ7 có số liệu |
| 10 | E13 (tùy chọn) hoặc chạy lại điểm CV > 10% | |
| 11–12 | Phân tích, hình, viết chương | bản nháp chương |

## 10. Rủi ro

| Rủi ro | Phát hiện ở | Dự phòng |
|---|---|---|
| Mã giai đoạn 2 lỗi khi chạy trên cụm (chưa từng chạy) | Tuần 1 | Dành trọn tuần 1–3; nếu E9 không chạy được thì giai đoạn 3 lấy mốc là `certified` trong E10 |
| `MEM=1g` × N=7 vượt Docker VM 7,65 GiB | Tuần 1 | Nâng VM lên ≥ 10 GiB; không được thì chỉ N=4 |
| Bằng chứng đọc cho height đã ra khỏi cửa sổ 1024 | E11 | Đếm và báo; cửa sổ 1024 height ≫ độ trễ phát hiện dự kiến (vài giây) |
| Node lỗi đề xuất block chứa bằng chứng giả để loại node trung thực | Unit test | Mọi node kiểm lại bằng chứng trong vòng apply; bằng chứng giả bị bỏ và đếm |
| Loại oan khi root sai được xác nhận (quorum f+1, F5-F4) | E12 | Đây là kết quả cần đo, không phải lỗi |

## 11. Giới hạn cần nêu

- Loại node chỉ có tác dụng ở tầng đọc và attestation; node bị loại vẫn tham gia đồng thuận.
- F6 không có bằng chứng nếu node né ký (trừ `sign-always`).
- Bằng chứng đọc chỉ kiểm được trong cửa sổ 1024 height đã xác nhận.
- Quorum f+1 không an toàn khi có ≥ f+1 node lỗi thông đồng; 2f+1 an toàn nhưng mất sẵn sàng đọc.
- E10–E12 chỉ có raft và bft2; mọi giới hạn của giai đoạn 1–2 vẫn áp dụng.
