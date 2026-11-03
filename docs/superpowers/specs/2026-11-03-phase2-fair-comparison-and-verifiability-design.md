# Thiết kế giai đoạn 2: so sánh công bằng và chi phí của tính kiểm chứng được

- **Ngày:** 2026-11-03
- **Bối cảnh:** tiếp nối giai đoạn 1 (`docs/superpowers/specs/2026-10-29-pluggable-consensus-experiments-design.md`), sau hai bộ số liệu M3 (`results/final/`) và cloud (`results/cloud-bf100ms/`); phát hiện và hạn chế ghi ở `experiments/NOTES.md`.
- **Phương án đã chốt:** làm phần A (điều kiện để kết quả RQ1/RQ2 đứng vững) rồi hai đóng góp B1 (chi phí của đọc có kiểm chứng và chữ ký thật) và B2 (xác nhận root trạng thái trong giao thức).

## 1. Mục tiêu

Bài gốc (Ge et al., PVLDB 15(5), 2022) kết luận đồng thuận là nút cổ chai nhưng chỉ đo hiệu năng. Giai đoạn 1 thêm 4 backend và tiêm lỗi, nhưng: tải vòng kín làm block không đầy và thông lượng phụ thuộc bộ hẹn giờ; tài nguyên và tham số giữa các backend không cân bằng; hai adapter (bft2, tendermint) có giới hạn riêng làm sai lệch kết quả E6; và phần "verifiable" của hệ hybrid (proof, chữ ký) chưa được hiện thực hay đo.

### Câu hỏi nghiên cứu

- **RQ1/RQ2 (đo lại):** ở **thông lượng bão hòa dưới một ngưỡng độ trễ**, với tài nguyên và tham số cân bằng và adapter đã sửa, thứ hạng và khoảng cách giữa kafka, raft, tendermint, bft2 là gì?
- **RQ4 (mới):** đọc có kiểm chứng (Merkle proof được client kiểm) và chữ ký Ed25519 thật làm mất bao nhiêu thông lượng, độ trễ, byte và CPU, so với chi phí đồng thuận?
- **RQ5 (mới):** nếu các node ký và đồng thuận về root trạng thái của từng block (attestation), thời gian phát hiện trạng thái sai và số lần đọc sai mà client chấp nhận giảm bao nhiêu so với auditor ngoài; giá phải trả là bao nhiêu?

### Tiêu chí thành công

1. Mỗi RQ có số liệu từ một máy duy nhất, 3 lần chạy mỗi điểm, CV ≤ 10% ở các điểm không lỗi (theo quy tắc chạy thêm như giai đoạn 1).
2. Kiểm tra cùng root digest (`experiments/check_consistency.sh`) đạt cho cả 4 backend sau mọi thay đổi ở core, adapter và đường đọc.
3. Với F4 có attestation: client dùng đọc có xác nhận (`certified`) chấp nhận **0** giá trị sai; node lỗi tự chuyển sang trạng thái `diverged` trong ≤ 2 chu kỳ xác nhận.
4. Bản nháp chương thực nghiệm giai đoạn 2 cuối tuần 8.

### Không làm (non-goals)

- Tấn công equivocation, leader đề xuất block khác nhau (cần sửa sâu thư viện đồng thuận).
- Chữ ký gộp BLS / chữ ký ngưỡng; chạy nhiều máy thật; sharding; đồng bộ trạng thái cho node quay lại. Ghi là hướng mở rộng.
- Ký block ở tầng Veritas (HotStuff và Tendermint đã ký ở tầng đồng thuận; Kafka/Raft là CFT).
- Cây Merkle nhiều phiên bản (proof cho root cũ): proof luôn được tạo tại thời điểm đọc.

## 2. Phần A: phương pháp đo công bằng

### A1. Tải vòng mở và điểm bão hòa

- `ycsbbench --rate=R` (tổng req/s, 0 = vòng kín như cũ): một bộ lập lịch phát thao tác theo khoảng đều `1/R`, chia vòng tròn cho các worker; `--max-inflight=M` (mặc định 4096) chặn số request đang chờ. Khi đạt M, thao tác bị bỏ và đếm `dropped` (không chờ, để tốc độ gửi không tự giảm).
- **Độ trễ tính từ thời điểm lên lịch**, không phải từ lúc gửi thật (tránh coordinated omission).
- Quét tốc độ cho từng backend: `RATES` mặc định `250 500 1000 1500 2000 3000 4000` (tendermint: `50 100 200 300 400 600`), N=4, block 100, YCSB-A, flush 100 ms.
- **Thông lượng bão hòa** của một backend = tốc độ gửi lớn nhất mà tps đạt được ≥ 0,95 × tốc độ gửi, write p99 ≤ **2000 ms** và `dropped` = 0. Báo cáo kèm đường cong p99 theo thông lượng đạt được.
- CSV thêm một cột cuối `offered_rps` (0 = vòng kín). `plot.py` và các script đọc CSV nhận cả header 22 cột cũ (coi như `offered_rps` = 0) lẫn header 23 cột mới.

### A2. Cân bằng tài nguyên và tham số

- Broker Kafka: thêm `KAFKA_CPUS`/`KAFKA_MEM` (mặc định giữ 2 CPU / 1536m như giai đoạn 1). Cấu hình công bằng: `KAFKA_CPUS=0.5 KAFKA_MEM=768m` (bằng một node).
- Tendermint: thêm `TM_TIMEOUT_COMMIT` (mặc định để trống = mặc định của Tendermint, 1 s). Quét `1s`, `100ms`, `0s`.
- `MEM=1g` cho mọi lượt của giai đoạn 2 (giai đoạn 1 bị OOM ở 768m khi thông lượng cao).
- Cả bốn biến (`KAFKA_CPUS`, `KAFKA_MEM`, `TM_TIMEOUT_COMMIT`, `RATE`/`MAX_INFLIGHT`) đi vào bảo vệ tham số `.params` của driver (EXP nào cũng chỉ có một bộ giá trị).

### A3. Sửa giới hạn adapter

- **bft2 (hàng đợi chung):** một block `Submit` ở node i được sidecar chuyển tới mọi replica khác (`POST /forward` trên cổng 7202). Mọi replica giữ cùng tập block chờ; leader nào cũng đề xuất được. Định danh block = SHA-256 của bytes. Khi một replica thấy proposal chứa id X, X chuyển từ hàng đợi sang `inflight`; khi X được commit thì xóa; nếu sau 8 view X vẫn chưa commit thì trả X về đầu hàng đợi. Commit log của sidecar bỏ qua id đã commit (giữ 65536 id gần nhất), nên mỗi block được `Deliver` đúng một lần.
- **tendermint (không chặn khi bão hòa):** `DeliverTx` không bao giờ chờ kênh Deliver trong lúc giữ khóa của app. Block được đẩy vào hàng đợi nội bộ có giới hạn byte (`128 MiB`); vượt giới hạn là lỗi nghiêm trọng: node dừng có log `tendermint: deliver queue overflow`, không treo và không bỏ block. `CheckTx` không dùng khóa mà `DeliverTx` giữ.
- Sau khi sửa: chạy lại `check_consistency.sh` cho bft2 và tendermint, rồi E6 cho F1, F2 và F5-F2 của bft2, tendermint ở N=4 và 7. So sánh với hai phát hiện của đợt cloud (tendermint F2 mất liveness; bft2 F1/F2 còn 0,1–0,24 tỉ lệ ghi).

## 3. Phần B: đóng góp

### B1. Chữ ký thật và đọc có kiểm chứng (RQ4)

**Chữ ký.** Gói mới `veritas/sigs`: Ed25519 (`crypto/ed25519`, thư viện chuẩn). Khóa sinh tất định từ seed chung và định danh (`sigs.KeyFor(seed, id)`), giống cách `bft2node/node/keys.go` sinh khóa, nên không cần phân phối khóa.

- `SetRequest` thêm `client_id` (string) và `sig` (bytes) = chữ ký trên bytes chuẩn hóa `key 0x00 value 0x00 version(8 byte big-endian)`.
- Cờ node `--verify-sigs=none|ingress|all`: `ingress` kiểm ở `Server.Set`/`BatchSet` (node nhận request); `all` kiểm thêm ở `Executor` trên mọi replica trước khi áp dụng (mô hình BFT: không tin proposer). Set có chữ ký sai: ở `ingress` trả `codes.Unauthenticated`; ở `all` (lúc áp dụng) bị bỏ, seq vào `aborted`.
- Client: `driver.Open` nhận `ClientKey`; `ycsbbench --sign` bật ký.

**Đọc có kiểm chứng.** RPC mới `GetVerified(GetVerifiedRequest{key}) → GetVerifiedResponse{value, version, height, root_digest, side_nodes, non_membership_leaf_data, attestations}`.

- Node đọc giá trị, version, proof, root và height **trong cùng một lần giữ khóa executor**, nên proof luôn khớp root tại height đó.
- Client kiểm bằng `merkletree.VerifyProof(proof, root, key, value+"-"+version, sha256)`. Proof sai thì lượt đọc là lỗi `proof`, và giá trị không được dùng.
- `ycsbbench --read-mode=plain|verified|certified`: `certified` thuộc B2.
- Đo thêm: byte trung bình của phản hồi đọc, thời gian CPU kiểm proof phía client.

### B2. Xác nhận root trong giao thức (RQ5)

**Attestation.** Sau khi áp dụng block ở height h với `h % K == 0` (`--attest-every=K`, mặc định 1), node ký `(h, root_h)` bằng khóa node (`sigs.KeyFor(seed, "node<i>")`) thành `RootAttestation{height, root, node_id, sig}`.

- Cách gửi: gắn vào block dữ liệu kế tiếp của chính node (`Block.attestations`). Nếu sau 100 ms node không có block dữ liệu nào, nó gửi một block chỉ chứa attestation. Mọi backend đều đi qua `Consensus.Submit`, nên cơ chế không phụ thuộc backend.
- Block chỉ chứa attestation vẫn tăng height (mọi node thấy cùng dãy), không có Set.

**Certifier** (trong core, chạy trên vòng apply, tất định). Gom attestation theo height. Height h được **xác nhận** khi có Q chữ ký hợp lệ từ Q node khác nhau trên cùng một root, với `Q = 2f+1`, `f = floor((N-1)/3)` (N=4: Q=3; N=7: Q=5). Mặc định `--attest-quorum=0` nghĩa là tự tính Q.

- Chữ ký sai, node không có trong cấu hình, hoặc attestation trùng của cùng node cho cùng height: bỏ qua và đếm trong metrics.
- Nếu root đã xác nhận của h khác root của chính node: node chuyển sang `diverged`. Nó ghi log `core: diverged at height h`, `Get`/`GetVerified` trả `codes.DataLoss`, `Verify` vẫn trả lời để auditor quan sát. Đây là F4 chuyển thành fail-stop.
- Giữ attestation của 1024 height gần nhất.

**Đọc có xác nhận** (`--read-mode=certified`). Node tạo proof tại height h như B1, rồi chờ (tối đa 5 s) tới khi h được xác nhận. Phản hồi kèm Q attestation của h.

- Client kiểm proof, và kiểm ≥ Q chữ ký hợp lệ từ các node khác nhau trên đúng `(h, root)`.
- Không đủ thì lượt đọc là lỗi `cert`. Hết thời gian chờ thì trả `codes.DeadlineExceeded`.

**Chỉ số RQ5:**
- thời gian phát hiện trong giao thức, từ lúc bật lỗi tới dòng `diverged`, lấy từ log node;
- số đọc sai được client chấp nhận (`wrong_gets`);
- số đọc bị client từ chối (lỗi `proof`/`cert`);
- thông lượng và độ trễ khi bật attestation, so với khi tắt (không lỗi);
- so với thời gian phát hiện của auditor ngoài (`.audit`).

## 4. Ma trận thí nghiệm

| Mã | RQ | Biến | Giữ cố định | Lượt (×3 rep) |
|---|---|---|---|---|
| E7 | RQ1/2 | tốc độ gửi (A1) × 4 backend | N=4, block 100, YCSB-A, flush 100 ms, MEM=1g, tham số mặc định | (3 × 7 + 6) × 3 = 81 |
| E7f | RQ1/2 | như E7 với `KAFKA_CPUS=0.5 KAFKA_MEM=768m` (kafka) và `TM_TIMEOUT_COMMIT` ∈ {100ms, 0s} (tendermint) | như E7 | (7 + 2 × 6) × 3 = 57 |
| E6' | RQ3 | F1, F2, F5-F2 sau A3 | bft2, tendermint; N ∈ {4, 7}; tốc độ = 80% thông lượng bão hòa E7 | 2 × 2 × 3 × 3 = 36 |
| E8 | RQ4 | `--verify-sigs` ∈ {none, ingress, all} × `--read-mode` ∈ {plain, verified} | raft, bft2; N=4; workload a và c; tốc độ = 80% bão hòa | 2 × 2 × 6 × 3 = 72 |
| E9 | RQ5 | attestation tắt/bật (K ∈ {1, 10}) × lỗi {none, F4, F5-F4} × read-mode {plain, certified} | raft, bft2; N=4; tốc độ = 80% bão hòa; `certified` chỉ khi có attestation; F5-F4 chỉ cho bft2 | (10 + 15) × 3 = 75 |

Tổng 321 lượt, mỗi lượt 2,5–5 phút: khoảng 18 giờ.

- **Máy:** mọi lượt chạy trên một máy, mặc định là phiên cloud (4 CPU, CV thấp ở giai đoạn 1). Những gì cần netem (RTT, F3) không thuộc giai đoạn 2.
- **Vì sao chỉ raft và bft2 ở E8, E9:** đường đọc và attestation không phụ thuộc backend; một CFT và một BFT là đủ để so. Kafka và tendermint vẫn được kiểm bằng `check_consistency.sh`.

## 5. Lộ trình

| Phase | Tuần | Việc | Điều kiện hoàn thành |
|---|---|---|---|
| P7 | 1–2 | A1, A2; chạy E7, E7f | Có thông lượng bão hòa của 4 backend ở cấu hình mặc định và công bằng |
| P8 | 2–3 | A3; chạy lại E6' | `check_consistency.sh` đạt cho bft2 và tendermint; có E6' |
| P9 | 4–5 | B1; chạy E8 | Kiểm tra cùng root digest đạt với `--verify-sigs=all`; có E8 |
| P10 | 5–7 | B2; chạy E9 | Tiêu chí thành công 3; có E9 |
| P11 | 8 | Phân tích, viết | Bản nháp chương thực nghiệm giai đoạn 2 |

Mỗi phase kết thúc bằng một tag (`p7-openloop`, `p8-adapters`, `p9-verify`, `p10-attest`).

## 6. Rủi ro

| Rủi ro | Dự phòng |
|---|---|
| Bộ lập lịch vòng mở không đạt tốc độ khi máy bão hòa | Ghi tốc độ thực phát; lượt có `dropped` > 0 hoặc tốc độ thực < 0,98 × tốc độ gửi bị đánh dấu, không dùng để xác định điểm bão hòa |
| Hàng đợi chung của bft2 sinh block trùng | Khử trùng ở commit log (A3); test bắt buộc ở bft2node |
| Ký và kiểm Ed25519 ở `all` làm node bão hòa CPU 0,5 | Đó là kết quả cần đo; báo cáo cùng CPU node |
| Attestation làm tăng số block (block chỉ chứa attestation) | Đo riêng ở E9 với K=1 và K=10 |
| Không kịp B2 | B1 đứng riêng được; B2 rút thành chỉ có `diverged` (không có đọc có xác nhận) |

## 7. Giới hạn cần nêu

- Một máy, tối đa N=7, không có netem (như đợt cloud).
- Khóa sinh từ seed chung: đủ để đo chi phí, không phải quản lý khóa thật.
- Attestation dùng quorum 2f+1 cho cả backend CFT, để so cùng điều kiện; với Kafka/Raft đó là giả định thêm, không phải tính chất của giao thức.
- Đọc có xác nhận chỉ chứng minh giá trị khớp trạng thái tại một height đã được xác nhận, không chứng minh đó là giá trị mới nhất.
