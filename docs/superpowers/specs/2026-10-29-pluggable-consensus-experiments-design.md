# Thiết kế thực nghiệm: Veritas với đồng thuận cắm-rút (pluggable consensus)

- **Ngày:** 2026-10-29
- **Bối cảnh:** Phần thực nghiệm luận văn thạc sĩ, phát triển từ bài *Hybrid Blockchain Database Systems: Design and Performance* (Ge et al., PVLDB 15(5), 2022).
- **Trạng thái:** Đã duyệt thiết kế qua 4 phần; chờ duyệt bản spec.

## 1. Mục tiêu

Bài gốc kết luận rằng **giao thức đồng thuận là nút cổ chai** của hệ hybrid blockchain database, nhưng chỉ thử Kafka (CFT) và Tendermint (BFT), và chỉ đo hiệu năng. Luận văn đi tiếp bằng cách biến Veritas thành nền tảng mà **chỉ tầng đồng thuận thay đổi**, rồi đo cả hiệu năng lẫn chi phí bảo mật.

### Câu hỏi nghiên cứu

- **RQ1:** Một BFT có độ phức tạp tuyến tính (HotStuff) thay đổi throughput, latency và khả năng mở rộng so với Tendermint ra sao?
- **RQ2:** Raft (CFT dựa trên leader) so với Kafka approve-all (CFT pub-sub) thế nào? Raft có tránh được chi phí ACK O(N²) của Veritas (Kafka) không?
- **RQ3:** Khi có f node lỗi hoặc Byzantine, mỗi backend mất bao nhiêu hiệu năng? Lớp ledger (Sparse Merkle Tree và `Verify`) phát hiện sai lệch trạng thái nhanh tới đâu?

### Tiêu chí thành công

1. Trả lời RQ1–RQ3 bằng số liệu tái lập được: mỗi điểm dữ liệu chạy 3 lần, ghi kèm commit hash và tham số.
2. Có bằng chứng so sánh công bằng: kiểm tra hồi quy ở P1 và kiểm tra cùng root digest ở P2/P3 (mục 3).
3. Có bản nháp chương thực nghiệm vào cuối tuần 16 (đường đầy đủ) hoặc tuần 13 (đường rút gọn).

### Ràng buộc

- **Thời gian:** 3–4 tháng.
- **Phần cứng:** MacBook M3 Pro (ARM64), 18 GB RAM. Mọi node chạy bằng Docker trên một máy; quy mô 4–16 node.
- **Nhân lực:** một người, viết Go.

### Không làm (non-goals)

- Sharding, API SQL, đổi CSDL nền (bài gốc đã cho thấy CSDL nền ảnh hưởng ít).
- Tấn công equivocation (leader gửi block khác nhau cho từng follower): phải sửa sâu bên trong thư viện đồng thuận. Ghi là hướng mở rộng.
- Tấn công Kafka broker hoặc TSO: chúng được coi là trung thực, như trong bài gốc. Ghi trong phần giới hạn.
- Triển khai trên nhiều máy thật.
- Gộp ACK bằng chữ ký BLS.

## 2. Kiến trúc

### Hiện trạng repo (đã khảo sát)

- `veritas/server.go`: logic thực thi gắn chặt với Kafka. `batchLoop` gom block rồi `Produce`; `applyLoop` đọc Kafka, kiểm tra version với Redis, gửi approve, commit khi đủ `len(Parties)` approve.
- `veritastm/server.go`: mỗi `Set` gọi `BroadcastTxCommit` một tx (không gom block). `Verify()` trả `nil, nil` (chưa cài). `BatchSet` chưa cài. `LedgerApp.DeliverTx` ghi Redis và ledger.
- `raftkv/`: dùng hashicorp/raft, có ledger, nhưng theo proto riêng (`proto/raftkv`), không theo API Veritas; chưa có script benchmark.
- Phiên bản: Tendermint v0.35.0, `confluent-kafka-go` v1.7.0, hashicorp/raft v1.3.2.

### Thiết kế mới

```
Client (bộ benchmark YCSB)
      │ gRPC Set/Get/Verify/BatchSet (proto/veritas, giữ nguyên)
┌─────▼───────────────────────────┐
│ veritas/core                     │  chung cho mọi backend
│  - Batcher: gom tx thành block   │
│  - Executor: kiểm tra version,   │
│    ghi Redis, append ledger SMT  │
│  - Verify: proof Sparse Merkle   │
│  - FaultPolicy hooks (mục 4)     │
└─────▲───────────────┬───────────┘
      │ Deliver()     │ Submit(block)
┌─────┴───────────────▼───────────┐
│ consensus.Consensus (interface)  │
├──────────┬──────────┬───────────┬──────────┐
│ kafka    │ raft     │ tendermint│ hotstuff │
└──────────┴──────────┴───────────┴──────────┘
```

```go
type Consensus interface {
    Submit(ctx context.Context, blk []byte) error // đề xuất một block
    Deliver() <-chan []byte                       // block đã thống nhất, đúng thứ tự
    Close() error
}
```

### Quyết định thiết kế

1. **Kafka backend giữ giao thức approve-all** của bài gốc, chỉ chuyển phần đó từ `applyLoop` vào `consensus/kafka`. Block chỉ được `Deliver` khi đủ N approve. Nhờ vậy kết quả Kafka vẫn so được với bài báo.
2. **Raft, Tendermint, HotStuff** chỉ trả block theo thứ tự. Executor trên mọi node chạy cùng một hàm thực thi tất định.
3. **Batcher nằm trong core** và gửi cả block thành một đơn vị (với Tendermint: một ABCI tx = một block Veritas). Block size vì thế cố định như nhau cho mọi backend; bài gốc không làm được điều này với Tendermint.
4. **Luật kiểm tra version dùng chung** trong Executor. Version lấy từ:
   - Kafka, Raft: TSO (như bài gốc).
   - Tendermint, HotStuff: tất định theo `(height, index)`, vì TSO là thành phần tập trung, không hợp môi trường BFT.
   Đây là **biến gây nhiễu đã biết** và phải được ghi rõ trong luận văn.
5. **`Verify` dùng chung trong core**, thay thế bản `veritastm` đang trả `nil`.
6. Mỗi backend nằm trong package riêng `consensus/<tên>`, chọn bằng flag khi khởi động `cmd/veritas`.
7. **Chế độ commit** (bổ sung khi lập plan): code gốc `veritas/server.go` trả `Set` ngay khi tx vào hàng đợi (async), còn `veritastm` chờ commit (sync). Core hỗ trợ `--commit-mode=async|sync`. **Mọi thí nghiệm so sánh E1–E6 dùng `sync`**; `async` chỉ dùng cho kiểm tra hồi quy P1 (để so được với code gốc) và được nêu trong luận văn như một phát hiện về bài gốc.
8. **Kafka approve-all tất định** (bổ sung khi lập plan): code gốc tự tính approve của chính node ngay khi kiểm tra, nên các node có thể hoàn tất block theo thứ tự khác nhau. Backend mới chỉ tính approve khi đọc lại từ topic, để mọi node commit cùng thứ tự và kiểm tra cùng root digest có nghĩa.

## 3. Các phase và lộ trình

| Phase | Tuần | Việc chính | Điều kiện hoàn thành (exit) |
|---|---|---|---|
| **P0. Môi trường và baseline** | 1–2 | `git init`, tag `p0-baseline`. Build trên ARM64 (nâng `confluent-kafka-go` lên bản có arm64; build lại image Docker arm64). Chạy lại thí nghiệm theo số client và số node (4/8/16) cho Veritas (Kafka) và Veritas (TM) bằng code gốc. | Có số liệu baseline trên M3. Xu hướng khớp bài gốc: Kafka cao hơn TM khoảng 10×, TM giảm khi tăng node. |
| **P1. Refactor core và Kafka backend** | 3–5 | Tách `veritas/core` và interface `Consensus`; chuyển approve-all vào `consensus/kafka`. Unit test cho Executor tất định. | **Kiểm tra hồi quy:** throughput Veritas-Kafka mới chênh không quá ±10% so với P0 ở các cấu hình N=4, block 100, YCSB-A, 64 và 256 client. |
| **P2. Raft và Tendermint backend** | 6–7 | `consensus/raft` tái dùng hashicorp/raft từ `raftkv`; `consensus/tendermint` chuyển `LedgerApp` sang ABCI theo block. | **Kiểm tra cùng root digest:** cùng một workload, mọi node của mọi backend kết thúc với cùng SMT root digest. |
| **Mốc HotStuff** | 7–8 | Spike: cụm HotStuff 4 node (thư viện Go có sẵn, ví dụ relab/hotstuff; cần đánh giá độ hoàn thiện) chạy được trên arm64 và commit được block. | **Đạt** (cuối tuần 8): sang P3 với HotStuff. **Không đạt:** P3 dùng CometBFT bản mới, hoặc bỏ P3 (đường rút gọn). |
| **P3. Backend BFT thứ hai** | 9–10 | `consensus/hotstuff` (hoặc CometBFT). | Qua kiểm tra cùng root digest như P2. |
| **P4. Tiêm lỗi** | 10–12 | `FaultPolicy`, script F1–F5, auditor (mục 4). | Mỗi kịch bản chạy được bằng một script; kết quả ổn định qua 3 lần chạy. |
| **P5. Chạy ma trận thí nghiệm** | 8–14 | Chạy E1–E6 (mục 5), ban đêm. Backend nào sẵn sàng thì chạy backend đó trước. | Mỗi điểm dữ liệu có 3 lần chạy, độ lệch chuẩn ≤ 10%. |
| **P6. Phân tích và dự phòng** | 15–16 | Vẽ biểu đồ, viết chương thực nghiệm, chạy lại điểm bất thường. | Có bản nháp chương thực nghiệm. |

**Đường rút gọn (13 tuần):** bỏ P3, so sánh Kafka, Raft, Tendermint. RQ1 đổi thành "Tendermint so với CFT".

**Quy ước:** mỗi phase kết thúc bằng một git tag (`p0-baseline`, `p1-core`, `p2-backends`, `p3-bft2`, `p4-faults`). Script thí nghiệm ghi commit hash vào mọi file kết quả.

## 4. Tiêm lỗi (RQ3)

### Mô hình mối đe dọa

- N = 3f+1 node, với (N, f) ∈ {(4, 1), (7, 2), (10, 3)}, áp dụng cho cả CFT lẫn BFT để so cùng điều kiện.
- Client, Kafka broker và TSO được coi là trung thực.

### Kịch bản

| Mã | Kịch bản | Cách tiêm |
|---|---|---|
| F1 | Crash: f node dừng giữa lượt chạy | `docker stop` theo lịch |
| F2 | Im lặng: node vẫn sống nhưng không gửi approve/vote | `FaultPolicy.OnVote` bỏ thông điệp |
| F3 | Chậm: node hoặc leader trễ 200 ms | `tc netem` trong container (`NET_ADMIN`) |
| F4 | Sai trạng thái: node ghi giá trị sai vào Redis/SMT, trả `Get` sai | `FaultPolicy.OnApply` làm hỏng value |
| F5 | Vượt ngưỡng: f+1 node lỗi kiểu F2 hoặc F4 (chỉ cho BFT) | Ghép F2/F4 với f+1 node |

### Cài đặt

- `FaultPolicy` có 3 hook trong core: `OnSubmit`, `OnVote`, `OnApply`. Bật bằng flag cho từng node; mặc định là no-op.
- Với Kafka, `OnVote` nằm ở bước gửi approve. Với Raft/Tendermint/HotStuff, F2 được thực hiện bằng cách chặn thông điệp đồng thuận của node đó ở tầng mạng (`tc`/iptables trong container), nên không cần sửa thư viện.
- **Auditor:** client riêng, mỗi giây gọi `Verify` trên mọi node, so root digest, đánh dấu node lệch đa số và ghi thời điểm phát hiện.

### Giả thuyết

| | Kafka (approve-all) | Raft | Tendermint / HotStuff |
|---|---|---|---|
| F1 | Dừng hẳn (cần đủ N approve) | Chạy tiếp nếu còn đa số; bầu leader mới nếu leader chết | Chạy tiếp; HotStuff đổi view nhanh hơn |
| F2 | Dừng hẳn | Chạy tiếp | Chạy tiếp |
| F3 | Chậm theo node chậm nhất | Chậm nếu node chậm là leader | Chậm theo timeout của round/view |
| F4 | Chạy tiếp; chỉ auditor phát hiện | Như Kafka | Như Kafka; auditor phát hiện |
| F5 | không áp dụng | không áp dụng | Mất liveness hoặc tính an toàn |

### Chỉ số RQ3

Throughput khi có lỗi so với không lỗi; thời gian khôi phục (đến khi đạt lại 90% throughput); thời gian mất liveness; số `Get` sai mà client nhận được; thời gian để auditor phát hiện.

## 5. Ma trận thí nghiệm và phương pháp đo

### Ma trận

| Mã | RQ | Biến thay đổi | Giữ cố định | Số cấu hình |
|---|---|---|---|---|
| E1 | RQ1/2 | Số client {4, 16, 64, 256} | N=4, block 100, YCSB-A, phân phối uniform | 4 backend × 4 = 16 |
| E2 | RQ1/2 | N {4, 7, 10, 13, 16} | Số client tốt nhất từ E1 | 4 × 5 = 20 |
| E3 | RQ1/2 | Block size {10, 100, 1000} | N=4 | 4 × 3 = 12 |
| E4 | RQ1/2 | Workload YCSB A/B/C | N=4 | 4 × 3 = 12 |
| E5 | RQ1/2 | RTT {0, 20, 60 ms} | N=4 | 4 × 3 = 12 |
| E6 | RQ3 | F1–F4 × f {1, 2}; F5 cho BFT | Block 100 | khoảng 36 |

Tổng khoảng 108 cấu hình × 3 lần chạy ≈ 324 lượt, mỗi lượt khoảng 2,5 phút (kể cả dựng cụm), tức khoảng 14 giờ.

Không đưa vào: phân phối khóa, kích thước bản ghi, thời gian xử lý, đổi CSDL nền (bài gốc cho thấy ít ảnh hưởng hoặc không liên quan tới đồng thuận).

### Phương pháp đo

- Mỗi lượt: khởi động 10 giây (bỏ số liệu), đo 60 giây. Riêng E6: đo 180 giây, tiêm lỗi ở giây thứ 60.
- Trước mỗi lượt: nạp sẵn dữ liệu YCSB (giá trị 1 KB như bài gốc).
- Mỗi node giới hạn bằng nhau `--cpus` và `--memory`. Ghi lại CPU của host; điểm có CPU host > 90% được đánh dấu "laptop bão hòa" trên biểu đồ.
- 3 lần chạy, báo trung bình ± độ lệch chuẩn; độ lệch chuẩn > 10% thì chạy lại.
- Cắm sạc và để máy rảnh khi đo.
- Mỗi lượt ghi một dòng CSV gồm: commit hash, backend, N, f, số client, block size, workload, RTT, kịch bản lỗi, và các chỉ số. Script Python đọc CSV và vẽ biểu đồ.

### Chỉ số

| Nhóm | Chỉ số | Thu thập bằng |
|---|---|---|
| Hiệu năng | Throughput; latency trung bình, p50, p99 | Bộ benchmark Go (sửa để ghi latency từng request) |
| Chi phí đồng thuận | Byte mạng trên mỗi block (mọi backend); số thông điệp Kafka (như Hình 8) | NET I/O từ `docker stats` chia cho số block đã commit; offset Kafka |
| Phân rã thời gian | Gom block / chờ đồng thuận / thực thi / ghi ledger | Đo trong core |
| Tài nguyên | CPU, RAM mỗi node | `docker stats`, lấy mẫu mỗi giây |
| Bảo mật | Như mục 4 | Benchmark và auditor |

## 6. Rủi ro

| Rủi ro | Phát hiện ở | Dự phòng |
|---|---|---|
| Kafka/cgo không build được trên ARM64 | P0, tuần 1 | Nâng `confluent-kafka-go`; nếu vẫn lỗi thì đổi sang `segmentio/kafka-go` và làm lại baseline với client mới |
| 16 node vượt sức laptop | P0 | Giới hạn N ≤ 13, ghi trong phần giới hạn |
| Thư viện HotStuff chưa hoàn thiện | Mốc tuần 8 | CometBFT, hoặc đường rút gọn |
| Tendermint 0.35 có lỗi đồng bộ | P2 | Chuyển sang CometBFT (ABCI gần giống) |
| Trễ tiến độ | Cuối mỗi phase | Cắt E5 trước, rồi E3. E1, E2, E6 là lõi, không cắt |

## 7. Giới hạn cần nêu trong luận văn

- Mọi node chạy trên một laptop; mạng được giả lập bằng `tc netem`.
- Quy mô tối đa 16 node (bài gốc: 64).
- Kafka broker và TSO được coi là trung thực; không thử equivocation.
- Cách gán version khác nhau giữa backend CFT (TSO) và BFT (`(height, index)`).
- Chạy theo thời gian thay vì số tx cố định, nên chỉ so xu hướng với bài gốc, không so từng con số.
