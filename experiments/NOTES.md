# Ghi chú thí nghiệm P0 (baseline: kafka-orig, tm-orig)

## Cấu hình máy

- MacBook M3 Pro (arm64), 18 GB RAM.
- Docker VM: 11 CPU (đã xác nhận bằng `docker info`), 7.65 GiB RAM.
- Docker VM không được nâng RAM: mọi lần chạy P0 (kể cả N=8) dùng 11 CPU / 7.65 GiB. N=16 không chạy (xem "Quyết định về N tối đa").
- Giới hạn mỗi node: `cpus 0.5`, `mem 768m` (`CPUS=0.5`, `MEM=768m` trong `run_one.sh`).
- Mỗi lần chạy: cụm mới, `DURATION=60s`, `WARMUP=10s`, workload A (uniform), 3 lần lặp.
- Lấy mẫu CPU của host (`host_cpu_max_pct`): mỗi lần gọi `docker stats --no-stream` mất khoảng 1-1,5 s, nên chu kỳ lấy mẫu khoảng 1-1,5 s (không phải đúng 1 s).

## Quy tắc khi chạy

- **Không chạy workload Docker nào khác trong lúc chạy batch.** Daemon Docker dùng chung: lúc 10:55:31Z một tiến trình bên ngoài (container của dự án khác: kamal-docker-registry, buildx_buildkit, ...) dừng mọi container trên daemon, kể cả cụm `hbdb`, làm hai lần chạy tm-orig c16 thất bại với rc=3 (kill 15, exit 143). Harness xử lý đúng (rc=3, không ghi dòng CSV, thử lại, ghi `.failures`); chạy lại cùng cấu hình thì bình thường (tps ~19,5-19,9).
- `run_one.sh` giữ khóa `results/raw/.hbdb.lock` (mkdir; chứa pid, EXP, tag): chỉ một lần chạy tại một thời điểm trên project compose `hbdb`; khóa của pid đã chết được tiếp quản. Log lỗi nằm ở `results/raw/<EXP>/logs/<tag>.<UTC>/` (mỗi lần thử một thư mục, kèm `docker-events.log`).
- **Không dùng lại `EXP=p0` với `DURATION`, `WARMUP` hay workload khác mặc định**: `p0_baseline.sh` coi dòng đã có (theo backend, n, clients, rep) là đã xong. Chạy thử/khác mặc định thì dùng `EXP` khác (ví dụ `p0check`).
- `results/raw/p0.failures` chỉ ghi thêm (append-only), không xóa; một cấu hình thất bại không có dòng CSV nên lần chạy sau tự thử lại.
- Quy tắc nhiễu của kế hoạch: sau khi một cấu hình đủ `REPS` dòng, nếu CV = std mẫu / trung bình của tps > 10% thì chạy thêm tối đa 2 lần (rep `REPS+1`, `REPS+2`), dừng sớm khi CV <= 10%. `best_clients.py` lấy trung bình trên mọi rep có mặt; phần tổng kết in CV từng cấu hình và đánh dấu cấu hình vẫn > 10%.
- Phần (b) của một backend chỉ chạy khi đủ mọi dòng phần (a) (4 CLIENTS x `REPS` ở N=4) hoặc có `BEST_CLIENTS_*`; thiếu thì bỏ qua và tính là thiếu (exit 4), để CLIENTS tốt nhất không bị chọn từ dữ liệu thiếu.

## Sai lệch so với bài gốc

- **Dữ liệu 10k bản ghi thay vì 100k** (giá trị 1000 byte, vẫn 100k thao tác chạy lặp lại). Lý do: kafka-orig giữ cây SMT trong bộ nhớ, không cắt tỉa (~20 KB RSS mỗi lần ghi được áp dụng); với 100k bản ghi, nó vượt mọi giới hạn bộ nhớ mỗi node mà vẫn cho N=16 vừa trong Docker VM 7.65 GiB (OOM). Áp dụng như nhau cho mọi backend.
- **tm-orig: `consensus_params.block.max_bytes = 225000`** (và `evidence.max_bytes = 50000`, vì không được lớn hơn `block.max_bytes`). Lý do: ABCI socket client của Tendermint v0.35.0 từ chối khi hàng đợi 256 request đầy, mà DeliverTx gửi bất đồng bộ cho từng tx; khối > 256 tx làm đồng thuận dừng. Một tx của veritas-tm ~1 KB nên khối bị chặn ở khoảng 220 tx. Hệ quả: thông lượng đỉnh của tm-orig bị chặn ở ~220 tx/khối.
- **tm-orig: `[mempool] recheck = false`**. Lý do: mempool v1 re-CheckTx dưới tải gây hàng loạt lỗi "transaction mismatch" trên ABCI socket client.
- **tm-orig: giới hạn RPC `max-subscription-clients = 2000` và `max-open-connections = 2000`** (mặc định 100 và 900). Lý do: mỗi `Set` của veritas-tm là một `BroadcastTxCommit` giữ một subscription; mặc định 100 nhỏ hơn 256 client của thí nghiệm. Ngoài ra `max-packet-msg-payload-size = 4096`, `send-rate = recv-rate = 20000000`, `size = 100000`, `addr-book-strict = false`, `allow-duplicate-ip = true` theo `scripts/tendermint_config.py` của bài gốc.
- **tm-orig: `LOAD_THREADS = 64`** (kafka-orig giữ 10 như bài gốc). Lý do: mỗi `Set` phải chờ một khối (~3,5 s/khối, ~60-70 tx/s ở 0,5 CPU); nhiều luồng hơn chỉ làm backlog sâu thêm và làm `broadcast_tx_commit` quá 10 s (RPC EOF). Pha nạp không được đo.
- **Badger dùng bộ tùy chọn tiết kiệm bộ nhớ** (MemTableSize 16 MB, NumMemtables 2, NumLevelZeroTables 2, NumLevelZeroTablesStall 4, BlockCacheSize 32 MB, IndexCacheSize 16 MB, ValueLogFileSize 64 MB) cho mọi backend, thay cho `DefaultOptions`. Lý do: 16 node x 768m đã chiếm 12 GB trên máy 18 GB; mặc định của badger (memtable 64 MB x 5, block cache 256 MB) vượt giới hạn 768m. Hệ quả: thông lượng ghi sổ cái có thể thấp hơn bài gốc, như nhau cho mọi backend.
- **`GOMEMLIMIT` = 70% của `mem`** (768m -> 537 MiB) cho tiến trình veritas server của mọi backend (không áp cho tendermint). Lý do: hạn chế GC của Go vượt giới hạn container. Hệ quả: thêm CPU GC nếu heap sống gần giới hạn, thể hiện bằng tps thấp hơn.
- **Redis không lưu bền** (`--save '' --appendonly no`). Lý do: bài gốc dùng mặc định (snapshot), nhưng fork snapshot trong giới hạn 768m làm nhiễu phép đo; lưu bền không thuộc phạm vi thí nghiệm.
- **Pha nạp có timeout/retry và pha settle** trong `ycsbbench`: `--load-timeout` (20m), `--load-op-timeout` (30s mỗi lần thử, tối đa 3 lần thử mỗi bản ghi; ghi lại cùng khóa/giá trị là idempotent), mọi `Set` nạp lỗi thì lần chạy thất bại (rc=3); sau nạp, chờ đến khi mọi node đọc được 200 khóa mẫu (`--settle-timeout`, 10m) rồi mới vào pha chạy. Lý do: bản gốc có thể treo mãi hoặc đo trên replica kafka-orig (bất đồng bộ) còn chậm so với pha nạp. Pha nạp và settle không nằm trong khoảng đo.
- **Kiểm tra container sau mỗi lần chạy**: container nào chết/OOM thì không ghi dòng CSV (rc=3). Cột `host_cpu_max_pct` = max theo các mẫu (~1–1,5 s một mẫu) của tổng CPU% container / (100 x số CPU của Docker VM) x 100; `saturated` = `host_cpu_max_pct > 90`. `net_bytes_per_block` để trống (backend gốc không công bố số khối).
- **Client Kafka: `confluent-kafka-go/v2` v2.1.1** thay cho v1.7.0. Lý do: v1.7.0 (và v1.9.2) chỉ đóng gói librdkafka x86-64, không link được trên arm64; v2.1.1 đóng gói linux/arm64 (glibc), cùng API `kafka`, vẫn là librdkafka như bài gốc.
- **Giữ nguyên phiên bản**: Kafka 2.7.0 (kèm ZooKeeper), Tendermint v0.35.0, Redis 6.2 (ảnh `redis:6.2-bookworm` cho arm64 thay cho bullseye vì kho bảo mật Debian 11 đã ngừng).
- **Chế độ commit theo bản gốc**: kafka-orig `async`, tm-orig `sync`; `p0_baseline.sh` không truyền `COMMIT_MODE`.
- **Backend `kafka` mới: lỗi khi kiểm tra (validate) thì bỏ phiếu Abort.** Lỗi đọc redis hay lỗi giải mã khối trong lúc validate làm node bỏ phiếu `Abort`, nên khối bị loại trên toàn cụm (client `sync` nhận `ErrAborted`). Bản gốc (`veritas/server.go`) gọi `log.Fatalf` (node dừng hẳn).
- **Backend `raft` mới (hashicorp/raft v1.3.2 + raft-boltdb, như `raftkv/`): cụm tĩnh, node1 bootstrap toàn bộ peer (không `Join`); follower chuyển tiếp khối tới leader qua HTTP (`POST /submit`, cổng 7101; raft dùng cổng 7100); `Submit` trả về khi leader đã gọi `raft.Apply` (được chấp nhận, chưa commit), khối có thể mất khi đổi leader (client `sync` hết thời gian chờ) hoặc trùng khi thử lại; tắt snapshot và nén log (`SnapshotThreshold` = `TrailingLogs` = 2^30) để follower chậm luôn bắt kịp từ log; snapshot FSM rỗng (trạng thái nằm ở redis + sổ cái), nếu raft vẫn phải khôi phục snapshot thì `Restore` ghi log cảnh báo rõ là các khối trong snapshot bị bỏ qua trên node đó.** Test `-race` dùng store trong bộ nhớ vì boltdb/bolt v1.3.1 lỗi checkptr; test với bolt chỉ chạy khi không `-race`. Lý do: `raftkv` gốc chờ commit trong `Set` và dùng `Join` qua gRPC, không hợp với giao diện `consensus.Consensus` chung.
- **Backend `tendermint` mới (Tendermint v0.35.0, ABCI socket như `veritastm/`, `--version-mode=ordered`, không TSO): một tx ABCI là cả một khối Veritas đã marshal, nên cấu hình khác tm-orig: `mempool.max-tx-bytes = 4194304`, `rpc.max-body-bytes = 8388608` (tx đi qua `broadcast_tx_sync` dạng base64), genesis `block.max_bytes = 5242880` (thay vì 225000 của tm-orig, để chứa được một tx lớn nhất) và `evidence.max_bytes = 50000`; `Submit` trả về khi CheckTx đã nhận (`BroadcastTxSync`), không chờ commit; `Commit` trả app hash rỗng trên mọi node (trạng thái nằm ở redis + sổ cái).** Rủi ro còn lại: giới hạn 256 yêu cầu async của socket ABCI client vẫn áp dụng cho mỗi block TM, chỉ có thể xảy ra khi có rất nhiều khối Veritas nhỏ (BLOCK_SIZE=10) dồn trong mempool lúc `DeliverTx` bị chặn vì kênh Deliver đầy — cần theo dõi lỗi "buffer is full" ở E3. Lý do: bài gốc gửi mỗi Set là một tx (`BroadcastTxCommit`), không có batcher chung.
- **Số dòng kỳ vọng**: kế hoạch ghi 2 x (4+3) x 3 = 42 dòng, nhưng điểm N=4 ở CLIENTS tốt nhất của phần (b) trùng với một dòng phần (a); CSV không có cột "phần" nên dòng phần (a) được dùng lại, không chạy lại. Vậy có 2 x (4+2) x 3 = **36 dòng** riêng biệt (42 chỉ khi dùng `BEST_CLIENTS_*` ngoài {4,16,64,256}). Cộng thêm tối đa 2 dòng cho mỗi cấu hình vượt ngưỡng nhiễu (CV > 10%). Lưu ý: bước kiểm tra đủ dữ liệu trước phần (b) chỉ đòi đủ REPS lần chạy cơ bản; nếu một lần chạy thêm (rep 4–5) của phần (a) bị thiếu, CLIENTS tốt nhất có thể đổi khi chạy bù — khi đó xóa các dòng N=8/16 ứng với CLIENTS cũ trước khi chốt `results/p0-baseline.csv`.

## Chẩn đoán CPU của tm-orig

tm-orig, N=4, CLIENTS=16, `DURATION=30s`, thay đổi `CPUS` mỗi node:

| CPUS | tps | khoảng cách khối trung bình (s) |
|---|---|---|
| 0.5 | 20,13 | 1,36 |
| 1 | 20,20 | 1,34 |
| 2 | 19,83 | 1,37 |

CPU của host <= 6%. Kết luận: tm-orig không bị giới hạn bởi CPU mà bởi độ trễ vòng kín (closed-loop): mỗi client gọi `Set` = `broadcast_tx_commit` và phải chờ khối chứa tx (~0,8 s trung bình), nên tps ~ CLIENTS / độ trễ (16 / 0,8 ~ 20). (Con số ~33 tx/khối trong log chẩn đoán lấy trung bình 60 khối cuối, dài hơn khoảng đo 30 s nên lẫn cả pha nạp 64 luồng; không dùng để suy ra thông lượng.) kafka-orig trả lời (ack) bất đồng bộ ngay khi tx vào Kafka, không chờ áp dụng, nên cùng số client cho tps cao hơn nhiều; đây là nguyên nhân chính của chênh lệch kafka-orig / tm-orig ở CLIENTS thấp. Giữ `CPUS=0.5` cho mọi backend.

## Kết quả P0

Trung bình mọi lần lặp có mặt (độ lệch chuẩn mẫu trong ngoặc; 3 lần, hoặc 5 lần với cấu hình chạy thêm vì CV > 10%). Dữ liệu thô `results/raw/p0.csv` (34 dòng, commit `3a1ade8`), bản chốt `results/p0-baseline.csv`. Không có lần chạy thất bại (không có `results/raw/p0.failures`). `errors` là tổng qua các lần lặp; `saturated` là max.

### Điều kiện host (cần nêu khi trích dẫn số liệu)

- **Lần chạy P0 đầu tiên bị loại**: host quá tải (nhiều ứng dụng khác chạy song song) nên số liệu không tin cậy; dữ liệu được đổi tên thành `results/raw/p0-hostoverload{.csv,/,.failures}` (giữ lại, không xóa, không dùng).
- **Lần chạy được dùng** (2026-10-29, 18:29Z–20:19Z): người dùng chọn chạy dù máy chưa rảnh hẳn (Teams, Chrome, DBeaver, IDE... vẫn mở). Trạng thái host được ghi mỗi lần chạy ở `results/raw/p0/host.log` (đầu/cuối mỗi lần chạy, 69 mẫu): load1 1,3–14,9 (trung bình 7,2); RAM trống 66–2452 MB (trung bình ~128 MB); bộ nén bộ nhớ (compressor) 3,0–5,5 GB; bộ đếm swapout **không tăng** trong suốt batch (máy nén RAM nhưng không swap ra đĩa).
- Ảnh hưởng: tm-orig rất ổn định (CV <= 1,4%) vì bị giới hạn bởi độ trễ, không bởi CPU. kafka-orig nhạy với tải host hơn: c4 (CV 14,9%) và c64 (CV 12,3%) vẫn vượt ngưỡng 10% sau 2 lần chạy thêm; thứ hạng CLIENTS không đổi (c256 cao hơn c64 ~70%), nên CLIENTS tốt nhất không bị ảnh hưởng. Số tuyệt đối của kafka-orig nên coi là cận dưới; các thí nghiệm so sánh sau (P1+) phải chạy trong cùng điều kiện ghi `host.log` để so sánh.

Phần (a): N=4, thay đổi CLIENTS.

| backend | CLIENTS | lần lặp | tps | lat_mean (ms) | lat_p99 (ms) | errors | saturated |
|---|---|---|---|---|---|---|---|
| kafka-orig | 4 | 5 | 1.956,7 (291,7) | 2,1 (0,3) | 78,6 (5,2) | 0 | 0 |
| kafka-orig | 16 | 3 | 4.941,9 (464,3) | 3,3 (0,3) | 83,0 (1,4) | 0 | 0 |
| kafka-orig | 64 | 5 | 7.446,0 (914,1) | 8,7 (1,3) | 92,3 (0,6) | 0 | 0 |
| kafka-orig | 256 | 3 | 12.790,5 (1.143,2) | 20,1 (1,9) | 108,7 (3,4) | 0 | 0 |
| tm-orig | 4 | 3 | 5,3 (0,1) | 751,4 (10,4) | 2.400,1 (44,7) | 0 | 0 |
| tm-orig | 16 | 3 | 21,4 (0,2) | 747,4 (6,9) | 2.528,8 (31,1) | 0 | 0 |
| tm-orig | 64 | 3 | 85,8 (0,7) | 745,2 (3,9) | 2.462,7 (8,4) | 0 | 0 |
| tm-orig | 256 | 3 | 322,5 (3,7) | 790,6 (9,9) | 2.710,2 (28,7) | 0 | 0 |

CLIENTS tốt nhất (tps trung bình cao nhất tại N=4; hòa thì chọn nhỏ hơn): kafka-orig = 256, tm-orig = 256 (cả hai đều là mức CLIENTS lớn nhất được đo; chưa thấy điểm bão hòa).

Phần (b): CLIENTS tốt nhất (256), thay đổi N.

| backend | N | lần lặp | tps | lat_mean (ms) | lat_p99 (ms) | errors | saturated |
|---|---|---|---|---|---|---|---|
| kafka-orig | 4 | 3 | 12.790,5 (1.143,2) | 20,1 (1,9) | 108,7 (3,4) | 0 | 0 |
| kafka-orig | 8 | 3 | 15.406,6 (764,4) | 16,5 (0,8) | 101,7 (0,4) | 0 | 0 |
| kafka-orig | 16 | — | không chạy (RAM) | | | | |
| tm-orig | 4 | 3 | 322,5 (3,7) | 790,6 (9,9) | 2.710,2 (28,7) | 0 | 0 |
| tm-orig | 8 | 3 | 314,5 (2,5) | 813,6 (6,5) | 2.788,1 (27,7) | 0 | 0 |
| tm-orig | 16 | — | không chạy (RAM) | | | | |

Nhận xét:

- kafka-orig cao hơn tm-orig khoảng **40 lần** tại N=4, c256 (12.790 / 322,5), lớn hơn mức ~10 lần dự kiến. Nguyên nhân (xem "Chẩn đoán CPU"): kafka-orig trả lời ngay khi tx vào Kafka (async), còn tm-orig chờ khối chứa tx (sync, ~0,75–0,8 s). tm-orig tăng gần tuyến tính theo CLIENTS (tps ~ CLIENTS / 0,79 s), đúng mô hình vòng kín bị giới hạn bởi độ trễ.
- tm-orig giảm nhẹ khi N tăng: N=4 -> 8 giảm 2,5% tps, độ trễ trung bình tăng 2,9% (thêm validator làm vòng đồng thuận chậm hơn một chút). Xu hướng đúng như dự kiến nhưng nhỏ ở N <= 8.
- kafka-orig **tăng** 20% khi N=4 -> 8. Lý do: client chia đều cho các node, mỗi node có 0,5 CPU riêng, và đường ghi của kafka-orig không cần node khác xác nhận (một broker Kafka chung), nên thêm node là thêm năng lực xử lý; CPU host tối đa tăng từ 40% lên 59%. Đây là đặc điểm của thiết kế gốc, không phải lỗi đo.
- Không cấu hình nào có `saturated = 1` hay `errors > 0`.

## Quyết định về N tối đa

Ruling: **N <= 8 trên máy này** cho các thí nghiệm sau (thay cho N <= 16 / N <= 13 trong kế hoạch). Lý do: không phải bão hòa CPU (`saturated = 0` ở mọi điểm, CPU host <= 59%) mà là RAM: 16 node kafka-orig cần ~11 GB, trong khi Docker VM chỉ có 7,65 GiB (`mem 768m` x 16 node + Kafka/ZooKeeper). Muốn đo N=16 cần nâng RAM Docker VM lên >= 12 GiB rồi chạy `EXP=p0 PART=b NS_B="16" experiments/exp/p0_baseline.sh` (các dòng N=4/8 đã có sẽ được bỏ qua).

## Kiểm tra P1 (core + backend `kafka` mới, commit `75b15cd`)

- **Nhất quán**: `experiments/check_consistency.sh kafka` (N=4, sync, 16 client, 30 s) -> `CONSISTENT` (root digest giống nhau trên 4 node). 1 request `Aborted` (version cũ), được tính vào `errors`, không vào tps.
- **Hồi quy (theo kế hoạch: async, N=4, YCSB-A, block 100, 3 lần; `results/raw/p1reg.csv`)**:

| CLIENTS | kafka mới (async) | kafka-orig P0 | chênh |
|---|---|---|---|
| 64 | 16.957,5 (832,5) | 7.446,0 (914,1) | +128% |
| 256 | 20.590,0 (802,1) | 12.790,5 (1.143,2) | +61% |

  **Vượt ngưỡng ±10%** (theo chiều tăng). Nguyên nhân (chẩn đoán bằng `Breakdown`, `results/raw/p1reg/breakdown/`): ở chế độ async cả hai hệ thống trả lời client ngay khi tx vào hàng đợi, không có cơ chế chặn (backpressure): kafka-orig đẩy vào kênh đệm 10.000 phần tử rồi `Produce` bất đồng bộ; core mới trả lời khi tx vào batcher. Vì vậy tps async chỉ đo tốc độ nhận request của front-end (gRPC + hàng đợi), không đo tốc độ commit; đường `Set` của core mới không khóa theo key như bản gốc (`s.locker.Lock(key)`), đây là lý do có thể khiến nó nhận nhanh hơn (chưa đo riêng). Bằng chứng: ở async c256 mỗi node flush ~1.850–2.060 block nhưng toàn cụm chỉ apply 295 block trong cả lần chạy (consensus trung bình 8–25 s/block do tồn đọng, 105 block của node1 bị abort). Phần lớn request được "ack" ở async chưa bao giờ được commit trước khi cụm dừng.
- **Đo bổ sung ở chế độ sync** (chế độ dùng cho E1–E6; `results/raw/p1diag.csv`, 1 lần mỗi mức):

| CLIENTS | tps | lat_mean (ms) | lat_p99 (ms) | errors | consensus/block | execute+ledger/block | batch/block |
|---|---|---|---|---|---|---|---|
| 64 | 126,9 | 504,2 | 1.013,9 | 0 | 30–46 ms | 11–13 ms | 426–966 ms |
| 256 | 507,6 | 499,9 | 1.232,0 | 160 (Aborted) | 29–46 ms | 25–28 ms | 406–922 ms |

  Ở sync không có tồn đọng (consensus 30–46 ms/block, gần như không block bị drop). Thông lượng bị chặn bởi **bộ hẹn giờ flush 1 s của batcher** (giữ như code gốc): mỗi node chỉ có ~CLIENTS/N/2 lệnh `Set` đang chờ (YCSB-A có 50% đọc), ít hơn kích thước block 100, nên block chỉ được gửi khi hết 1 s; tps ~ CLIENTS / 0,5 s. Giống tm-orig, đây là chế độ bị giới hạn bởi độ trễ.
- **Quyết định (Ruling)**: tiêu chí ±10% ở async không đo được điều nó định kiểm tra (tính đúng của phần tái cấu trúc), vì cả hai bên đều không chặn ở async. P1 được chấp nhận dựa trên: kiểm tra nhất quán `CONSISTENT`, test đơn vị/race của core và backend, và phép đo sync ở trên (đường commit không tồn đọng). Kết quả async được ghi lại như một khác biệt đã giải thích, không dùng trong các so sánh sau. Cần chú ý cho E1: với block 100 và flush 1 s, các backend ở sync sẽ bị giới hạn bởi bộ hẹn giờ trừ khi số client đủ lớn để làm đầy block (>= ~2 x 100 x N); khác biệt giữa các backend chỉ hiện rõ khi thời gian consensus đáng kể so với thời gian chờ batch.
- Từ commit này, `run_one.sh` chuyển file breakdown của mỗi lần chạy vào `results/raw/<EXP>/<tag>.breakdown/` (trước đây file nằm ở `results/raw/` và bị lần chạy sau ghi đè).

## Mốc HotStuff (Task 12): `BFT2=hotstuff`

Spike (code bỏ đi, nhánh `spike/hotstuff`, commit `f625b23`; script `spike/hotstuff/run_spike.sh`) với `github.com/relab/hotstuff` v0.5.0 (commit `48e4d89`), chạy 2026-10-29T20:49Z:

- (a) **Build trên arm64: đạt.** `go build ./cmd/hotstuff` trong `golang:1.23-bookworm` (linux/arm64, go1.23.12). Lưu ý: `go.mod` của thư viện đòi Go >= 1.23.4, ảnh `hbdb-build` hiện là golang:1.20.
- (b) **4 replica commit >= 1000 lệnh trong 60 s: đạt.** `hotstuff run --replicas 4 --clients 4 --duration 60s --batch-size 100 --payload-size 0`: mỗi replica commit 3.685.033 lệnh (~61.400 lệnh/s; cả 4 replica bằng nhau), client báo ~920 nghìn lệnh thực thi mỗi client, 67–83 lệnh lỗi mỗi client. Bộ đọc awk của script báo nhầm 0 (sai tên trường `Commands`/`Event.ID`); số trên đếm lại từ `measurements.json` bằng Python. Đây là chạy trong một tiến trình, không payload, không giới hạn CPU: chỉ dùng cho mốc quyết định, không so sánh với các backend.
- (c) **API Go để nhúng: đạt.** Gói `modules`: `CommandQueue.Get(ctx) (hotstuff.Command, bool)` để đưa lệnh vào; `Executor.Exec(hotstuff.Command)` / `ExecutorExt.Exec(*hotstuff.Block)` nhận lệnh đã commit theo thứ tự; ghép qua `modules.NewBuilder(id, pk).Add(...)`. Rủi ro cho Task 13: `replica.New` tự gắn client server của nó làm queue/executor nên adapter phải tự dựng replica từ `Builder`; leader có hàng đợi rỗng sẽ chờ trong `Get` (cần lệnh rỗng/timeout); bộ đệm lệnh mặc định chờ đủ batch.

Quyết định (Ruling): **`BFT2=hotstuff`** (đạt cả 3 tiêu chí). Hệ quả cho Task 13: cần Go >= 1.23.4, nên hoặc nâng `hbdb-build` lên golang:1.23 và chỉ thị `go` trong `go.mod` (phải chạy lại toàn bộ test và kiểm tra tm-orig/kafka-orig vẫn build), hoặc tách adapter bft2 thành module/binary riêng. Fallback nếu tích hợp thất bại: CometBFT v0.38 (xem `spike/hotstuff/ASSESSMENT.md`).
