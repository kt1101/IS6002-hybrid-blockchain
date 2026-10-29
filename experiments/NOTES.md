# Ghi chú thí nghiệm P0 (baseline: kafka-orig, tm-orig)

## Cấu hình máy

- MacBook M3 Pro (arm64), 18 GB RAM.
- Docker VM: 11 CPU (đã xác nhận bằng `docker info`), 7.65 GiB RAM.
- Docker VM sau khi người dùng nâng RAM: _(chưa điền: ghi CPU / RAM thực tế dùng cho các lần chạy N=8, N=16)_.
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

Trung bình 3 lần lặp (độ lệch chuẩn trong ngoặc). _(chưa điền: sau khi chạy `experiments/exp/p0_baseline.sh`; dữ liệu thô `results/raw/p0.csv`, thất bại ghi ở `results/raw/p0.failures`)_

Phần (a): N=4, thay đổi CLIENTS.

| backend | CLIENTS | tps | lat_mean (ms) | lat_p99 (ms) | errors | saturated |
|---|---|---|---|---|---|---|
| kafka-orig | 4 | | | | | |
| kafka-orig | 16 | | | | | |
| kafka-orig | 64 | | | | | |
| kafka-orig | 256 | | | | | |
| tm-orig | 4 | | | | | |
| tm-orig | 16 | | | | | |
| tm-orig | 64 | | | | | |
| tm-orig | 256 | | | | | |

CLIENTS tốt nhất (tps trung bình cao nhất tại N=4; hòa thì chọn nhỏ hơn): kafka-orig = _?_, tm-orig = _?_.

Phần (b): CLIENTS tốt nhất, thay đổi N.

| backend | N | tps | lat_mean (ms) | lat_p99 (ms) | errors | saturated |
|---|---|---|---|---|---|---|
| kafka-orig | 4 | | | | | |
| kafka-orig | 8 | | | | | |
| kafka-orig | 16 | | | | | |
| tm-orig | 4 | | | | | |
| tm-orig | 8 | | | | | |
| tm-orig | 16 | | | | | |

Xu hướng cần kiểm tra: kafka-orig cao hơn tm-orig khoảng 10 lần tại N=4; tm-orig giảm khi N tăng. _(chưa điền: nhận xét sau khi có số liệu)_

## Quyết định về N tối đa

_(chưa điền: nếu N=16 có `saturated = 1`, ghi quyết định giới hạn N <= 13 cho các thí nghiệm sau; nếu không, giữ N <= 16.)_
