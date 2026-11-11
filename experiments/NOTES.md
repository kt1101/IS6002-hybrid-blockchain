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
- **Cây SMT của sổ cái được cắt tỉa** (`merkletree.NewPrunedSparseMerkleTree` trong `ledger.NewLedger`, nên áp dụng cho mọi backend kể cả kafka-orig/tm-orig; P0 đã đo trước thay đổi này): mỗi lần cập nhật/xóa một khóa, lá cũ, giá trị cũ và các nút trên đường đi cũ bị thay thế được xóa khỏi bộ nhớ (đếm tham chiếu theo hash, vì nhiều khóa có thể cùng giá trị); digest gốc và bằng chứng của gốc hiện tại không đổi, chỉ gốc cũ không còn đọc được (không chỗ nào đọc gốc cũ). Lý do: cây gốc không bao giờ xóa nút nên bộ nhớ tăng theo số lần ghi chứ không theo số khóa; pilot không lỗi (768m, GOMEMLIMIT 537 MiB, 256 client, 180 s): tendermint RSS node1 98→572 MiB và thông lượng mỗi 10 s 2602→1325, bft2 226→694 MiB, raft (flush 100 ms) 147→~600 MiB và thông lượng 12559→359, nên E6 (190 s) sẽ đo sự suy giảm này thay vì lỗi.
- **tm-orig: `consensus_params.block.max_bytes = 225000`** (và `evidence.max_bytes = 50000`, vì không được lớn hơn `block.max_bytes`). Lý do: ABCI socket client của Tendermint v0.35.0 từ chối khi hàng đợi 256 request đầy, mà DeliverTx gửi bất đồng bộ cho từng tx; khối > 256 tx làm đồng thuận dừng. Một tx của veritas-tm ~1 KB nên khối bị chặn ở khoảng 220 tx. Hệ quả: thông lượng đỉnh của tm-orig bị chặn ở ~220 tx/khối.
- **tm-orig: `[mempool] recheck = false`**. Lý do: mempool v1 re-CheckTx dưới tải gây hàng loạt lỗi "transaction mismatch" trên ABCI socket client.
- **tm-orig: giới hạn RPC `max-subscription-clients = 2000` và `max-open-connections = 2000`** (mặc định 100 và 900). Lý do: mỗi `Set` của veritas-tm là một `BroadcastTxCommit` giữ một subscription; mặc định 100 nhỏ hơn 256 client của thí nghiệm. Ngoài ra `max-packet-msg-payload-size = 4096`, `send-rate = recv-rate = 20000000`, `size = 100000`, `addr-book-strict = false`, `allow-duplicate-ip = true` theo `scripts/tendermint_config.py` của bài gốc.
- **tm-orig: `LOAD_THREADS = 64`** (kafka-orig giữ 10 như bài gốc). Lý do: mỗi `Set` phải chờ một khối (~3,5 s/khối, ~60-70 tx/s ở 0,5 CPU); nhiều luồng hơn chỉ làm backlog sâu thêm và làm `broadcast_tx_commit` quá 10 s (RPC EOF). Pha nạp không được đo.
- **Badger dùng bộ tùy chọn tiết kiệm bộ nhớ** (MemTableSize 16 MB, NumMemtables 2, NumLevelZeroTables 2, NumLevelZeroTablesStall 4, BlockCacheSize 32 MB, IndexCacheSize 16 MB, ValueLogFileSize 64 MB) cho mọi backend, thay cho `DefaultOptions`. Lý do: 16 node x 768m đã chiếm 12 GB trên máy 18 GB; mặc định của badger (memtable 64 MB x 5, block cache 256 MB) vượt giới hạn 768m. Hệ quả: thông lượng ghi sổ cái có thể thấp hơn bài gốc, như nhau cho mọi backend.
- **`GOMEMLIMIT` = 70% của `mem`** (768m -> 537 MiB) cho tiến trình veritas server của mọi backend (không áp cho tendermint). Lý do: hạn chế GC của Go vượt giới hạn container. Hệ quả: thêm CPU GC nếu heap sống gần giới hạn, thể hiện bằng tps thấp hơn.
- **Redis không lưu bền** (`--save '' --appendonly no`). Lý do: bài gốc dùng mặc định (snapshot), nhưng fork snapshot trong giới hạn 768m làm nhiễu phép đo; lưu bền không thuộc phạm vi thí nghiệm.
- **Pha nạp có timeout/retry và pha settle** trong `ycsbbench`: `--load-timeout` (20m), `--load-op-timeout` (30s mỗi lần thử, tối đa 3 lần thử mỗi bản ghi; ghi lại cùng khóa/giá trị là idempotent), mọi `Set` nạp lỗi thì lần chạy thất bại (rc=3); sau nạp, chờ đến khi mọi node đọc được 200 khóa mẫu (`--settle-timeout`, 10m) rồi mới vào pha chạy. Lý do: bản gốc có thể treo mãi hoặc đo trên replica kafka-orig (bất đồng bộ) còn chậm so với pha nạp. Pha nạp và settle không nằm trong khoảng đo.
- **Kiểm tra container sau mỗi lần chạy**: container nào chết/OOM thì không ghi dòng CSV (rc=3). Cột `host_cpu_max_pct` = max theo các mẫu (~1–1,5 s một mẫu) của tổng CPU% container / (100 x số CPU của Docker VM) x 100; `saturated` = `host_cpu_max_pct > 90`. `net_bytes_per_block` để trống với kafka-orig/tm-orig (backend gốc không công bố số khối); với các backend core (kafka, raft, tendermint, bft2) nó được điền từ đợt sửa sau rà soát cuối (xem mục cuối; dữ liệu P0/P1 trước đó đều để trống).
- **Client Kafka: `confluent-kafka-go/v2` v2.1.1** thay cho v1.7.0. Lý do: v1.7.0 (và v1.9.2) chỉ đóng gói librdkafka x86-64, không link được trên arm64; v2.1.1 đóng gói linux/arm64 (glibc), cùng API `kafka`, vẫn là librdkafka như bài gốc.
- **Giữ nguyên phiên bản**: Kafka 2.7.0 (kèm ZooKeeper), Tendermint v0.35.0, Redis 6.2 (ảnh `redis:6.2-bookworm` cho arm64 thay cho bullseye vì kho bảo mật Debian 11 đã ngừng).
- **Chế độ commit theo bản gốc**: kafka-orig `async`, tm-orig `sync`; `p0_baseline.sh` không truyền `COMMIT_MODE`.
- **Backend `kafka` mới: lỗi khi kiểm tra (validate) thì bỏ phiếu Abort.** Lỗi đọc redis hay lỗi giải mã khối trong lúc validate làm node bỏ phiếu `Abort`, nên khối bị loại trên toàn cụm (client `sync` nhận `ErrAborted`). Bản gốc (`veritas/server.go`) gọi `log.Fatalf` (node dừng hẳn).
- **Backend `raft` mới (hashicorp/raft v1.3.2 + raft-boltdb, như `raftkv/`): cụm tĩnh, node1 bootstrap toàn bộ peer (không `Join`); follower chuyển tiếp khối tới leader qua HTTP (`POST /submit`, cổng 7101; raft dùng cổng 7100); `Submit` trả về khi leader đã gọi `raft.Apply` (được chấp nhận, chưa commit), khối có thể mất khi đổi leader (client `sync` hết thời gian chờ) hoặc trùng khi thử lại; tắt snapshot và nén log (`SnapshotThreshold` = `TrailingLogs` = 2^30) để follower chậm luôn bắt kịp từ log; snapshot FSM rỗng (trạng thái nằm ở redis + sổ cái), nếu raft vẫn phải khôi phục snapshot thì `Restore` ghi log cảnh báo rõ là các khối trong snapshot bị bỏ qua trên node đó.** Test `-race` dùng store trong bộ nhớ vì boltdb/bolt v1.3.1 lỗi checkptr; test với bolt chỉ chạy khi không `-race`. Lý do: `raftkv` gốc chờ commit trong `Set` và dùng `Join` qua gRPC, không hợp với giao diện `consensus.Consensus` chung.
- **Backend `tendermint` mới (Tendermint v0.35.0, ABCI socket như `veritastm/`, `--version-mode=ordered`, không TSO): một tx ABCI là cả một khối Veritas đã marshal, nên cấu hình khác tm-orig: `mempool.max-tx-bytes = 4194304`, `mempool.max-txs-bytes = 134217728` (128 MiB, để mempool vừa giới hạn 768m cạnh GOMEMLIMIT 537 MiB của server), `rpc.max-body-bytes = 8388608` (tx đi qua `broadcast_tx_sync` dạng base64), genesis `block.max_bytes = 5242880` (thay vì 225000 của tm-orig, để chứa được một tx lớn nhất) và `evidence.max_bytes = 50000`; `block.max_gas` giữ mặc định của testnet như tm-orig; `Submit` trả về khi CheckTx đã nhận (`BroadcastTxSync`), không chờ commit; `Commit` trả app hash rỗng trên mọi node (trạng thái nằm ở redis + sổ cái).** Giới hạn số tx mỗi block TM: `execBlockOnProxyApp` gửi mọi `DeliverTx` của một block bằng `DeliverTxAsync` trong một vòng lặp chặt, nhanh hơn goroutine gửi duy nhất của socket ABCI client, mà hàng đợi chỉ có 256 chỗ (yêu cầu thứ 257 lỗi "buffer is full", đồng thuận dừng) bất kể app nhanh đến đâu; CheckTx cũng gửi async trên kết nối mempool, nên khi `DeliverTx` bị chặn vì kênh Deliver đầy, hơn 256 `broadcast_tx_sync` đồng thời sẽ lỗi "buffer is full" và hiện thành lỗi `Submit` (tính vào cột `errors`). Bất biến bảo vệ: ở chế độ `sync` mỗi khối Veritas chứa ít nhất một Set đang chờ, nên số tx trong mọi mempool ≤ số Set đang chờ ≤ CLIENTS (lúc chạy) hoặc LOAD_THREADS (lúc nạp); vì vậy `run_one.sh` và `check_consistency.sh` từ chối `BACKEND=tendermint` khi `COMMIT_MODE` khác `sync`, `CLIENTS > 256` hoặc `LOAD_THREADS > 256` (`hbdb_check_tendermint_limits` trong `lib.sh`; E1–E6 đều `sync` với CLIENTS ≤ 256). Đã thử giới hạn bằng gas (CheckTx `GasWanted = 1` + `block.max_gas = 200`) nhưng nó gây treo khoảng 30 s có thể lặp lại ngay khi bắt đầu nạp (các block TM rỗng, `broadcast_tx_sync` hết thời gian chờ), nên đã bỏ. Lý do: bài gốc gửi mỗi Set là một tx (`BroadcastTxCommit`), không có batcher chung.
- **Backend `bft2` mới (HotStuff, `relab/hotstuff` v0.5.0, chained HotStuff + ECDSA, `--version-mode=ordered`, không TSO): thư viện cần Go >= 1.23.4 nên chạy trong tiến trình riêng `bft2-node` (module Go riêng `bft2node/`, build bằng golang:1.23 qua `experiments/docker/Dockerfile.bft2`; module chính giữ nguyên `go.mod` và golang:1.20), một replica mỗi container, replica nói chuyện với nhau qua cổng 7200 (gorums/gRPC của thư viện, không TLS), khóa ECDSA suy ra tất định từ seed chung + ID replica (chỉ dùng cho testbed); veritas gọi API cục bộ 127.0.0.1:7201 (`POST /submit`, luồng `GET /deliver` khung có độ dài và số thứ tự); một lệnh HotStuff là đúng một khối Veritas (giữ nguyên byte); `Submit` trả về khi sidecar đã nhận vào hàng đợi (chưa commit); leader chỉ đề xuất khối của chính nó, một khối mỗi view, khi hàng đợi rỗng quá 20 ms thì đề xuất khối rỗng (không giao cho veritas) để chuỗi 3 khối của chained HotStuff vẫn commit được; khối của mình bị fork thì đưa lại đầu hàng đợi (hiếm khi có thể giao trùng); timeout view = max(ước lượng của thư viện trên các view thành công, 100 ms) × 1,2^k với k = số view hết giờ liên tiếp (về 0 khi một view thành công), tối đa 2 s — không chuyển timeout cho bộ ước lượng của thư viện vì nó nhân trung bình lên mãi; khi một replica chết, mỗi lượt leader của nó tốn 2 view hết giờ (view trước nó, vì phiếu bầu gửi tới leader đã chết, và view của chính nó) và một khối bị fork (được đề xuất lại); thứ tự leader là round robin nhưng hoán vị lại (tất định, theo seed chung) ở mỗi epoch N view, vì với round robin cố định replica đứng ngay trước một replica chết không bao giờ có QC cho khối của mình (phiếu bầu gửi tới leader kế tiếp đã chết); blockchain của thư viện không bao giờ xóa khối nên dùng bản sao xóa các view cũ nhất khi tổng khối giữ lại vượt 64 MiB (lệnh + ~0,5 KB mỗi khối; không bao giờ xóa 64 view gần nhất dưới chiều cao đã commit), tức vài trăm nghìn view khi chỉ có khối rỗng và ~650 view với khối Veritas 100 KB; replica tụt lại xa hơn thế không bắt kịp được: sau 50 lần "failed to commit" liên tiếp `bft2-node` thoát mã 3 nên container chết (lần chạy bị đánh dấu hỏng thay vì đo âm thầm một cụm nhỏ hơn); `bft2-node` có `GOMEMLIMIT` = 20% `mem` (768m -> 153 MiB; khối giữ lại 64 MiB + backlog commit 32 MiB + hàng đợi đề xuất 16 MiB + ~25 MiB runtime); F2 (im lặng mạng) cho bft2 chặn cổng 7200 (`hbdb_consensus_ports`); không hỗ trợ khởi động lại replica (không chuyển trạng thái).** Lý do: thư viện chỉ có API nhúng qua `modules.Builder` (không phải dịch vụ có sẵn), `replica.New` gắn sẵn client server riêng của nó.
- **RQ3, F2 trên `kafka` chỉ giữ lại phiếu bầu (`--fault=silent`), không chặn đề xuất:** khối do chính node im lặng đề xuất vẫn được tính là approve của nó và được các node còn lại duyệt, nên vẫn commit, còn khối của các node khác thì không bao giờ đủ phiếu; vì vậy F2-kafka còn khoảng 1/N thông lượng (các client gắn với node im lặng), khác với F2 im lặng mạng (iptables) của raft/tendermint/bft2 làm node im lặng hoàn toàn.
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

Trung bình mọi lần lặp có mặt (độ lệch chuẩn mẫu trong ngoặc; 3 lần, hoặc 5 lần với cấu hình chạy thêm vì CV > 10%). Dữ liệu thô `results/raw/p0.csv` (34 dòng, commit `8a54d00`), bản chốt `results/p0-baseline.csv`. Không có lần chạy thất bại (không có `results/raw/p0.failures`). `errors` là tổng qua các lần lặp; `saturated` là max.

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

## Kiểm tra P1 (core + backend `kafka` mới, commit `a5195b7`)

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

Spike (code bỏ đi, nhánh `spike/hotstuff`, commit `bcd8b17`; script `spike/hotstuff/run_spike.sh`) với `github.com/relab/hotstuff` v0.5.0 (commit `48e4d89`), chạy 2026-10-29T20:49Z:

- (a) **Build trên arm64: đạt.** `go build ./cmd/hotstuff` trong `golang:1.23-bookworm` (linux/arm64, go1.23.12). Lưu ý: `go.mod` của thư viện đòi Go >= 1.23.4, ảnh `hbdb-build` hiện là golang:1.20.
- (b) **4 replica commit >= 1000 lệnh trong 60 s: đạt.** `hotstuff run --replicas 4 --clients 4 --duration 60s --batch-size 100 --payload-size 0`: mỗi replica commit 3.685.033 lệnh (~61.400 lệnh/s; cả 4 replica bằng nhau), client báo ~920 nghìn lệnh thực thi mỗi client, 67–83 lệnh lỗi mỗi client. Bộ đọc awk của script báo nhầm 0 (sai tên trường `Commands`/`Event.ID`); số trên đếm lại từ `measurements.json` bằng Python. Đây là chạy trong một tiến trình, không payload, không giới hạn CPU: chỉ dùng cho mốc quyết định, không so sánh với các backend.
- (c) **API Go để nhúng: đạt.** Gói `modules`: `CommandQueue.Get(ctx) (hotstuff.Command, bool)` để đưa lệnh vào; `Executor.Exec(hotstuff.Command)` / `ExecutorExt.Exec(*hotstuff.Block)` nhận lệnh đã commit theo thứ tự; ghép qua `modules.NewBuilder(id, pk).Add(...)`. Rủi ro cho Task 13: `replica.New` tự gắn client server của nó làm queue/executor nên adapter phải tự dựng replica từ `Builder`; leader có hàng đợi rỗng sẽ chờ trong `Get` (cần lệnh rỗng/timeout); bộ đệm lệnh mặc định chờ đủ batch.

Quyết định (Ruling): **`BFT2=hotstuff`** (đạt cả 3 tiêu chí). Hệ quả cho Task 13: cần Go >= 1.23.4, nên hoặc nâng `hbdb-build` lên golang:1.23 và chỉ thị `go` trong `go.mod` (phải chạy lại toàn bộ test và kiểm tra tm-orig/kafka-orig vẫn build), hoặc tách adapter bft2 thành module/binary riêng. Fallback nếu tích hợp thất bại: CometBFT v0.38 (xem `spike/hotstuff/ASSESSMENT.md`).

## Driver E1–E6 (Task 17)

- Mã: `experiments/exp/e1_clients.sh` … `e6_faults.sh` (dùng chung `exp_lib.sh`, danh sách backend ở `backends.sh`: `kafka raft tendermint bft2`, backend chưa có trong `HBDB_IMPLEMENTED_BACKENDS` thì bỏ qua và ghi chú). Mọi driver: `COMMIT_MODE=sync`, REP=1..3, `--dry-run` (mỗi lần chạy một dòng `DRY_RUN:` trên stdout), có thể chạy tiếp (bỏ qua dòng CSV đã có, khóa gồm backend, n, f, clients, block_size, workload, rtt_ms, fault, rep), thử lại một lần khi `run_one.sh` trả rc 2/3, lỗi ghi vào `results/raw/<EXP>.failures`, quy tắc CV > 10% thêm tối đa 2 lần chạy như P0 (`cv.py` có thêm bộ lọc `cột=giá trị` để tách các cấu hình cùng backend/n/clients), tổng kết cuối cùng (exit 4 nếu thiếu dòng/thất bại/bỏ qua backend). Không dùng lại một `EXP` với `DURATION`, `WARMUP` hay `BATCH_FLUSH` khác.
- **BATCH_FLUSH**: khi block 100 ở chế độ sync, thông lượng ở CLIENTS nhỏ bị chặn bởi bộ hẹn giờ flush 1 s của batcher (mỗi node chỉ giữ ~CLIENTS/N/2 lệnh `Set` đang chờ, ít hơn 100), nên các đường cong E1 có thể giống nhau; núm `BATCH_FLUSH` (`run_one.sh` → `up.sh` → `gen_compose.py --batch-flush` → cờ `--batch-flush` của `cmd/veritas`, chỉ backend core) cho phép chạy một đợt quét tiếp theo. E1–E6 giữ mặc định 1 s (không truyền cờ); CSV vẫn 22 cột nên giá trị khác 1 s chỉ nằm trong tên tag (`_bf<giá trị>`) và log, và driver bắt buộc dùng `EXP` khác.
- **E2**: mặc định `NS="4 7"` (máy này tối đa N=8, xem "Quyết định về N tối đa"): 4 backend × 2 N × 3 = 24 lần chạy; trên máy lớn hơn dùng `NS="4 7 10 13 16"` (60 lần chạy). N=4 chạy lại trong `e2.csv`. Với `kafka`, `PRE_DOWN_HOOK=experiments/exp/kafka_ops.sh` đọc CURRENT-OFFSET/LOG-END-OFFSET (logic của `scripts/get_kafka_ops.sh`) trước `down.sh` và ghi `results/raw/e2-kafka-ops.csv`; số đếm tính trên cả vòng đời cụm (nạp + settle + warmup + đo), không chỉ cửa sổ đo.
- **CLIENTS của E2–E6** = CLIENTS tốt nhất của từng backend trong `results/raw/e1.csv` (`best_clients.py`, chỉ khi E1 đủ 4 mức × 3 lần của backend đó; thiếu thì bỏ qua backend, exit 4); ghi đè bằng `FIXED_CLIENTS` hoặc `BEST_CLIENTS_<BACKEND>`. Cột `f` của E1–E5 là 0.
- **E4**: `WORKLOAD` là `a`, `b`, `c` (tên file `results/ycsb/<w>.load.dat`), không phải `workloada`.
- **E6**: (N, f) ∈ {(4,1), (7,2)}, `DURATION=180s`, lỗi ở giây 60; F1–F4 lên f node cuối (`fault_default_nodes`), F5 chỉ cho tendermint/bft2 với f+1 node cuối, cả hai kiểu F5-F2 (mất liveness) và F5-F4 (mất an toàn): 20 cấu hình cho mỗi cặp (N, f), tức 40 cấu hình × 3 = 120 lần chạy (108 nếu `F5_KINDS="F5-F4"`); từ đợt sửa sau rà soát cuối thêm lỗi leader (F1/F3 lên node1 cho raft và kafka, `E6_LEADER=1`): 48 cấu hình × 3 = 144 lần chạy (132 nếu `F5_KINDS="F5-F4"`, 120 nếu `E6_LEADER=0`). Số dòng kỳ vọng: E1 48, E2 24 (60 với 5 mức N), E3 36, E4 36, E5 36, E6 144. Tps của lần chạy có lỗi là trung bình của phần lành và phần lỗi nên CV > 10% là thường gặp (`EXTRA_REPS=0` để tắt chạy thêm).
- **Quyết định về BATCH_FLUSH (người dùng chọn, 2026-10-31)**: đo thử raft N=4, 16 client, block 100, sync: flush 1 s → ~32 req/s, flush 100 ms → 314,5 req/s (`results/raw/bfcheck.csv`), tức ở 1 s thông lượng chủ yếu phản ánh bộ hẹn giờ chứ không phải giao thức đồng thuận. Chọn **"cả hai"**: E1–E6 chạy với 1 s như kế hoạch (so được với P0 và code gốc), và thêm một đợt E1 với 100 ms: `EXP=e1-bf100ms BATCH_FLUSH=100ms experiments/exp/e1_clients.sh` (thêm ~2 giờ). Khi viết chương thực nghiệm, so sánh giữa các backend ở CLIENTS nhỏ phải dựa vào đợt 100 ms.

## Sửa sau rà soát cuối (final review, 2026-10-31)

Thay đổi trong harness (CSV vẫn 22 cột; dữ liệu mới nằm ở file phụ cạnh `<tag>.timeline` trong `results/raw/<EXP>/`):

- **Tách đọc/ghi (C1)**: `<tag>.ops.csv` (`op,count,tps,lat_mean_ms,lat_p50_ms,lat_p99_ms,errors,aborted`, một dòng `read`, một dòng `write`, chỉ trong cửa sổ đo; `errors` = lỗi khác `Aborted`, `aborted` = lỗi gRPC `Aborted`, cột `errors` của CSV vẫn đếm cả hai) và `<tag>.wtimeline` (số Set thành công mỗi giây, cùng gốc t=0 với `.timeline`). Cột độ trễ của CSV vẫn trộn Get (đọc redis cục bộ ~1 ms) và Set (chờ khối) nên p50 có thể nhảy giữa hai mode; `plot.py` thay độ trễ của mỗi dòng có file `.ops.csv` bằng độ trễ dòng `write` (cột `lat_source` = write|csv) và vẽ thêm hình `<exp>_lat_vs_<x>.png` (p50, p99 của ghi).
- **Tài nguyên (C2)**: `<tag>.stats` ghi mọi mẫu `docker stats` của mọi container (giờ host, tên, CPU%, RAM, NetIO rx/tx tích lũy, kèm số byte; NetIO của docker chỉ có **3 chữ số có nghĩa**, mẫu cách nhau ~2 s, nên chỉ dùng cho tài nguyên). `net_bytes_per_block` (backend core, **chỉ FAULT=none**) = Δ(tổng bộ đếm chính xác `/sys/class/net/eth0/statistics/tx_bytes` của node1..nodeN, + broker Kafka với backend kafka) / số khối node1 áp dụng giữa hai snapshot breakdown; bộ đếm được đọc trong cùng lệnh `docker compose exec` gửi SIGUSR2 (snapshot ở đầu cửa sổ đo và ngay sau khi bench kết thúc, `breakdown-node<i>.snap1/2.json`; giá trị trong `<tag>.snaps`), nên byte và khối lấy cùng một thời điểm. Giá trị gồm mọi byte node (và broker) gửi trong cửa sổ: lưu lượng đồng thuận, cả các height rỗng của tendermint (`create-empty-blocks`) và view rỗng của bft2 (IdleWait ~20 ms), nên ở tải thấp bft2/tendermint bị đội lên (smoke c16: bft2 69,5 kB so với raft 27,3 kB), và cả phản hồi cho client (giá trị Get ~1 KB; ước lượng ngoại tuyến bằng rx của container client trong `.stats`); TSO và client không tính. Để trống khi FAULT != none (node bị dừng mất bộ đếm, cửa sổ trộn trước/sau lỗi). Bộ lấy mẫu dừng khi `run_one.sh` chết và nghỉ 0,5 s khi `docker stats` lỗi (M1).
- **Client bị mất (I1)**: `ycsbbench` cho mỗi worker nghỉ sau lỗi (50 ms, gấp đôi tới 1 s, về lại khi thành công; `--error-backoff`), nên client gắn với node chết không còn lặp lỗi hàng nghìn lần/s (ít tải TSO, `errors` nhỏ hơn nhiều so với các lần chạy cũ). `rq3.py`: với F1 và F2 mạng (F2 raft/tendermint/bft2, F5-F2) ngưỡng phục hồi = 0,9 × pre_mean × (N − k)/N, k = số node bị lỗi lấy ra khỏi cụm (client gắn tĩnh với node nên phần client của node chết luôn ~0 tps); báo `recovery_threshold_tps`, `threshold_share`, `faulty_nodes`, `n` (N lấy từ tag). F2 của kafka (chỉ giữ phiếu), F3, F4 giữ ngưỡng 0,9 × pre_mean.
- **Lỗi leader (I4)**: E6 thêm F1:node1@60 và F3:node1@60 cho raft và kafka (`E6_LEADER=1`, `E6_LEADER_BACKENDS="raft kafka"`): node1 bootstrap raft nên thực tế luôn là leader; kafka không có node leader (broker sắp thứ tự) nên đó là đối chứng. Với raft có lỗi, `<tag>.leaders` chứa mọi dòng `entering leader state` của log các node (timestamp docker) để biết ai là leader lúc lỗi. E6 = 144 lần chạy.
- **Chuyển tiếp khối của raft khi leader chết (N5)**: follower gửi khối tới leader qua HTTP; trước đây client HTTP chỉ có giới hạn tổng 15 s (`forwardTimeout`) và kết nối giữ sẵn (keep-alive) tới leader vừa chết bị nuốt gói, nên mỗi node treo ~15 s (smoke F1:node1@15: `stall_s=14`, write p99 16 s) dù leader mới đã có sau ~1,8 s. Nay mỗi lần chuyển tiếp có dial timeout 1 s và bị hủy ngay khi `raft.Leader()` không còn là đích (leader mới hoặc đang bầu), rồi thử lại tới leader mới, nên lỗi leader của raft chỉ tốn khoảng thời gian bầu (~2–3 s). Khối mà leader cũ có thể đã nhận vẫn có thể bị giao trùng (core chấp nhận). Dữ liệu E6 raft F1:node1 trước thay đổi này đo hằng số của adapter, không phải Raft.
- **RTT E5 (I5)**: netem RTT_MS/2 cũng đặt trên egress của broker Kafka (ảnh `hbdb-kafka` thêm iproute2, container có NET_ADMIN) và của TSO; node↔node và node↔broker đều RTT_MS, client↔node và client↔TSO RTT_MS/2 (TSO được coi là dịch vụ ở xa nửa WAN, client không bị trễ). Dữ liệu E5 trước thay đổi này không so được với sau. Mô hình: broker là một "site" riêng, nên một đề xuất kafka node→broker→node tốn 1 RTT và phiếu bầu thêm 1 RTT (~2×RTT mỗi commit), so với raft leader↔follower 1×RTT (+1×RTT khi follower chuyển tiếp).
- **Kiểm tra hợp lệ (I7)**, chỉ với FAULT=none, sai thì rc=3 (không ghi dòng, driver thử lại): lỗi ghi **không tính `Aborted`** > 1% tổng số ghi (`VALID_MAX_WRITE_ERR_PCT`; lỗi `Aborted` là kết quả theo thiết kế, kafka c256 ở P1 có ~1,04% Aborted, nếu tính vào thì kafka có thể bị loại khỏi E1 rồi E2–E6); ≥ 5 giây liên tiếp trong cửa sổ đo không có thao tác thành công (`VALID_ZERO_RUN_SECS=5`; brief đề nghị "bất kỳ giây 0 nào", nhưng P0 tm-orig c4 có một giây 0 ở cả 3 lần lặp hợp lệ, tendermint c16 có giây 0 rời rạc và một vòng đổi round của tendermint có thể > 3 s; dừng ≥ 30 s còn bị bắt bởi lỗi ghi hết thời gian; đặt 1 để theo nghĩa đen); log node có `FATAL apply`, `buffer is full`, `replica failed, exiting`/`can no longer commit`; với backend core chạy `auditor --once` sau 5 s (thử lại 2 lần khi DIVERGENT), DIVERGENT thì hỏng, không có kết luận thì chỉ cảnh báo; kết quả ở `<tag>.final_audit`.
- **Bộ nhớ bft2**: một container bft2 chạy cả veritas và sidecar `bft2-node`; trước đây veritas có `GOMEMLIMIT` = 70% `mem` (537 MiB) cộng 153 MiB của sidecar = 690 MiB trên 768m (redis chỉ còn ~78 MiB). Nay veritas của bft2 có `GOMEMLIMIT` = 70% × (`mem` − giới hạn của sidecar) = 430 MiB (768m; `bft2_veritas_gomemlimit` trong `gen_compose.py`), tổng 583 MiB; các backend khác giữ 537 MiB.
- **Không cho máy ngủ (I8)**: driver E1–E6 (không phải `--dry-run`) tự chạy lại dưới `caffeinate -dims` khi có lệnh này (macOS), `HBDB_CAFFEINATED=1` đánh dấu, `NO_CAFFEINATE=1` để tắt.
- **Thứ tự chạy (I9)**: rep là vòng ngoài cùng: mọi cấu hình chạy rep 1 rồi mới tới rep 2, rep chạy thêm (CV > 10%) sau cùng; khóa tiếp tục chạy không đổi.
- **Bảo vệ tham số và đĩa (M3/M4)**: `results/raw/<EXP>.params` ghi DURATION, WARMUP, CPUS, MEM, LOAD_THREADS, BATCH_FLUSH, commit ở lần chạy thật đầu tiên; lần sau khác giá trị thì driver dừng (`EXP_ALLOW_COMMIT_CHANGE=1` chỉ bỏ qua commit). Trước mỗi lần chạy cần ≥ 5 GiB trống trên đĩa chứa `results/raw` (`EXP_MIN_FREE_GB`), nếu không thì dừng batch (exit 1).

Chỉ ghi chú (chưa sửa):

- **I2**: breakdown khi SIGTERM là tích lũy cả vòng đời cụm, gồm pha nạp 10k bản ghi qua node1 (ở CLIENTS nhỏ pha nạp chiếm phần lớn số khối) nên trung bình từng pha lẫn nạp + chạy và node1 khác các node khác; số liệu "phân rã thời gian" của cửa sổ đo phải lấy hiệu `snap2 − snap1`; breakdown không đếm số SharedLog nên chưa suy ra được kích thước khối thực tế.
- **I3**: ở vòng kín CLIENTS ≤ 256 mỗi node chỉ có ~CLIENTS/(2N) ≈ 32 Set đang chờ, nên block 100 và 1000 không bao giờ đầy: hai điểm E3 đó đều là "những gì đang chờ khi bộ hẹn giờ flush", phải giống nhau; BLOCK_SIZE chỉ là cận trên. bft2 đề xuất đúng một khối Veritas mỗi view và chỉ từ hàng đợi của leader hiện tại, nên ở block 10 thông lượng ghi bị chặn ~10 × số view/s và mỗi Set chờ tới lượt leader của node mình (tới N view) + chuỗi 3 khối: đây là giới hạn của adapter, không phải của HotStuff; phải nêu cạnh kết quả bft2 ở E2/E3/E5.
- **I6**: broker Kafka có 2 CPU / 1,5 GB (TSO 0,5 CPU / 256 MB) ngoài giới hạn 0,5 CPU mỗi node, nên kafka đẩy việc sắp thứ tự sang một broker mạnh còn raft làm đồng thuận trong 0,5 CPU của node (ảnh hưởng RQ2); tendermint dùng `timeout-commit = 1s` (+ `create-empty-blocks`) còn bft2 nhịp view ~20 ms (IdleWait 20 ms, timeout view tối thiểu 100 ms), nên khoảng cách RQ1 giữa hai backend BFT phần lớn phản ánh nhịp cấu hình này.
- **M2**: cột `commit` là `git describe --dirty` của checkout, không kiểm tra ảnh Docker được build từ commit đó: **sau mọi thay đổi code phải chạy lại `experiments/build.sh`** trước khi chạy (đợt này đổi `ycsbbench`, `veritas`, ảnh `hbdb-kafka`; ảnh cũ thì `ycsbbench` từ chối cờ `--ops` và lần chạy hỏng rc=3).
- **M7/M8**: `errors` của kafka và raft không cùng ý nghĩa: ở kafka (VersionClient, approve-all như bản gốc) một SharedLog cũ làm cả khối bị Abort (tới 100 Set lỗi `Aborted`), ở raft chỉ SharedLog cũ đó bị hủy; ngoài ra batcher đảo thứ tự version TSO nên kafka/raft có lỗi `Aborted` mà tendermint/bft2 (version theo thứ tự đồng thuận) không thể có. Tách theo nguyên nhân: cột `aborted` của `<tag>.ops.csv` (lỗi gRPC `Aborted`) và `errors` (còn lại: DeadlineExceeded, Unavailable, ...); "Errors by message" trong `<tag>.bench.log` nay gộp được theo nguyên nhân vì `seq <n>` trong thông điệp được thay bằng `seq N` (trước đó mỗi thông điệp là duy nhất nên chỉ 20 lỗi đầu hiện riêng, còn lại vào "(other)").

## Chiến dịch đo E1–E6 và dữ liệu cuối (Task 19, 2026-10-31 → 2026-11-01)

Mọi dòng đo dùng commit **`e7baa60`** (cột `commit` của CSV ghi mã cũ `a0e9aa9`, xem "Mã commit sau khi đổi email"; ảnh Docker build lúc 2026-10-31T15:58Z từ commit đó). Các commit sau (`e27614b`, `4e15297`, `80fbd66` và commit kết quả) chỉ đổi `experiments/plot/`, `experiments/NOTES.md`, `results/` (không đổi mã đo).

### Trình tự

| Đợt | Thời gian (UTC) | Kết quả |
|---|---|---|
| Chiến dịch E1 → E1 100 ms → E2 → E6 (cổng lỗi ghi 1%) | 10-31 16:50 → 11-01 09:30 | đủ dòng cho kafka, raft, bft2; tendermint bị loại khỏi E2–E6 vì E1 c256 thiếu 1 lần (khối mất khi bão hòa, xem dưới) |
| Chạy bù tendermint (`VALID_MAX_WRITE_ERR_PCT=5`, driver chỉ chạy dòng thiếu) | 11-01 09:32 → 15:59 | không lỗi; tendermint có trong mọi thí nghiệm |
| Chạy lại 1 lần (3 rep mới) cho 18 cấu hình `plot.py` đánh dấu CV > 10%, vào `results/raw/<exp>-rerun.csv` | 11-01 16:01 → 19:03 | 54/54 dòng, không lỗi |
| Đo lại toàn bộ E1 100 ms (`EXP=e1-bf100ms-r2`, cổng 5%) | 11-01 19:06 → 21:43 | 64/64 dòng, không lỗi |

Điều kiện host: chiến dịch bắt đầu khi máy chỉ còn ~88 MB RAM trống, ~8 GB bộ nhớ nén, ~14 GiB đĩa trống (máy không "yên").

### Quy tắc dữ liệu cuối (`experiments/plot/assemble_final.py results/raw results/final`)

- E1–E6: các dòng chiến dịch (+ chạy bù). Với mỗi cấu hình đã chạy lại: CV của 3 lần chạy lại ≤ 10% thì **thay** các dòng chiến dịch bằng dòng chạy lại; ngược lại **gộp** (rep của lần chạy lại đánh số tiếp sau rep cuối). Quyết định từng cấu hình ghi trong `results/final/assembly.txt` (10 thay, 4 gộp; 4 cấu hình E1 100 ms còn lại thay bằng đợt đo lại toàn bộ).
- E1 100 ms = **chỉ** `e1-bf100ms-r2` (xem "Lệch mức" dưới).
- File phụ của từng lần chạy (`.ops.csv`, `.timeline`, `.wtimeline`, `.faults`, `.audit`) được chép sang `results/final/<exp>/` theo tag cuối (không commit: chạy lại script để tạo).
- Hình: `plot.py results/final/*.csv --out results/figs`; bảng RQ3: `rq3_table.py results/final/e6 --out results/figs` → `results/figs/rq3_summary.csv`.
- **Dừng chạy lại** ở 11 cấu hình vẫn CV > 10% sau hai đợt (`results/rerun.txt`; không điểm nào bị cờ host bão hòa): E1 100 ms c64 và c256 của kafka/raft/bft2 (CV 13–26%) và tendermint c256 (36%); tendermint E3 block 10 (16%), E4 workload b (12%), E6 F2 n=4 (13%); bft2 E6 F2 n=7 (15%). Phải báo cáo kèm độ lệch chuẩn và min–max.

### Lệch mức của E1 100 ms trong chiến dịch (mối đe dọa tính hợp lệ)

Cùng commit và ảnh, CPU host như nhau (~23%), nhưng E1 100 ms trong chiến dịch thấp hơn đợt đo lại ở mọi mức client (c4: kafka 71 → 78, bft2 56 → 64; c256: kafka 574 → 1115, raft 594 → 1150, bft2 723 → 1170 req/s). Timeline lần chạy chiến dịch bft2 c256 rơi xuống 50–190 req/s hàng chục giây trong khi CPU node ~10% (node chờ, không bị giới hạn); lần đo lại giữ ~1500–2400 req/s với node chạm 0,5 CPU. Nguyên nhân khả dĩ nhất là trạng thái host/VM (thiếu RAM) lúc đó; chưa chứng minh được vì không ghi bộ nhớ host theo thời gian. Tỷ lệ giây "lõm" của timeline không tách được hai trường hợp (đợt đo lại cũng có), nên không dùng làm bộ lọc. Các thí nghiệm flush 1 s bị trần ~510 req/s của bộ hẹn giờ nên ít nhạy, và các lần chạy lại của chúng khớp trong 10% (trừ các cấu hình đã thay). **Khuyến nghị:** ghi `vm_stat`/áp lực bộ nhớ host theo từng lần chạy nếu đo thêm.

### Kết quả chính (tps trung bình, dữ liệu cuối)

- **E1 flush 1 s**: kafka/raft/bft2 trùng nhau (c4 ≈ 7,5; c16 32; c64 127; c256 478–488) vì bộ hẹn giờ flush; tendermint 5 / 19 / 85 / 294.
- **E1 flush 100 ms** (dùng để so giữa backend, xem quyết định BATCH_FLUSH): c4 kafka 78, raft 76, bft2 64, tendermint 6; c16 295 / 279 / 237 / 24; c64 845 / 824 / 693 / 95; c256 1115 / 1150 / 1170 / 247. Ở tải thấp kafka ≈ raft > bft2 (~20%); ở c64–c256 ba backend chồng lấn (CV 13–26%), node ~42–47% CPU (gần trần 0,5) ở c256. Tendermint thấp hơn một bậc do `timeout-commit = 1s` (I6).
- **E2** (c256, N 4 → 7): kafka 460 → 451, raft 498 → 449, bft2 481 → 501 (giá trị chạy lại; chiến dịch 394 với CV 19%), tendermint 298 → 244.
- **E3** (block 10 / 100 / 1000): kafka 1457 / 504 / 510, raft 1644 / 510 / 508, bft2 1611 / 510 / 510, tendermint 279 / 298 / 278. Block 100 và 1000 bằng nhau đúng như I3 (flush 1 s); chỉ block 10 lấp đầy theo kích thước.
- **E4** (a / b / c): ~510 / ~5050 / ~11400–11700 với kafka, raft, bft2; tendermint 260 / 2322 / 11490. Đọc không đi qua đồng thuận nên workload c giống nhau.
- **E5** (RTT 0 / 20 / 60 ms): kafka 507 / 505 / 498, raft 511 / 508 / 506, bft2 508 / 507 / 471, tendermint 290 / 255 / 208. Flush 1 s che phần lớn RTT; tendermint và bft2 (nhiều vòng thông điệp mỗi khối) chịu RTT rõ nhất.

### Tendermint: mất khối khi bão hòa

Ở c256 đôi khi cả một khối Veritas (64 Set) hết thời gian 30 s (~1,4% lượt ghi), cổng 1% loại lần chạy. Nguyên nhân: kênh Deliver của core đầy → `DeliverTx` chờ trong khi giữ mutex của ABCI app → `CheckTx` bị chặn → `broadcast_tx_sync` hết 30 s. Đây là áp lực ngược của adapter khi bão hòa (đã biết từ review Task 11), không phải lỗi mới; vì vậy tendermint chạy bù với cổng 5% (lần treo thật vẫn bị loại; số lỗi nằm trong cột `errors`).

### RQ3: chỉ số trên timeline ghi

`rq3.py` (từ `80fbd66`) tính mọi chỉ số liveness/thông lượng trên `<tag>.wtimeline` (Set thành công mỗi giây) khi có (`timeline_source=write`, cả 157 lần chạy E6). Trước đó dùng timeline tổng: Get được phục vụ cục bộ nên vẫn thành công khi đồng thuận đã dừng (các đợt "nhú" mỗi 30 s giữa các Set hết thời gian), khiến bft2/tendermint F5-F2 bị báo "recovered". Vì vậy `pre_mean_tps`/`min_after_fault_tps` của `rq3_summary.csv` là **lượt ghi/s** (~255 với kafka/raft/bft2, ~130–157 với tendermint), không so trực tiếp với tps tổng của CSV. `stall_s` = số giây 0 liên tiếp đầu tiên sau lỗi (một giây 0 rời rạc vài giây sau cho `stall_s = 1`); cửa sổ sau lỗi là ~119 s. `detect_status = invalid` khi auditor không có kết luận (node bị dừng/cô lập không trả lời), không phải lỗi đo.

### Đối chiếu giả thuyết F1–F5 (spec mục 4) với E6

tps = tps tổng của CSV trên toàn lần chạy 180 s (60 s lành + 120 s lỗi); mốc không lỗi = E2 cùng N (kafka 460 / 451, raft 498 / 449, bft2 481 / 501, tendermint 298 / 244 ở N=4 / 7); phục hồi/dừng theo `rq3_summary.csv` (ghi).

| | Giả thuyết | Kết quả | Kết luận |
|---|---|---|---|
| F1 kafka | Dừng hẳn | not_recovered, ghi = 0 suốt 119 s, tps 171–173 (= phần trước lỗi), node1 hay node4 như nhau | **Đúng** |
| F1 raft | Chạy tiếp; bầu leader mới nếu leader chết | recovered; leader (node1) 2,6 s, follower 3,6 s (N=4); tps 413–442 | **Đúng** (bầu lại ~2–3 s sau sửa N5) |
| F1 tendermint / bft2 | Chạy tiếp; HotStuff đổi view nhanh hơn | cả hai recovered; phục hồi bft2 2,0 s / 5,3 s, tendermint 3,6 s / 5,0 s (N=4 / 7); tps bft2 384 / 305, tendermint 175 / 171 | Chạy tiếp: **đúng**. "HotStuff nhanh hơn": **chỉ ở N=4**, N=7 ngang nhau |
| F2 kafka | Dừng hẳn | N=7 (2 node im lặng): ghi = 0 suốt 119 s. N=4 (1 node): ghi còn đúng 64/s (= client của node im lặng): khối của node trung thực thiếu phiếu nên bị loại (Aborted), khối do chính node im lặng đề xuất vẫn đủ phiếu | **Đúng một phần**: node trung thực dừng, client của node lỗi vẫn ghi được |
| F2 raft | Chạy tiếp | recovered 8,3 s / 12,0 s, tps 421 / 405 | **Đúng** |
| F2 tendermint / bft2 | Chạy tiếp | recovered; tendermint dừng ghi 16,6 s (N=4), tps 102 / 101; bft2 6,0 s / 11,5 s, tps 377 / 264 | **Đúng**, tendermint chịu cô lập kém hơn nhiều |
| F3 kafka | Chậm theo node chậm nhất | tps 458–465 so với mốc 460 / 451 (N=7 hai node chậm: 432, −4%); dừng ghi ≤ 1 s | **Không thấy rõ** ở flush 1 s (200 ms < 1 s flush) |
| F3 raft | Chậm nếu node chậm là leader | leader 479 / 475 so với follower 482 / 468 (mốc 498 / 449): không khác | **Không thấy** ở flush 1 s (200 ms < 1 s flush) |
| F3 tendermint / bft2 | Chậm theo timeout round/view | tendermint 236 / 225 (−20% / −8%), bft2 462 / 411 (−4% / −18%) | **Đúng** (chậm, không dừng) |
| F4 (mọi backend) | Chạy tiếp; chỉ auditor phát hiện | recovered, tps 484–499 (tendermint 259–295), auditor phát hiện sau 0,7–1,5 s ở mọi lần chạy; client đọc từ node lỗi nhận giá trị sai (`wrong_gets` 2–5 nghìn mỗi lần chạy) | Chạy tiếp + auditor phát hiện: **đúng**; nhưng không chỉ auditor: client của node lỗi đọc sai |
| F5-F2 (BFT, f+1 node) | Mất liveness | bft2 và tendermint, N=4 và 7: not_recovered, ghi = 0 suốt ~119 s | **Đúng** (mất liveness) |
| F5-F4 (BFT, f+1 node) | Mất tính an toàn | đồng thuận không bị ảnh hưởng (tps 486–498 / 262–306), f+1 node có trạng thái sai, auditor phát hiện sau 0,9–1,4 s, `wrong_gets` 3–10 nghìn | **Đúng một phần**: an toàn ở tầng trạng thái/đọc bị vi phạm (và được phát hiện), thứ tự đồng thuận không bị ảnh hưởng vì F4 làm hỏng lúc áp dụng, không làm hỏng phiếu |

Ghi chú khi trích dẫn: kafka F2 N=4 là `not_recovered` dù `stall_s = 1` (ghi giữ 64/s, dưới ngưỡng 0,9 × trước lỗi); số lần lặp của E6 là 3, trừ các cấu hình đã chạy lại (5–8).

## Mã commit sau khi đổi email (2026-11-02)

Toàn bộ lịch sử (44 commit, nhánh `master` và `spike/hotstuff`, tag `p0-baseline` … `p4-faults`) được viết lại để email tác giả và người commit là `khoatran.yuji@gmail.com` (trước đây `khoatran@freec.asia`); nội dung (tree) của mọi commit không đổi. Mã commit trong ghi chú này đã được thay bằng mã mới, nhưng **cột `commit` của các CSV kết quả giữ mã cũ** (đó là giá trị ghi lúc đo): `3a1ade8` (P0, `results/p0-baseline.csv`) = `8a54d00`, `a0e9aa9` (E1–E6, `results/final/*.csv`) = `e7baa60`. Bảng đầy đủ:

| Ngày | Mã cũ | Mã mới | Commit |
|---|---|---|---|
| 2026-10-29 | `c59a854` | `0976d86` | chore: import original artifact |
| 2026-10-29 | `e85397f` | `419c71b` | build: arm64 toolchain and images |
| 2026-10-29 | `27aa82f` | `800fbfe` | build: arm64 fixes (bookworm node image, kafka client v2) |
| 2026-10-29 | `2fa0ab7` | `7732fb7` | bench: time-based runs, percentiles, CSV output |
| 2026-10-29 | `cf52980` | `399b8aa` | harness: docker-compose cluster, health check, single-run driver |
| 2026-10-29 | `ba07253` | `f1da9b8` | consensus: interface and in-memory hub |
| 2026-10-29 | `d6c8e60` | `c2cb146` | audit: root digest collection and divergence check |
| 2026-10-29 | `1b60d0d` | `ae99c50` | exp: P0 baseline driver and notes |
| 2026-10-29 | `2cc8777` | `6326dd6` | plot: aggregation, rerun list, figures |
| 2026-10-29 | `a5d01fd` | `f9be1b3` | core: deterministic executor |
| 2026-10-29 | `687d170` | `e1b567e` | core: batcher, gRPC server, sync/async commit, breakdown |
| 2026-10-29 | `3a1ade8` | `8a54d00` | exp: host-state log per run, configurable part (b) cluster sizes |
| 2026-10-30 | `72e8539` | `e544bc0` | exp: P0 baseline results (kafka-orig, tm-orig; N=4,8) |
| 2026-10-30 | `75b15cd` | `a5195b7` | consensus/kafka: approve-all Kafka backend wired into cmd/veritas |
| 2026-10-30 | `3211ecf` | `7a7a158` | exp: P1 checks; keep per-run core breakdown files |
| 2026-10-30 | `f625b23` | `bcd8b17` | spike: hotstuff gate script (`spike/hotstuff`) |
| 2026-10-30 | `4fb6640` | `d526e00` | docs: HotStuff gate decision |
| 2026-10-30 | `d46155b` | `ebbcac2` | consensus: raft backend |
| 2026-10-30 | `7b39010` | `388236d` | consensus/raft: fix round 1 (go.sum, race-safe tests, no snapshots) |
| 2026-10-30 | `0d12192` | `a0a6187` | consensus: tendermint backend |
| 2026-10-30 | `3c8f20e` | `f42a12a` | consensus/tendermint: fix round 1 (gas cap per block, mempool bytes, port wait) |
| 2026-10-30 | `384ad8f` | `4e66fd7` | consensus/tendermint: fix round 2 (drop gas cap, sync/client guard) |
| 2026-10-30 | `fb6c2b5` | `61c425f` | faults: crash, slow, network-silent injection |
| 2026-10-30 | `b996d59` | `a78ec5a` | faults: fix round 1 (keep RTT record, prompt F1, orphan guard, netem limit) |
| 2026-10-30 | `48920bd` | `c4d6878` | audit: periodic auditor and RQ3 metrics |
| 2026-10-30 | `3679997` | `ace4050` | audit: fix round 1 (detect guard, dip-based recovery, stop order) |
| 2026-10-30 | `fb08079` | `70aa001` | consensus: bft2 (hotstuff sidecar) |
| 2026-10-30 | `97993a2` | `192db6c` | consensus/bft2: fix round 1 (go.sum, crash liveness) |
| 2026-10-30 | `4411d34` | `43742a8` | bft2node: stop the reverse-view test loop before View underflows |
| 2026-10-31 | `bc58da4` | `50a5e6b` | consensus/bft2: fix round 2 (view timeout, retention, fault ports) |
| 2026-10-31 | `17ed744` | `b4e8785` | core: fault policy (silent, corrupt) |
| 2026-10-31 | `2985576` | `1566bac` | core: fault policy on bft2 (rebase onto p3-bft2) |
| 2026-10-31 | `2f9f0f2` | `e720ba3` | core: fault policy fix round 1 (pin kafka silent, confirm activation, F4 cap) |
| 2026-10-31 | `857e394` | `43a5aa6` | exp: E1-E6 drivers |
| 2026-10-31 | `c4d0e20` | `89555a0` | exp: E1-E6 drivers fix round 1 (hermetic tests, plan validation) |
| 2026-10-31 | `27ef981` | `86e17e1` | docs: batch-flush decision for E1-E6 |
| 2026-10-31 | `8ee51fb` | `338ccc4` | ledger: prune orphaned SMT nodes (bounded memory) |
| 2026-10-31 | `561f95f` | `25bb911` | final-fix: read/write split, resource stats, validity gate, leader faults, run guards |
| 2026-10-31 | `9265dcd` | `bfd72b7` | final-fix: gofmt recorder_test.go |
| 2026-10-31 | `a0e9aa9` | `e7baa60` | final-fix: round 2 (aborted split, gate calibration, exact net bytes, raft forward timeout, bft2 mem) |
| 2026-11-02 | `f4e4668` | `e27614b` | plot: figures for E3-E6 (x axis per experiment, E6 bars per fault) and rq3_table.py |
| 2026-11-02 | `a5b21aa` | `4e15297` | plot: E6 no-fault ticks in backend colour, duplicate (backend, fault) rows raise |
| 2026-11-02 | `cbf3735` | `80fbd66` | plot: RQ3 liveness metrics on the write timeline when .wtimeline is usable (timeline_source key and column) |
| 2026-11-02 | `b02d523` | `5389261` | results: final data and figures |

## Đợt chạy cloud: E1–E6 ở flush 100 ms (2026-11-02 → 2026-11-03)

Mục đích: bộ hẹn giờ flush 1 s che phần lớn khác biệt giữa các giao thức ở E1–E6 (xem "Quyết định về BATCH_FLUSH"); đợt này chạy lại cả E1–E6 với `BATCH_FLUSH=100ms` trên **một máy**, để E2–E6 lấy CLIENTS tốt nhất từ E1 đo trên cùng máy. Dữ liệu này **không gộp** với dữ liệu M3 (phần cứng khác).

### Môi trường

- Phiên Claude Code trên cloud: linux/amd64, **4 CPU**, 15,7 GiB RAM, kernel 6.18 (firecracker), Docker 29.9.2 (dockerd khởi động tay). Host rảnh (không có ứng dụng khác).
- Giới hạn giữ như M3: mỗi node `CPUS=0.5`, `MEM=768m`; broker Kafka 2 CPU / 1,5 GB; 10k bản ghi YCSB 1 KB; `DURATION=60s`, `WARMUP=10s` (E6: 180 s, lỗi ở giây 60).
- Build: `experiments/cloud/build.sh` (các Dockerfile trong `experiments/docker/` giữ nguyên). Mạng ra ngoài đi qua proxy chặn TLS: image gốc được thay qua `--build-context` bằng bản có CA của proxy; `deb.debian.org` bị chính sách mạng chặn (403), nên `hbdb-node` dựng trên Ubuntu 24.04 với **cùng binary Redis 6.2.24** chép từ `redis:6.2-bookworm` (`experiments/cloud/Dockerfile.redis-ubuntu`). Tendermint v0.35.0, Kafka 2.7.0, librdkafka (confluent-kafka-go v2.1.1, amd64) như M3.
- Driver: `experiments/cloud/campaign.sh` (EXP `cloud-eN-bf100ms`, `E1_EXP=cloud-e1-bf100ms`). Dữ liệu: `results/cloud-bf100ms/` (CSV, `.failures`, `.params`, sidecar mỗi lượt); `experiments/cloud/assemble.py` → `results/cloud-bf100ms/final/eN.csv` (cột `exp` đổi thành `eN`; chỉ chép sidecar của lượt có dòng CSV, vì lượt hỏng vẫn để lại `.timeline` mà `rq3_table.py` sẽ đếm như một lượt); hình và bảng RQ3 ở `results/cloud-bf100ms/figs/`.
- Mã đo không đổi suốt đợt: từ `f598595` tới cuối, ngoài `results/` chỉ có `.gitignore` và `experiments/cloud/assemble.py` thay đổi. Cột `commit` vì vậy có nhiều mã (các commit chỉ thêm kết quả) và vài dòng `-dirty` (lúc `assemble.py` chưa commit); E6 chạy tiếp với `EXP_ALLOW_COMMIT_CHANGE=1`.

### Không chạy được / sai lệch so với kế hoạch

- **Không có E5 và F3**: kernel không có `sch_netem` (`CONFIG_NET_SCH_NETEM is not set`), nên không tạo được RTT hay node chậm. `campaign.sh` tự phát hiện và chạy E6 với `E6_KINDS="F1 F2 F4"` (cờ mới của `e6_faults.sh`; không có F3, kể cả F3 leader): 108 lượt thay vì 144. E5/F3 phải đo trên máy có netem (M3).
- **N=7 bão hòa CPU host**: mọi điểm N=7 (E2, E6) có `saturated=1` (CPU host > 90%: 7 × 0,5 + broker 2 + TSO trên 4 CPU). Kết quả N=7 phản ánh máy 4 CPU nhiều hơn giao thức; không dùng để kết luận về khả năng mở rộng.
- **Container của phiên khởi động lại** lúc ~22:50Z (giữa E6). Ổ đĩa giữ nguyên; lượt đang chạy dở không có dòng CSV; driver chạy tiếp và bỏ qua dòng đã có.
- **Tendermint mất khối khi bão hòa** (như M3, xem "Tendermint: mất khối khi bão hòa"): ở c256 nhiều lượt có > 1% lượt ghi hết thời gian. E2–E4 được chạy bù với `VALID_MAX_WRITE_ERR_PCT=5` (như M3). Còn thiếu: E3 block 10 chỉ có 2/3 rep (lần thử thứ ba dừng 20 s liên tiếp, rồi 5,8% lỗi ghi); hai rep phụ của E1 c256 (CV 13%) đều hỏng.
- **OOM ở E6 với 768m**: thông lượng cloud cao gấp 1,5–3 lần M3, nên trong lượt 180 s bộ nhớ node (veritas + sidecar + redis) chạm trần:
  - bft2 với F4/F5-F4 (`corrupt`): mọi lượt bị OOM (F4 không làm giảm thông lượng, nên bộ nhớ tăng suốt 180 s; RSS ~590–645 MiB rồi bị kill). Đã chạy bổ sung **4 cấu hình này với `MEM=1g`** ở EXP riêng `cloud-e6mem1g-bf100ms` (12/12 lượt đạt; `final/e6mem1g.csv`, `figs/e6mem1g_*`, `figs/rq3_summary_e6mem1g.csv`). Đây là biến khác (giới hạn bộ nhớ) so với mọi lượt khác, phải ghi rõ khi trích dẫn.
  - raft N=7: F1 leader và F4 chỉ có 2/3 rep (lượt còn lại OOM cả khi chạy lại).
- Rep phụ do CV > 10% vẫn chạy như kế hoạch (`results/cloud-bf100ms/rerun.txt`: 15 cấu hình còn cờ, phần lớn là E6, nơi tps trộn phần lành và phần lỗi nên CV cao là bình thường).

### Kết quả chính (tps trung bình, `final/`)

E1 (N=4, block 100), so với E1 100 ms trên M3:

| CLIENTS | kafka | raft | bft2 | tendermint | M3 (kafka / raft / bft2 / tendermint) |
|---|---|---|---|---|---|
| 4 | 78 | 78 | 66 | 6,1 | 78 / 76 / 64 / 6 |
| 16 | 313 | 315 | 253 | 24 | 295 / 279 / 237 / 24 |
| 64 | 1041 | 1039 | 856 | 93 | 845 / 824 / 693 / 95 |
| 256 | 1789 | 1851 | 1690 | 312 | 1115 / 1150 / 1170 / 247 |

- CV ≤ 7% ở mọi điểm trừ tendermint c256 (13%): máy cloud yên hơn nhiều so với M3 (CV 13–26% ở c64–c256).
- Ở tải thấp hai máy cho cùng số (giới hạn bởi độ trễ); ở c64–c256 cloud cao hơn 25–60%.
- Thứ hạng ổn định: kafka ≈ raft > bft2 (thấp hơn ~15–20% ở c4–c64, ~5–9% ở c256) ≫ tendermint (thấp hơn ~6 lần, do `timeout-commit = 1s`, I6). CLIENTS tốt nhất = 256 cho cả 4 backend.

E2–E4 (CLIENTS 256):

| | kafka | raft | bft2 | tendermint |
|---|---|---|---|---|
| E2 N=4 / N=7 | 1828 / 1690 | 1870 / 2004 | 1656 / 1516 | 298 / 249 |
| E3 block 10 / 100 / 1000 | 1693 / 1811 / 1771 | 1829 / 1760 / 1891 | 1322 / 1654 / 1667 | 190 / 314 / 302 |
| E4 workload a / b / c | 1880 / 8567 / 14357 | 1935 / 8923 / 14549 | 1688 / 7178 / 13335 | 327 / 2224 / 13560 |

- E2: N=7 bão hòa CPU host (trên), không kết luận.
- E3: ở flush 100 ms, block 10 làm giảm bft2 (−20%) và tendermint (−40%), gần như không ảnh hưởng kafka/raft. **Block 100 và 1000 vẫn bằng nhau**: với 256 client mỗi node chỉ có ~CLIENTS/(2N) ≈ 32 `Set` đang chờ, nên block 100 vẫn không đầy; flush 100 ms làm hết hiện tượng bộ hẹn giờ che giao thức, nhưng không làm block đầy. Muốn block đầy cần nhiều client hơn (tendermint bị chặn ở 256).
- E4: như M3; đọc phục vụ cục bộ nên workload c gần như bằng nhau.

### RQ3 (E6, cloud)

Chỉ số dùng ở đây: **tỉ lệ ghi** = lượt ghi thành công/giây trung bình sau lỗi (t = 70–180 s) chia cho trước lỗi (t = 0–60 s), từ `.wtimeline`. Lưu ý:

- Thông lượng tự giảm trong lượt 180 s (ledger/badger lớn dần ở thông lượng cao): ở F4, lỗi không chạm tới đồng thuận nhưng tỉ lệ của kafka/raft/bft2 vẫn chỉ 0,7–0,85. So các lỗi khác với mức F4, không với 1,0.
- `recovery_status` của `rq3.py` (giây đầu tiên ≥ 90% mức trước lỗi) cho kết quả sai với tendermint: timeline ghi của tendermint theo đợt (vd. 192, 0, 192 mỗi giây), một giây vượt ngưỡng là tính "recovered" dù sau đó ghi = 0 hẳn. Bảng `figs/rq3_summary.csv` vẫn được tạo như M3, nhưng kết luận dưới đây dựa trên tỉ lệ ghi.

| Tỉ lệ ghi (rep) | kafka | raft | tendermint | bft2 |
|---|---|---|---|---|
| F1 N=4 (node4) | 0,00 | 0,60–0,67 | 0,33–0,50 | 0,24 |
| F1 N=7 (2 node) | 0,00 | 0,64–0,72 | 0,38–0,51 | 0,09–0,11 |
| F2 N=4 | 0,43–0,48 | 0,63–0,69 | 0,00–0,02 | 0,17–0,19 |
| F2 N=7 | 0,00 | 0,66–0,80 | 0,00–0,01 | 0,00–0,07 |
| F4 N=4 | 0,65–0,71 | 0,71–0,74 | 0,85–0,94 | 0,78–0,79 (1g) |
| F4 N=7 | 0,71–0,83 | 0,68–0,80 | 0,88–1,57 | 0,79–0,84 (1g) |

- **F1/F2 kafka**: như M3 — crash làm dừng ghi hẳn (approve-all); F2 N=4 chỉ còn client của node im lặng ghi được.
- **raft**: mọi lỗi f node (kể cả crash leader node1: bầu lại, dừng ghi 1–2 s) giữ mức ngang F4 (~0,65–0,8): không mất gì ngoài client của node lỗi.
- **tendermint F2 (1 node bị cô lập, N=4) và F2 N=7: mất liveness** (ghi = 0 từ ~10–20 s sau lỗi tới hết lượt, 3/3 và 5/5 rep), trái với giả thuyết và với M3 (M3: dừng 16,6 s rồi chạy tiếp ở ~1/3). F1 (crash) thì chạy tiếp ở 0,33–0,51. Khả năng là áp lực ngược của adapter ABCI (kênh Deliver đầy → `DeliverTx` giữ mutex) bị kích hoạt khi một validator im lặng ở tải cao; **cần điều tra trước khi dùng làm kết luận**.
- **bft2 F1/F2**: vẫn chạy (dừng ghi ≤ 1 s, trừ F2 N=7) nhưng ghi chỉ còn 0,17–0,24 (N=4) và 0,0–0,11 (N=7): khác M3 (ở M3, tps tổng của bft2 F1 còn ~80% mốc không lỗi). Nguyên nhân phù hợp với giới hạn adapter (I3): mỗi view chỉ đề xuất từ hàng đợi của leader hiện tại, view của node chết hết thời gian, và client của node đó bị mất.
- **F5-F2 (f+1 node im lặng)**: tendermint và bft2 mất liveness như M3 (bft2 vẫn còn ghi lẻ tẻ: tps tổng ~620 gồm đọc).
- **F4 / F5-F4**: đồng thuận không bị ảnh hưởng; auditor phát hiện sau 0,2–1,5 s ở mọi lượt có dòng CSV; client đọc từ node lỗi nhận giá trị sai (`wrong_gets` 1,7–42 nghìn mỗi lượt, cao hơn M3 vì đọc nhiều hơn). bft2 F4/F5-F4 chỉ có ở 1g (trên).

## Giai đoạn 2: hiện thực (2026-11-06 → 2026-11-07)

Theo spec `docs/superpowers/specs/2026-11-03-phase2-fair-comparison-and-verifiability-design.md` và plan cùng ngày. Đã có mã và unit test cho P7–P10 (Task 1–11); **chưa chạy** E7, E7f, E6', E8, E9 và `check_consistency.sh` (phiên hiện thực không có Docker). Chưa có số liệu giai đoạn 2, nên chưa có tag `p7-openloop` … `p10-attest`.

### Môi trường kiểm thử

- Go 1.20.14 (module chính) và 1.23.12 (`bft2node/`) chạy trực tiếp trên host, không qua `hbdb-build`.
- Sandbox không cho mở cổng local: các test cần TCP listen (cụm bft2 trong tiến trình, raft, `httptest`, `veritas/audit`) không chạy được, kể cả hai test mới `TestForwardedBlockCommitsFromAnyLeader` và `TestForwardSurvivesOneCrash`. Phải chạy lại cả bộ test trong `hbdb-build` / `hbdb-bft2-build` trước khi đo.

### Sai lệch so với plan

- `protoc-gen-go` v1.3.2 thay vì v1.3.5: v1.3.2 sinh lại đúng từng byte file đã commit; `scripts/genproto_docker.sh` cố định phiên bản này.
- Tốc độ vòng mở là một chiều của cấu hình trong `exp_lib.sh` (cột `offered_rps`), nên E7 quét nhiều tốc độ trong cùng một EXP; biến môi trường `RATE` trong `.params` chỉ là giá trị mặc định.
- `.ops.csv` thêm các cột cuối `dropped, read_bytes_mean, verify_cpu_us_mean, proof_errors, cert_errors, dataloss_errors`.
- bft2: lỗi F2 (iptables) chặn cả cổng chuyển tiếp 7202, để node im lặng không tiếp tục chuyển block.
- tendermint: khi tràn hàng đợi 128 MiB, app đóng Deliver (vòng apply dừng) nhưng tiến trình không thoát.
- Attestation: chỉ ký các block có tx ở height chia hết cho K, cộng với block không có tx khi root đã đổi từ lần ký trước; node rảnh có root chưa được ký gửi một block rỗng ("tick"). Nếu ký cả block chỉ chứa attestation thì cụm rảnh sinh block vô hạn; nếu không có tick thì đọc có xác nhận với K > 1 trên cụm rảnh không bao giờ xong.
- Đọc có xác nhận: proof luôn được lấy tại một height đã ký (ngay nếu root hiện tại là root đã ký, nếu không thì vòng apply lấy proof ngay sau height được ký kế tiếp), rồi chờ xác nhận, tổng tối đa 5 s.
- E8 và E9 tự gọi lại chính mình cho mỗi cặp cấu hình, mỗi cặp một EXP (`e8-<sigs>-<read>`, `e9-a<K>-<read>`), vì CSV không có cột cho chúng.

## Giai đoạn 2: chạy trên M3 (giai đoạn 3, Task 1–2)

### Môi trường

- Docker VM (2026-11-09, `docker info`): `MemTotal` = 8.217.890.816 byte (7,65 GiB), 11 CPU. Nhỏ hơn 10 GiB nên **N=7 bỏ**: với `MEM=1g`, N=7 cần ~7 GiB cho node cộng TSO/client, không vừa VM. Mọi thí nghiệm giai đoạn 3 (E10–E12) và các lượt giai đoạn 2 chạy lại trên M3 chỉ dùng N=4.
- Session Claude Code không truy cập được Docker (sandbox chặn `/Users/yuji/.docker/run/docker.sock`), nên các lệnh Docker (build, test Go trong `hbdb-build`, `check_consistency.sh`, driver thí nghiệm) do người dùng chạy trên terminal của họ; kết quả được ghi lại ở đây.

### Build, test và kiểm tra nhất quán (Task 1, 2026-11-09)

- `experiments/build.sh`: đạt (ảnh `hbdb-build`, `hbdb-bft2-build`, `hbdb-node` build lại từ nhánh này).
- Test Go (trong `hbdb-build`, `-race`): `./consensus/... ./veritas/... ./cmd/auditor ./cmd/veritas` đều `ok` (17 gói có test). Không dùng `./...`: các gói cũ `BlockchainDB/`, `tso_zookeeper/` thiếu mục go.sum (như README). `bft2node` (`hbdb-bft2-build`): 4/4 gói `ok`, gồm `TestForwardedBlockCommitsFromAnyLeader`, `TestForwardSurvivesOneCrash` (lần đầu chạy được, phiên hiện thực giai đoạn 2 không có Docker).
- Dữ liệu YCSB: `results/ycsb/*.dat` của checkout chính (10k bản ghi, giai đoạn 1) chép sang worktree (SHA-256 `a.load.dat` trùng).
- `ATTEST_EVERY=1 MEM=1g BATCH_FLUSH=100ms experiments/check_consistency.sh <b>`: kafka, raft, tendermint, bft2 đều **CONSISTENT**, không có dòng `core: diverged` trong log.
- `gofmt -l veritas cmd` chỉ liệt kê 5 file gốc không sửa (`veritas/benchmark/util.go`, `veritas/server.go`, `cmd/kafkabench`, `cmd/mongodb`, `cmd/veritastm-mongodb`).


## Giai đoạn 3: smoke test truy vết (Task 8 Step 4, 2026-11-10)

`EXP=smoke-p3 BACKEND=raft N=4 ATTEST_EVERY=1 READ_MODE=certified FAILOVER=1 SIGN_READS=on-failure FAULT=F6:node4@20 DURATION=40s experiments/run_one.sh` ở commit `3935e95` (ảnh build lại bằng `experiments/build.sh`; flush mặc định 1 s, 16 client). Lệnh Docker chạy trong Terminal panel của session.

- Exit 0, có dòng CSV: 18,25 tps (trần ~CLIENTS / 0,9 s của flush 1 s), `errors` = 0, **`wrong_gets` = 0**.
- `readlie` bật lúc t = 20,0 s, xác nhận lúc 20,5 s. `<tag>.evict`: cả 4 node ghi `evicted node 4 at height 378 (bad_read)`, **cùng height**, khoảng 1,2–1,7 s sau khi lỗi bật (log 01:39:59.85–.93Z).
- `.ops.csv`: `rejected_reads` = 3, `failovers` = 4 (4 worker có node chính là node4), `evidence_sent` = 2, `wrong_before_failover` = 0, `cert_timeouts` = 0, `quorum_errors` = 0.
- `.diverged` và `.certs` rỗng: F6 không đổi trạng thái, nên không có root mâu thuẫn và không có dòng `core: certified height` (dòng này chỉ ghi khi có root mâu thuẫn). Auditor: 52 tick, đều nhất quán.

## Giai đoạn 2/3: E7 quét tốc độ trên M3 (Task 2, 2026-11-10)

Lệnh: `EXP=e7 BACKENDS="raft bft2" RATES="250 500 600 700 800 900 1000 1500 2000" experiments/exp/e7_rate.sh` (N=4, 256 client, block 100, flush 100 ms, workload A, 60 s + 10 s warmup, 3 rep; bft2 2000 có 5 rep do CV 0,098). Commit đo `7461028`, 14:31–16:01 UTC, 56 hàng, không lượt nào hỏng. Phân tích: `experiments/plot/saturation.py results/raw/e7.csv --ops-dir results/raw --out results/e7` (bản `4c86bc4`), kết quả `results/e7/saturation.csv`, `saturation_points.csv`.

**Lượt quét đầu (`results/raw/e7-noisy*`, lưới mặc định) bỏ toàn bộ**, kể cả rep 1–2 trước 02:30: raft 1000 req/s khi đó write p99 2,3–3,6 s, nay 0,9–2,1 s (trung bình 1,4 s); compressor của host 6,5–8,5 GB (nay ~4 GB), từ ~02:43 host swap ~66k trang/phút. Lượt mới: swap-out của host chỉ tăng ~23k trang trong lượt đầu tiên rồi đứng yên; load1 3,9–13 (cụm tự tạo ~10–13), chỉ vượt 15 ở bft2 2000 (cụm quá tải).

| backend | sat_rps (p99 ≤ 2 s) | tps tại đó | write p99 | slo_rps (p99 ≤ 1 s, liên tục) | write p99 | **E9_RATE** |
|---|---|---|---|---|---|---|
| raft | 1000 | 999,5 | 1388 ms | 600 | 262 ms | **480** |
| bft2 | 1000 | 999,9 | 1824 ms | 600 | 458 ms | **480** |

- Thông lượng bám tốc độ đặt tới 1000 (CV ≤ 0,005); 1500: raft 1498 tps (p99 3,5 s), bft2 1498 tps nhưng drop 1076 op; 2000: raft 1961, bft2 1862 tps (drop 33,9k), p99 3,9 / 5,8 s.
- Write p99 dao động giữa rep ở vùng 700–1000 (bft2 700: 3,2 / 1,3 / 1,4 s; raft 1000: 0,9 / 1,2 / 2,1 s) và không đơn điệu (raft 900 trung bình 0,98 s nằm trong SLO, 700/800 thì không). `slo_rps` dừng ở rate đầu tiên vượt SLO nên lấy 600 cho cả hai.
- **Định nghĩa tốc độ E9–E12** (khác plan, theo rà soát): `e9_rate` = 80% của rate cao nhất mà nó và mọi rate thấp hơn đều có tps ≥ 0,95R, write p99 ≤ 1 s và không drop. Theo plan cũ (80% của `sat_rps`) sẽ là 800 req/s, nơi write p99 đã 1,0–1,6 s và hàng đợi chi phối. `E9_RATE_RAFT=480`, `E9_RATE_BFT2=480`.
- Giới hạn của E9 ở 480 req/s: thông lượng bằng tốc độ nạp (dưới điểm bão hòa), nên E9 chỉ đo được chi phí **độ trễ** của attestation, không đo được mất thông lượng.
- Nguồn gốc commit: tốc độ E9 lấy từ E7 chạy ở commit `7461028`; E9 chạy ở commit sau khi gộp nhánh `-p3fix` (mục 5–8 của rà soát: hàng đợi evidence, thời điểm failover; host guard). Đường ghi (Set → block → apply) không đổi giữa hai commit.
- Abort (cột `errors` của CSV chính gộp lỗi và abort; `.ops.csv` tách `aborted`): raft có abort, tăng theo tải (ghi bị abort mỗi rep: 5–7 ở 500, 23–33 ở 1000, 61–94 ở 1500 req/s, tối đa ~0,2% số ghi), bft2 = 0 ở mọi rate. Nguyên nhân: raft/kafka dùng phiên bản TSO (`VersionClient`), Set có phiên bản ≤ phiên bản đã lưu bị bỏ khi thứ tự đồng thuận khác thứ tự TSO; bft2/tendermint dùng `VersionOrdered` (phiên bản = vị trí trong block), không bao giờ abort.

## Giai đoạn 2/3: E9 attestation trên M3 (Task 2, 2026-11-10/11)

Lệnh: `E9_RATE_RAFT=480 E9_RATE_BFT2=480 experiments/exp/e9_attest.sh` (N=4, 256 client, MEM=1g, flush 100 ms, 180 s + 10 s warmup, lỗi ở giây 60, 3 rep; raft 10 + bft2 15 cấu hình). Commit đo `9560921` (sau khi gộp rà soát; đường ghi như E7 `7461028`). 19:25–23:39 UTC, 75/75 hàng, `failures in this run: none`; host guard: 75/75 `ok`, 0 lượt bị loại vì host (chạy thử trước đó: 1 lượt raft bị loại đúng, CPU ngoài Docker trung bình 261%, write p99 1,23 s). Phân tích: `experiments/plot/rq5.py results/raw/e9-*.csv --raw-dir results/raw --out results/e9` → `results/e9/rq5_runs.csv`, `rq5_summary.csv`.

| backend | attest | đọc | lỗi | phát hiện trong giao thức | detect_protocol_s | detect_auditor_s | wrong_gets | tps |
|---|---|---|---|---|---|---|---|---|
| raft | 0 | plain | F4 | 0/3 | – | 0,17 | 4733 | 480 |
| raft | 1 | plain / certified | F4 | 3/3, 3/3 | 0,38 / 0,32 | 0,26 / 0,19 | 0 / 0 | 440 / 441 |
| raft | 10 | plain / certified | F4 | 3/3, 3/3 | 0,47 / 0,45 | 0,44 / 0,13 | 0 / 0 | 441 / 440 |
| bft2 | 0 | plain | F4 | 0/3 | – | 0,87 | 4723 | 480 |
| bft2 | 1 | plain / certified | F4 | 3/3, 3/3 | 0,32 / 0,31 | 1,15 / 0,81 | 0 / 0 | 441 / 440 |
| bft2 | 10 | plain / certified | F4 | 3/3, 3/3 | 0,40 / 0,45 | 0,89 / 0,55 | 0 / 0 | 441 / 441 |
| bft2 | 0 | plain | F5-F4 | 0/3 | – | 0,88 | 9522 | 480 |
| bft2 | 1 | plain / certified | F5-F4 | 0/3, 0/3 (+1 race) | – / (0,38) | 0,84 / 0,83 | 9544 / 0 | 480 / 320 |
| bft2 | 10 | plain / certified | F5-F4 | 0/3, 0/3 | – | 0,47 / 0,57 | 9530 / 0 | 480 / 320 |

- F4 (1 node sai, quorum 2f+1 = 3): attestation phát hiện ở mọi lượt trong 0,3–0,47 s (attest 10 chậm hơn attest 1 ~0,1 s); node lệch tự từ chối đọc (`DataLoss`), nên `wrong_gets` = 0 kể cả đọc thường; mất ~40 tps = 1/4 số đọc (gửi tới node4) trong 120 s sau lỗi, vd. raft a1 plain rep 1: 7095 đọc lỗi (không bật failover ở E9). Không attestation: client nhận ~4,7k giá trị sai mỗi lượt, auditor thấy trong 0,17–0,88 s.
- F5-F4 (2 node sai thông đồng, bft2): không phía nào đủ 3 chữ ký, không height nào được xác nhận → không phát hiện trong giao thức; đọc thường vẫn nhận ~9,5k giá trị sai; đọc certified không nhận giá trị sai nhưng mọi đọc sau lỗi đều hết hạn chờ chứng chỉ (vd. a1 rep 2: 14441 đọc thành công = phần trước lỗi, 27535 lỗi) → tps 320 (mất sẵn sàng đọc, đúng như spec 11). bft2 a1 certified rep 1 là **race của việc tiêm lỗi**, không phải lỗi Certifier: hai SIGUSR1 gửi cùng lúc (21:19:38.844) nhưng node3 áp lỗi trước height 3013, node4 sau; ở height 3013 chỉ một node sai nên node 1, 2, 4 xác nhận root đúng `ca48b58e` (quorum 3 hợp lệ), node3 lệch và bị cả 4 node loại ở height 3020 (`divergent_root`). `rq5.py` đánh dấu `injection_race` và không tính vào `detected_runs`.
**Độ trễ (phân tích lại, vòng review 2; `rq5.py` `333fc26`, không đo thêm).** Lượt không lỗi, trung bình 3 rep:

| backend | attest | đọc | write p50 | write mean | write p99 (min–max) | read p50 | read p99 | queue/block (node) |
|---|---|---|---|---|---|---|---|---|
| raft | 0 | plain | 71 | 102 | 1179 (859–1514) | 1,1 | 63 | 6,6 |
| raft | 1 | plain | 81 | 115 | 1326 (1155–1478) | 1,1 | 62 | 11,0 |
| raft | 1 | certified | 78 | 116 | 1380 (1305–1466) | 92 | 1498 | 14,6 |
| raft | 10 | plain | 77 | 115 | 1461 (1036–1757) | 1,1 | 63 | 11,6 |
| raft | 10 | certified | 82 | 137 | 2144 (1560–2662) | 223 | 2257 | 23,6 |
| bft2 | 0 | plain | 100 | 144 | 1798 (1537–2041) | 1,3 | 63 | 3,3 |
| bft2 | 1 | plain | 99 | 147 | 1679 (1430–1872) | 1,2 | 67 | 4,6 |
| bft2 | 1 | certified | 98 | 152 | 1887 (1727–2051) | 102 | 1970 | 3,2 |
| bft2 | 10 | plain | 102 | 163 | 2167 (1935–2438) | 1,2 | 74 | 2,9 |
| bft2 | 10 | certified | 96 | 160 | 2266 (1889–2558) | 225 | 2583 | 7,7 |

(ms; queue/block = thời gian một block chờ apply loop, từ breakdown snapshot đầu/cuối cửa sổ: trung bình 3 rep, mỗi rep trung bình 4 node; min–max theo rep: raft a0 5,7–7,5, a1-plain 7,0–15,0, a1-certified 12,7–17,6, a10-plain 6,5–14,3, a10-certified 15,8–27,9; bft2 a0 1,5–6,4, a1-plain 4,0–5,3, a1-certified 2,5–4,2, a10-plain 2,2–3,5, a10-certified 6,5–9,7.)

- **Chắc:** đọc certified đắt: read p50 ~92–102 ms ở attest 1 và ~223–225 ms ở attest 10 (chờ height được attest kế tiếp), so với 1,1 ms đọc thường; read p99 1,5–2,6 s. Không phụ thuộc thứ tự chạy (a10-plain chạy sau a1-certified vẫn 1,1 ms).
- **Chắc:** không tách được chênh lệch write p50 dưới ~10 ms giữa các mức attestation (trung bình nhóm raft 71–82, bft2 96–102 ms; theo rep: raft a0 65–75, a1-plain 76–85, a0 F4 68–90 ms).
- **Chưa chắc:** ảnh hưởng của attestation lên đuôi độ trễ ghi. Write p99 lệch 300–1100 ms giữa các rep; chỉ raft a10-certified nổi lên (mean 137 so với 115 ms, p99 2,1 so với 1,5 s). Các cặp chạy theo thứ tự a0 → a1 → a10 nên mean/p99 còn lẫn với thứ tự.
- **Giải thích cần nêu (không sửa mã):** đọc certified chờ height kế tiếp được chứng minh ngay trong apply loop (`attestor.afterApply` gọi `prove` cho mọi reader đang chờ, `ProveValue` giữ khóa độc quyền của executor), nên block kế tiếp xếp hàng sau các proof. Breakdown ủng hộ ở 3 trong 4 cặp: queue/block certified > plain cùng attest_every (raft a1 14,6 so với 11,0 ms, a10 23,6 so với 11,6; bft2 a10 7,7 so với 2,9), execute/block không đổi (1,2–1,9 ms); ở a10 reader dồn qua 10 block rồi được chứng minh một lượt. Cặp ngược: bft2 a1 certified 3,2 so với plain 4,6 ms (khoảng theo rep chồng nhau). Ngoài đọc certified, riêng attestation đã làm queue/block của raft tăng từ 6,6 (a0) lên 11,0 ms (a1-plain) ở đọc thường: ứng viên cho chi phí của attestation trên đường ghi, nhưng còn lẫn với thứ tự chạy (a0 chạy trước); để E9b kiểm. Mỗi cặp plain/certified cùng mức chạy liền nhau nên so sánh này ít bị thứ tự chi phối.
- **Lượt 180 s so với 60 s:** độ trễ xấu dần trong lượt, không phải vài đợt dừng hiếm. `.wtimeline` (Set thành công/giây, nạp 240/s) có độ lệch chuẩn 12–27/s ở 1/3 đầu (như E7 60 s: 16), 20–58 ở 1/3 giữa và 54–108 ở 1/3 cuối; các giây < 120 Set chỉ xuất hiện ở 2/3 sau (0 / 0–2 / 1–4). Nguyên nhân (trạng thái lớn dần: SMT, ledger, store; GC) chưa xác định; CPU/bộ nhớ node trong `docker stats` không cho thấy xu hướng rõ. Hệ quả: `slo_rps` của E7 (lượt 60 s) chỉ đúng cho lượt 60 s; giữ 480 req/s cho E9–E12 nhưng nêu rằng ở lượt 180 s write p99 đã 1–2,7 s.
- Thứ tự chạy của driver E10–E12 đã đổi sang xen kẽ (vòng ngoài là rep, cấu hình xoay vòng); nếu sau E10–E12 vẫn chưa tách được ảnh hưởng của attestation lên ghi thì đề xuất E9b (chỉ lượt không lỗi, xen kẽ, 5 rep, ~50 lượt).

## Sự cố: E10 chạy vòng kín (2026-11-11 00:41 UTC)

Lần khởi chạy đầu E10–E12 mất biến `E9_RATE_*` (lời nhắc cập nhật oh-my-zsh của tab Terminal mới nuốt ký tự đầu, `export` thành `xport`), nên `e10-plain` chạy vòng kín (2077 tps) trước khi bị dừng; vòng lặp kịp dựng cụm cho `e11-F4-plain`. Dữ liệu (1 lượt E10, params của e11-F4-plain) đã tách ra `results/raw/mistake-closedloop-*`, không dùng; chạy lại từ 00:44 UTC ở 480 req/s. Từ nay E10–E12 từ chối chạy khi không có tốc độ (trừ `E1x_CLOSED_LOOP=1`).

## Giai đoạn 3: E10–E12 trên M3 (Task 8–9, 2026-11-11)

Lệnh: `E9_RATE_RAFT=480 E9_RATE_BFT2=480 experiments/exp/e1{0,1,2}_*.sh` (N=4, 256 client, MEM=1g, flush 100 ms, attestation K=1, 3 rep, thứ tự xen kẽ theo rep). E10: 60 s + 10 s warmup, không lỗi, workload a và c. E11, E12: 180 s + 10 s warmup, lỗi ở giây 60 của cửa sổ đo, workload a. Commit đo `0f85077` (113 hàng) và `855ee23` (1 hàng chạy lại, `EXP_ALLOW_COMMIT_CHANGE=1`; giữa hai commit chỉ đổi driver, plot, NOTES, không đổi `veritas/`, cluster hay `run_one.sh`). E10 00:44–02:08 UTC, E11 02:06–13:43 (dừng 02:54–11:38), E12 13:42–15:48. 48 + 42 + 24 hàng, `failures in this run: none` ở lần chạy cuối. Phân tích: `experiments/plot/rq6.py results/raw/e1[012]-*.csv --raw-dir results/raw --out results/e1x` → `results/e1x/rq6_runs.csv`, `rq6_summary.csv`; độ trễ E10: `experiments/plot/e10_latency.py results/raw/e10-*.csv --raw-dir results/raw --out results/e1x` → `results/e1x/e10_latency.csv` (từ `.ops.csv`).

**Kiểm tra nhất quán với mã giai đoạn 3 (Task 10 bước 1, chạy sau E10–E12, 2026-11-11 15:56–16:01 UTC):** `ATTEST_EVERY=1 MEM=1g BATCH_FLUSH=100ms KEEP_LOGS=1 experiments/check_consistency.sh <b>` ở commit `446b625` (ảnh `hbdb-node` build 2026-11-10 16:47 UTC sau `9560921`; từ đó tới nay không đổi `veritas/`, `consensus/`, `cmd/`, `bft2node/`): kafka, raft, tendermint, bft2 đều **CONSISTENT**, compose có `--attest-every=1`, log container (`results/consistency/*_20261111T15*.logs`) không có dòng `core: evicted` hay `core: diverged`. Đây là lần đầu kafka và tendermint chạy với mã giai đoạn 3 (Task 1 chạy trước Task 3–7).

**Lưu trữ:** `results/raw/` bị gitignore nên toàn bộ dữ liệu thô của E7–E12 (36 MB, 7037 mục, kể cả `e7-noisy*`, `hg-trial*`, `smoke-p3*` và `mistake-*`: **không dùng**, chỉ để truy vết) được nén vào `results/archive/raw-e7-e12-2026-11-11.tar.gz` (3,9 MB, SHA-256 `75a2f1e8…62d5`, có trong git). Giải nén: `tar -xzf results/archive/raw-e7-e12-2026-11-11.tar.gz -C results`.

**Host guard:** 5 lượt bị loại (không ghi hàng, đã chạy lại): 02:17 raft F4 cert-fo r1 (swap 68k trang), 02:38 và 02:50 bft2 F6 plain r1 (swap 4,6k và 79k), 02:54 raft F6 cert-fo r1 (CPU ngoài Docker trung bình 299%, swap 1,45M) — ba lượt liên tiếp nên dừng tay sweep tới khi Mac rảnh (compressor ~8 GB → ~2,9 GB); 14:54 raft e12-q2f1-quorum r2 (swap 28k; lần thử 2 qua). `results/raw/e11-F6-plain.failures` còn dòng 02:50 của lần chạy bị dừng; hàng đó đã có sau khi chạy tiếp.

**Sự cố: Mac ngủ giữa lượt (2026-11-11 13:43–14:09 UTC).** Gập nắp khi chạy pin (`pmset -g log`: `Clamshell Sleep` 20:43 +0700, chỉ DarkWake ngắn tới 21:09) nên `caffeinate` không giữ được. Lượt raft e12-qf1-certified r1 kéo dài 13:42–14:12: lỗi tiêm ở t = 940 s thay vì 60 s, đồng hồ Docker VM chậm ~940 s so với host, `rq6.py` ra thời gian phát hiện −940 s. Host guard vẫn báo `ok` (CPU và swap bình thường; guard không xét khoảng trống giữa các mẫu: `.host` trống 947 s và 531 s). Quét `.host` của 189 lượt E9–E12: chỉ lượt này có khoảng trống > 30 s. Dữ liệu tách ra `results/raw/mistake-sleep-e12-qf1-certified/` (kèm hàng CSV gốc), chạy lại 15:45 ở `855ee23`. Sau sự cố (`run_one.sh`, sau E9–E12): host guard trả `noisy` khi hai mẫu `.host` trong pha chạy cách nhau quá `HOST_GAP_MAX_SECS` (mặc định 3 × `HOST_SAMPLE_SECS` = 15 s; khoảng cách bình thường giữa các mẫu trong pha chạy 6,0–7,1 s ở 189 lượt E9–E12; mẫu cuối, lấy sau bench và sidecar, cách mẫu trước 0,8–15,8 s nên không xét). E9–E12 chạy trước guard này; bằng chứng cho các lượt đó là lần quét `.host` ở trên.

**Hạn chế của params guard ở E9–E12:** `.params` của các EXP này ghi `RATE=0` (biến môi trường `RATE` của `run_one.sh`; tốc độ thật 480 req/s là tham số theo cấu hình của `exp_cfg`), nên guard không bắt được việc đổi `E9_RATE_*` giữa chừng (các hàng ở tốc độ khác sẽ thành cấu hình mới, không bị trộn vào hàng cũ vì `exp_has_row` khớp theo `offered_rps`). Từ nay driver E9–E12 ghi thêm `RATE_<backend>=<tốc độ>` khi tạo `.params` và dừng nếu tốc độ khác; các file `.params` cũ không có khóa này, không được sửa và không được kiểm (driver in một dòng note). Mọi hàng E9–E12 đều có `offered_rps` = 480.

### E10: chi phí không lỗi (6 lượt mỗi ô = workload a và c × 3 rep; mọi lượt tps 480, `rejected_reads` = 0, không eviction)

| backend | đọc | read p50 a | read p99 a | write p50 a | write p99 a | read p50 c | read p99 c |
|---|---|---|---|---|---|---|---|
| raft | plain | 1,5 | 41 | 91 | 377 | 1,4 | 7,8 |
| raft | quorum | 1,8 | 62 | 80 | 503 | 1,5 | 9,9 |
| raft | certified | 93 | 425 | 86 | 376 | 1,6 | 7,6 |
| raft | certified + sign-always | 101 | 450 | 91 | 406 | 1,6 | 7,9 |
| bft2 | plain | 1,5 | 49 | 108 | 501 | 1,3 | 7,2 |
| bft2 | quorum | 1,9 | 61 | 112 | 485 | 1,5 | 8,4 |
| bft2 | certified | 112 | 519 | 110 | 491 | 1,6 | 7,9 |
| bft2 | certified + sign-always | 109 | 558 | 106 | 424 | 1,7 | 9,2 |

(ms, trung vị 3 rep; min–max theo rep trong `results/e1x/e10_latency.csv`, vd. raft certified read p50 87–108, bft2 certified write p99 371–872.)

- Đọc certified chỉ đắt khi có ghi: ở workload a chờ height kế tiếp được attest (p50 93–112 ms, như E9 a1), ở workload c (chỉ đọc, không có block mới) 1,6 ms so với 1,3–1,4 ms đọc thường.
- Ký mọi phản hồi (`sign-always`) không thêm chi phí tách được khỏi certified (chênh p50 −4…+8 ms, trong khoảng theo rep).
- Đọc quorum (2 `Get` song song) thêm ~0,3–0,4 ms p50 và ~12–21 ms p99 so với đọc thường.
- Write p50/p99 không tách được giữa các chế độ đọc (khoảng theo rep chồng nhau); bft2 certified-signall có 3 `cert_timeouts` ở 1 lượt.
- Đuôi độ trễ E10 (lượt 60 s) không so trực tiếp được với lượt không lỗi của E9 (180 s, độ trễ xấu dần trong lượt, xem E9): cùng cấu hình certified, workload a, p99 gộp (cột `lat_p99_ms`, trung vị 3 rep) của E10 là 401 / 505 ms (raft / bft2), của E9 a1 là 1436 / 1965 ms. Chỉ p50 so được: 91 / 113 so với 82 / 99 ms.

### E11: truy vết 1 node lỗi (f = 1, node4; 3 lượt mỗi ô; không lượt race)

| lỗi | client | backend | phát hiện | detect_evidence_s | failover_s | wrong_gets | rejected_reads | evidence_sent | cert_timeouts | tps |
|---|---|---|---|---|---|---|---|---|---|---|
| F4 | plain | raft / bft2 | 3/3, 3/3 | 0,36 / 0,34 | – | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 440 / 441 |
| F4 | cert-fo | raft / bft2 | 3/3, 3/3 | 0,39 / 0,36 | 0,41 / 0,36 | 0 / 0 | 0 / 0 | 0 / 0 | 351 / 353 | 478 / 478 |
| F4 | cert-fo-sign | raft / bft2 | 3/3, 3/3 | 0,35 / 0,38 | 0,35 / 0,38 | 0 / 0 | 0 / 0 | 0 / 0 | 386 / 464 | 478 / 477 |
| F6 | plain | raft / bft2 | 0/3, 0/3 | – | – | 7115 / 7097 | 0 / 0 | 0 / 0 | 0 / 0 | 480 / 480 |
| F6 | cert-fo | raft / bft2 | 0/3, 0/3 | – | 0,23 / 0,19 | 0 / 0 | 64 / 64 | 0 / 0 | 0 / 5 | 480 / 480 |
| F6 | cert-fo-sign | raft / bft2 | 3/3, 3/3 | 0,37 / 0,37 | 0,30 / 0,30 | 0 / 0 | 17 / 16 | 13 / 10 | 0 / 0 | 480 / 480 |
| F6E | cert-fo-sign | raft / bft2 | 0/3, 0/3 | – | 0,28 / 0,31 | 0 / 0 | 64 / 64 | 0 / 0 | 3 / 0 | 480 / 480 |

(trung bình 3 lượt; `evict_all_s` ≈ `detect_evidence_s` + 0–0,07 s; `evidence_failed` = 0, `honest_evicted` = 0 ở mọi lượt.)

- F4: attestation loại node4 trong 0,34–0,39 s ở mọi lượt, như E9. Không failover thì 1/4 số đọc sau lỗi hỏng (tps 440); có failover thì tps 478, mọi worker chuyển trong ~0,4 s. Cái giá: sau khi loại node4 quorum vẫn 3 (spec 3.3), nên node 1–3 phải ký mọi height và mỗi lượt có 350–465 đọc certified hết giờ chờ chứng chỉ.
- F6 (nói dối khi đọc, trạng thái đúng): đọc thường nhận ~7,1k giá trị sai mỗi lượt, attestation không thấy gì (root đúng). Đọc certified bắt được mọi phản hồi sai (64 = số worker có node chính là node4, mỗi worker bị từ chối một lần rồi chuyển); không ký thì không có bằng chứng, node4 không bị loại. Ký khi nghi ngờ (`on-failure`): 9–17 bằng chứng mỗi lượt, node4 bị cả 4 node loại (`bad_read`) trong 0,29–0,46 s, và `rejected_reads` giảm còn 14–22 vì node bị loại sớm.
- F6E (né ký): như F6 cert-fo — client tự tránh node4 (0 giá trị sai) nhưng không có bằng chứng, node4 không bị loại. Đúng giới hạn nêu ở spec 3.2.

### E12: ngưỡng quorum với f+1 = 2 node lỗi thông đồng (F5-F4, node3 + node4; 3 lượt mỗi ô)

| quorum | đọc | backend | root đúng trước | root sai trước | wrong_gets | honest_evicted | cert_timeouts | tps |
|---|---|---|---|---|---|---|---|---|
| f+1 = 2 | certified | raft | 2 (r1, r3) | 1 (r2) | 0 / 19261 | 0 / 2 | 0–11 | 480 |
| f+1 = 2 | certified | bft2 | 2 (r1, r3) | 1 (r2) | 0 / 19257 | 0 / 2 | 0–8 | 480 |
| f+1 = 2 | quorum | raft | 3 | 0 | 0 | 0 | 0 | 479 |
| f+1 = 2 | quorum | bft2 | 1 (r3) | 2 (r1, r2) | 0 / 19261–19276 | 0 / 2 | 0 | 480 |
| 2f+1 = 3 | certified | raft / bft2 | không root nào được xác nhận | | 0 | 0 | 27,5k / 27,8k | 320 |
| 2f+1 = 3 | quorum | raft / bft2 | không root nào được xác nhận (+1 race raft r1) | | 9658 / 9653 | 0 | 0 | 480 |

(wrong_gets / honest_evicted ghi "lượt root đúng / lượt root sai"; bằng chứng đầu tiên sau 0,21–0,33 s ở mọi lượt f+1.)

- Cách đọc bảng `rq6_summary.csv` ở quorum f+1: `detected_runs` = 3 gồm cả lượt root sai thắng (khi đó người bị loại là node trung thực); loại đúng hay sai phải xem `evicted_all_runs` (mọi node lỗi bị loại) và `honest_evicted`.
- Quorum f+1: kết quả do thứ tự block quyết định, đúng spec 3.1. 4/12 lượt root sai được xác nhận trước (raft 1/6, bft2 3/6): node 1 và 2 bị loại oan (`divergent_root`) sau 0,21–0,33 s, sau đó mọi đọc certified hoặc quorum đều nhận giá trị sai có chứng chỉ hợp lệ (~19,3k mỗi lượt). Các lượt còn lại loại đúng node3 + node4, 0 giá trị sai. Số lượt quá ít để so raft với bft2.
- Quorum 2f+1: không phía nào đủ 3 chữ ký → không xác nhận, không loại ai. Đọc certified an toàn nhưng mất sẵn sàng sau lỗi (~27,5k đọc hết giờ, tps 320, như E9 F5-F4); đọc quorum (f+1 = 2 node) nhận ~9,7k giá trị sai (khi cả hai node được hỏi đều lỗi).
- raft e12-q2f1-quorum r1 là race của việc tiêm lỗi (như E9 bft2 a1 certified r1): ở height 2992 chỉ node4 đã áp lỗi, node 1–3 xác nhận root đúng `8298a09c` (quorum 3) và node4 bị loại ở height 2996; `rq6.py` đánh dấu `injection_race` và bỏ khỏi tóm tắt.

## Giai đoạn 2/3: E9b đối chứng độ trễ của attestation (2026-11-11/12)

Lệnh: `E9_PREFIX=e9b E9_FAULTS=none REPS=5 E9_RATE_RAFT=480 E9_RATE_BFT2=480 experiments/exp/e9_attest.sh` (như E9 nhưng chỉ lượt không lỗi, 5 rep, thứ tự xen kẽ: mỗi pass chạy đủ 10 cấu hình, EXP đầu pass xoay vòng). Commit đo `e54bb11` (host guard có kiểm khoảng trống mẫu; `.params` có `RATE_raft=480`, `RATE_bft2=480`). 16:11–19:07 UTC, 50/50 hàng, `failures in this run: none`; host guard: 1 lượt bị loại (16:25 bft2 a1-plain r1, swap 119k trang; lần thử 2 qua), 0 lượt có khoảng trống mẫu. Nguồn điện: lúc bắt đầu chạy pin (80%); `pmset` ghi đã dùng AC lúc 17:00:55 UTC (pin 70%) và không ghi thời điểm cắm, nên pass 1 (16:11–16:45) và đầu pass 2 có thể đã chạy pin một phần hoặc toàn bộ. Phân tích: `experiments/plot/rq5.py results/raw/e9b-*.csv --raw-dir results/raw --out results/e9b` rồi `experiments/plot/e9b_paired.py results/e9b/rq5_runs.csv --raw-dir results/raw --out results/e9b` → `results/e9b/rq5_{runs,summary}.csv`, `e9b_runs.csv` (thêm queue/block mỗi lượt), `e9b_paired.csv` (hiệu theo pass). Dữ liệu thô: `results/archive/raw-e9b-2026-11-12.tar.gz` (`results/raw/e9b-*`, giải nén `tar -xzf … -C results`).

| backend | attest | đọc | write p50 | write mean | write p99 (min–max) | read p50 | read p99 | queue/block (min–max) |
|---|---|---|---|---|---|---|---|---|
| raft | 0 | plain | 80 | 97 | 721 (417–1657) | 1,1 | 43 | 2,4 (1,4–16,5) |
| raft | 1 | plain | 88 | 99 | 609 (485–913) | 1,1 | 40 | 2,7 (1,4–3,4) |
| raft | 1 | certified | 81 | 92 | 558 (343–693) | 86 | 516 | 2,6 (1,2–3,3) |
| raft | 10 | plain | 81 | 92 | 543 (435–676) | 1,1 | 37 | 1,9 (1,7–2,6) |
| raft | 10 | certified | 82 | 100 | 821 (462–1863) | 211 | 899 | 3,0 (2,3–10,2) |
| bft2 | 0 | plain | 95 | 108 | 696 (519–885) | 1,2 | 45 | 1,0 (0,5–2,1) |
| bft2 | 1 | plain | 100 | 114 | 725 (554–936) | 1,2 | 47 | 1,3 (0,5–1,9) |
| bft2 | 1 | certified | 100 | 113 | 688 (630–848) | 105 | 680 | 1,1 (0,7–2,4) |
| bft2 | 10 | plain | 97 | 114 | 766 (563–1159) | 1,2 | 45 | 0,6 (0,4–3,5) |
| bft2 | 10 | certified | 98 | 112 | 678 (475–846) | 221 | 885 | 1,9 (0,9–2,8) |

(ms; write/read: trung bình 5 rep như `rq5_summary.csv`; queue/block: trung vị 5 rep, mỗi rep trung bình 4 node, từ breakdown snapshot đầu/cuối cửa sổ.)

Hiệu theo pass (cùng pass, cách nhau vài phút; `e9b_paired.csv`, trung vị của 5 hiệu và số pass có hiệu > 0):

| backend | so sánh | write p50 | write mean | write p99 | queue/block |
|---|---|---|---|---|---|
| raft | a1-plain − a0 | +1,7 (4/5) | −0,8 (1/5) | +39 (3/5) | −0,2 (2/5) |
| raft | a10-plain − a0 | −0,9 (1/5) | +0,5 (4/5) | +55 (3/5) | −0,3 (1/5) |
| raft | a1-certified − a1-plain | −8,8 (2/5) | −11,4 (1/5) | +36 (3/5) | −0,2 (1/5) |
| raft | a10-certified − a10-plain | −4,4 (2/5) | −2,1 (2/5) | +136 (4/5) | +1,3 (3/5) |
| bft2 | a1-plain − a0 | +6,1 (4/5) | +11,2 (3/5) | −56 (2/5) | −0,3 (2/5) |
| bft2 | a10-plain − a0 | +2,6 (3/5) | +2,9 (3/5) | −17 (2/5) | −0,4 (2/5) |
| bft2 | a1-certified − a1-plain | +2,2 (3/5) | +1,4 (3/5) | −62 (2/5) | −0,2 (1/5) |
| bft2 | a10-certified − a10-plain | +4,3 (3/5) | −3,3 (2/5) | −113 (2/5) | +0,5 (4/5) |

- **Kết luận cho câu hỏi còn mở của E9:** ở 480 req/s, lượt 180 s, không tách được ảnh hưởng của attestation (K = 1 hay 10) lên độ trễ ghi, kể cả đuôi. Hiệu theo pass đổi dấu giữa các pass ở mọi cặp; độ lớn (write p99 ±40–140 ms, p50 ≤ ~10 ms) nhỏ hơn nhiều so với chênh giữa các rep của chính một cấu hình (vd. raft a0 p99 417–1657 ms, a10-certified 462–1863 ms). Không cặp nào (độ trễ ghi hay queue/block) có hiệu cùng dấu ở cả 5 pass.
- **Không lặp lại được** mức tăng queue/block của E9 (raft 6,6 → 11,0 ms khi bật attestation; certified 14,6 / 23,6 ms): ở E9b queue/block của raft là 1,9–3,0 ms ở mọi cấu hình, của bft2 0,6–1,9 ms. Chỉ còn dấu hiệu nhỏ ở attest 10 + certified (raft +1,3 ms 3/5, bft2 +0,5 ms 4/5), phù hợp với việc proof chạy trong apply loop nhưng không đủ để làm đuôi ghi xấu đi đo được.
- **Lặp lại được:** chi phí đọc certified (read p50 86 / 105 ms ở attest 1, 211 / 221 ms ở attest 10 cho raft / bft2, so với 1,1–1,2 ms đọc thường; read p99 0,5–0,9 s) và write p50 như nhau giữa các mức (raft 80–88, bft2 95–100 ms).
- **E9 khác E9b:** cùng cấu hình, write p99 của E9 là 1,2–2,3 s, E9b 0,5–0,8 s; queue/block của E9 gấp ~3–8 lần. Độ trễ vẫn xấu dần trong lượt nhưng ít hơn (`.wtimeline` 190 s gồm warmup, chia ba phần bằng nhau, sd quần thể của Set/giây ở phần cuối, giây 126–189: E9b 16–96, E9 49–106; theo giây 130–190 như rà soát: E9b 16–98, E9 50–108; mục E9 ở trên ghi 54–108 theo cửa sổ khác; số giây < 120 Set ở 3 phần ba: E9b 2 / 19 / 41 trên 50 lượt, E9 3 / 34 / 74 trên 30 lượt). Nguyên nhân của đuôi ghi cao ở E9 chưa xác định; E9b không cho bằng chứng nó do nguồn điện hay lượt đầu sweep (xem lượt ngoại lệ dưới).
- **Lượt ngoại lệ raft a0-plain rep 1 (16:11:17–16:14:45 UTC, p99 1657 ms, queue/block 16,5 ms):** trùng với tải ngoài từ phiên review (`local_a92c7b91`) khi phiên đó chưa biết E9b đã chạy: `docker run … hbdb-build go test ./demo/web/` trong worktree UI, `python3 -m http.server 8765` và trình duyệt xem trang demo, 16:11:54–16:15:07 UTC. `host-runs.csv` của lượt: CPU ngoài Docker trung bình 124,9%, đỉnh 367,1%, verdict `ok`. Nguyên nhân khả dĩ nhất của ngoại lệ là tải ngoài đó, không quy được cho pin hay lượt đầu sweep. Dòng raft a0 của bảng tóm tắt (p99 721, max 1657; queue/block max 16,5) bị chính lượt này kéo lên.
- **Pass 1 ồn hơn cả pass:** CPU ngoài Docker trung bình của các lượt `ok`: trung vị 49,4% (max 125,5%) ở pass 1, 34,4% (max 192,1%) ở pass 2, 11,6–14,3% ở pass 3–5. Ba lượt pass 1 có ~125%: raft a0-plain r1 (tải của phiên review ở trên), bft2 a1-plain r1 (lần thử 2, sau lượt bị loại vì swap) và raft a1-certified r1; nguồn của hai lượt sau không rõ.
- **Độ nhạy (bỏ pass 1, chỉ pass 2–5):** hai kết luận đầu (độ trễ ghi, queue/block) không đổi. raft a0 có write p99 417–570 ms, queue/block 1,4–2,9 ms. Không cặp attestation/certified nào có hiệu cùng dấu ở cả 4 pass, trừ raft a10-plain − a0 write mean (+4,9 / +7,7 / +0,3 / +0,5 ms, trung vị +2,7 ms), nhỏ hơn nhiều so với chênh giữa các rep.
- **Giới hạn của host guard:** ngưỡng là 200% (tức 2 lõi) trên CPU trung bình ngoài Docker, nên một lượt có 125% tải ngoài vẫn `ok`; `verdict ok` không có nghĩa là host yên.
- **Hệ quả:** các con số đuôi độ trễ ghi tuyệt đối của E9 và E11–E12 chỉ nên đọc trong cùng một sweep; so sánh có ý nghĩa là so trong cùng pass. Câu hỏi chi phí ghi của attestation (E9) đóng lại: **không tách được ở 480 req/s, lượt 180 s**.
- **Tag:** `p7-openloop` và `p10-attest` đặt trên commit sửa NOTES này, sau `p11-accountability`, vì hai tag chờ đối chứng E9b.
