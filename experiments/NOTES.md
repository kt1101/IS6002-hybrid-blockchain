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
