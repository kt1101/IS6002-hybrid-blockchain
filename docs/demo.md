# Veritas Live Demo: hướng dẫn vận hành

Tài liệu này dành cho người trình bày buổi bảo vệ. Demo là một trang web (`cmd/demo`) điều khiển một cụm Veritas: gửi giao dịch, tạo tải, kiểm tra Merkle proof và gây lỗi. Thiết kế đầy đủ nằm ở `docs/superpowers/specs/2026-11-02-live-demo-design.md`.

Có hai chế độ:

| Chế độ | Cụm Veritas | Cần gì | Khi nào dùng |
|---|---|---|---|
| `sim` (mặc định) | In-process, mạng giả lập `memnet` | Chỉ Go | Phương án chính trên mọi máy, và phương án dự phòng |
| `docker` | Các container thật (kafka, raft, tendermint hoặc bft2) | Docker, image đã build | Khi muốn trình diễn trên cụm thật (MacBook) |

## 1. Chuẩn bị máy

### Chế độ sim

Chỉ cần Go. Lần chạy đầu `go run` có thể phải tải module Go qua mạng, nên đừng trông vào nó khi không có Wi-Fi: build sẵn binary từ hôm trước (thư mục `bin/` đã nằm trong `.gitignore`):

```bash
go build -o bin/demo ./cmd/demo
./bin/demo
```

`./bin/demo` nhận đúng các cờ như `go run ./cmd/demo`. Build lại sau mỗi lần code hoặc `overview.html` thay đổi.

### Chế độ docker

1. Build image (một lần, hoặc khi code thay đổi):

   ```bash
   experiments/build.sh
   ```

   Lệnh này build `hbdb-build`, `hbdb-bft2-build`, rồi `hbdb-node` và `hbdb-kafka`.

2. Dựng cụm bằng `demo_up.sh`:

   ```bash
   BACKEND=bft2 N=4 experiments/demo_up.sh
   ```

   - `BACKEND` là bắt buộc: `kafka`, `raft`, `tendermint` hoặc `bft2`. Các backend `kafka-orig` và `tm-orig` bị từ chối vì không có observer cho demo.
   - Script đặt `PUBLISH_PORTS=1 COMMIT_MODE=sync BATCH_FLUSH=200ms`, nên các cổng của node được mở ra `127.0.0.1` và giao dịch gõ tay commit nhanh (flush mỗi 200 ms).
   - Script "arm" sẵn lỗi mà demo không thể thêm về sau: `node3` ở chế độ corrupt (khi N >= 3), và với `kafka` thêm `node4` ở chế độ silent (khi N >= 4).
   - Script chờ `health.sh` báo cụm sẵn sàng, rồi in đúng lệnh chạy demo, ví dụ `go run ./cmd/demo --mode=docker --backend=bft2 --n=4`. Nếu N nhỏ hơn 3 (hoặc kafka với N nhỏ hơn 4) lệnh in ra có thêm `--armed-corrupt=none` / `--armed-silent=none`.

3. Chạy lệnh demo mà script vừa in (xem mục 2).

4. Khi xong, dọn cụm (xoá container, network và volume):

   ```bash
   experiments/cluster/down.sh
   ```

Các cổng trên máy host (docker mode):

| Thành phần | Địa chỉ |
|---|---|
| gRPC của node i | `127.0.0.1:19900+i` (node1 = 19901, ...) |
| Observer của node i | `127.0.0.1:19800+i` |
| TSO (kafka và raft) | `127.0.0.1:19870` |

## 2. Chạy demo

### Sim

```bash
go run ./cmd/demo
```

Mặc định: `--mode=sim`, `--listen 127.0.0.1:8080`, `--n 4`, `--slow-ms 200`. Mở http://127.0.0.1:8080. Nút "Results overview" trên trang dẫn tới `/overview` (dashboard kết quả thí nghiệm, chạy offline).

Tuỳ chọn: `--listen 127.0.0.1:9090` đổi cổng web, `--n` đổi số node, `--slow-ms` đổi độ trễ thêm của lỗi slow.

### Docker

Sau khi `demo_up.sh` báo cụm đã sẵn sàng:

```bash
go run ./cmd/demo --mode=docker --backend=bft2 --n=4
```

Các cờ riêng của docker mode (đều có mặc định hợp lý, thường không cần đặt):

| Cờ | Mặc định |
|---|---|
| `--backend` | bắt buộc: `kafka`, `raft`, `tendermint`, `bft2` |
| `--n` | số node của cụm đang chạy (4) |
| `--grpc-base` / `--events-base` | 19900 / 19800 |
| `--tso-addr` | `127.0.0.1:19870` với kafka và raft; không có TSO với tendermint và bft2 (`none` = tắt) |
| `--fault-script` | `experiments/demo_fault.sh` |
| `--armed-corrupt` | `node3` (`none` = không node nào) |
| `--armed-silent` | `node4` với kafka, không node nào với backend khác |

Nếu không node nào trả lời, demo thoát (mã 2) và nhắc lệnh `BACKEND=... N=... experiments/demo_up.sh`.

### Lỗi trong chế độ docker

Hành động lỗi được thực hiện bởi `experiments/demo_fault.sh` (gọi tự động từ nút trên UI).

| Lỗi | Gỡ được (Heal)? | Ghi chú |
|---|---|---|
| crash | Không | Cần dựng lại cụm bằng `demo_up.sh` |
| corrupt | Không | Chỉ chạy trên node đã arm (node3). Bấm trên node khác thì UI hiện thông báo lỗi của script |
| silent | Có, trừ kafka | Kafka: chỉ trên node4 đã arm, không gỡ được. Backend khác: chặn cổng consensus bằng iptables, Heal gỡ luật |
| slow | Có | Thêm 200 ms độ trễ ra khỏi node |

Muốn làm lại kịch bản trong docker mode: `experiments/cluster/down.sh`, rồi `demo_up.sh` lại.

### Lỗi trong chế độ sim

- crash, silent, slow gỡ được bằng Heal (với crash, bấm Heal sẽ thấy node catch-up các block bị lỡ).
- corrupt chỉ xoá được bằng nút "Reset cluster" (khởi động lại cụm rỗng).

## 3. Kịch bản 5 bước

Thanh kịch bản trên UI có 5 nút. Mỗi nút gọi đúng các API mà thao tác tay cũng dùng (không có đường tắt riêng), nên có thể vừa bấm nút vừa thao tác tay. Lời nói dưới đây chỉ là gợi ý.

### Bước 1: Write a transaction

UI điền sẵn `user42 = hello-committee` và gửi tới `node2`.

- "Em gửi một giao dịch Set đơn giản: khoá user42, giá trị hello-committee, tới node2."
- "Giao dịch đi vào hàng đợi consensus, được xếp thứ tự rồi mọi node thực thi như nhau."
- Chỉ vào kết quả hiện ra dưới nút Submit Set và dòng mới trong log.

### Bước 2: Replicate & commit

UI bật tải 64 client YCSB-A trong 60 giây. Khoá của tải là `ycsb0` đến `ycsb999`, nên `user42` không bị tải đụng tới. Lần bật tải đầu tiên, demo nạp trước 1.000 khoá (không tính vào số đo): nút và dòng trạng thái hiện "Preloading 1,000 keys…" vài giây (docker lâu hơn) rồi tx/s mới tăng.

- "Giờ em bật 64 client chạy workload YCSB-A trong 60 giây để thấy hệ thống chịu tải."
- Chỉ vào biểu đồ throughput (tx/s), p50, p99 và dãy block mới xuất hiện bên phải.
- "Các replica đều ở cùng chiều cao, cùng root: banner đang CONSISTENT."

### Bước 3: Verify a proof

UI chuyển sang màn Merkle proof, hỏi `user42` ở `node2`.

- "Người dùng không cần tin node nào: node trả về giá trị cùng đường đi Merkle proof, client tự tính lại root."
- Chỉ vào verdict (proof hợp lệ), đường đi (proof path) và bảng replica: các node cho cùng giá trị `hello-committee`.
- "Root này khớp với root của block đã commit."
- Đang có tải nên các replica có thể lệch nhau một block trong chốc lát. Verify hỏi mọi replica cùng lúc và hỏi lại vài lần; replica vẫn chậm thì hiện `valid · lagging` (cùng giá trị, root cũ hơn: đang bắt kịp, không phải gian lận). Chỉ replica có giá trị khác đa số mới hiện `valid · minority root`.

### Bước 4: Inject a fault

UI làm node3 corrupt, rồi gửi lại `user42 = hello-committee` tới node2.

- "Em cho node3 bị lỗi: nó thực thi sai trạng thái nhưng vẫn chạy."
- "Em gửi lại user42 để thấy rõ: node3 hiện hello-committee-CORRUPT, các node còn lại hiện hello-committee."
- Chỉ vào banner chuyển sang DIVERGENT, node3 có trạng thái Divergent và root của nó khác.
- Có thể nói thêm: crash thêm một node (ví dụ node4) bằng bảng Inject a fault để cho thấy cụm vẫn chạy khi một node ngừng.

### Bước 5: Detect & recover

UI hiện khung Recover và làm nổi banner.

- "Sai lệch lộ ra ngay vì mỗi replica công bố root sổ cái sau mỗi block, và hub của demo (giống auditor) so các root đó ở cùng chiều cao: đa số đồng ý root A, node3 lệch. Một client cũng tự phát hiện được bằng Verify: hỏi mọi replica và so root."
- Sim: bấm Heal trên node bị crash/silent/slow để cho thấy node catch-up; bấm "Reset cluster" để làm lại từ đầu. Nêu rõ corrupt không gỡ được.
- Docker: nêu rằng corrupt và crash không gỡ được từ UI, cần dựng lại cụm bằng `demo_up.sh`.
- "Điểm mấu chốt: tính toàn vẹn kiểm chứng được bằng root và proof, không dựa vào niềm tin ở một node."

## 4. Checklist kiểm tra trước buổi bảo vệ

Chế độ docker không test tự động được trong môi trường phát triển (không có Docker daemon). Hãy chạy checklist này bằng tay trên MacBook, ít nhất một ngày trước buổi bảo vệ, và lại lần nữa trước giờ trình bày.

Chuẩn bị:

- [ ] Hôm trước: `go build -o bin/demo ./cmd/demo`, rồi tắt mạng (Wi-Fi) và chạy `./bin/demo`, đi đủ 5 bước kịch bản ở sim mode. Đây là phương án dự phòng, phải chạy được khi không có mạng.
- [ ] `experiments/build.sh` chạy xong, không lỗi.
- [ ] `BACKEND=bft2 N=4 experiments/demo_up.sh` in ra lệnh `go run ./cmd/demo ...` (và `health.sh` không báo lỗi).
- [ ] Chạy lệnh demo đó, mở http://127.0.0.1:8080, thanh trên cùng hiện nút "Docker cluster" được chọn và nhãn cụm (ví dụ `bft2 · N=4 · f=1`), 4 node đều Up.

Kiểm tra từng chức năng:

- [ ] Giao dịch commit trên cả 4 node: gửi `user42 = hello-committee` tới node2, bảng replica thấy 4 node cùng giá trị, banner CONSISTENT.
- [ ] Verify: màn Merkle proof với `user42` ở node2 cho verdict hợp lệ và đường đi proof.
- [ ] Load: bật tải 64 client YCSB-A, thấy tx/s tăng, block mới liên tục, không có lỗi.
- [ ] Corrupt node3: node3 hiện `hello-committee-CORRUPT` sau khi gửi lại `user42`, banner chuyển DIVERGENT.
- [ ] Corrupt trên node khác (ví dụ node2): UI hiện thông báo lỗi của script (node chưa được arm), không treo.
- [ ] Slow: bấm slow trên một node, độ trễ tăng; bấm Heal, trở lại bình thường.
- [ ] Silent (không phải kafka): bấm silent trên một node, node bị đánh dấu lỗi; bấm Heal, node hoạt động lại.
- [ ] Crash node4: node4 hiện trạng thái Down, banner nêu node không liên lạc được; xác nhận Heal không khả dụng (cần dựng lại cụm).
- [ ] Sau khi thử xong: `experiments/cluster/down.sh`, dựng lại cụm sạch bằng `demo_up.sh`, và chạy kịch bản 5 bước một lần từ đầu đến cuối.

Kiểm tra tự động (chạy được ở bất kỳ máy nào có Go và Python với pytest; hai smoke test `node ...` cần Node và Playwright, xem mục 6):

```bash
go test -race ./demo/... ./veritas/core/ ./veritas/observe/ ./veritas/ledger/... ./consensus/memnet/
python3 -m pytest -q experiments/cluster
python3 -m pytest -q experiments/plot/test_build_dashboard.py
bash experiments/test_demo_scripts.sh
node demo/web/smoke.cjs
node experiments/plot/smoke_dashboard.cjs
```

## 5. Phương án dự phòng

Dùng theo thứ tự, khi phương án trước gặp sự cố:

1. Docker mode lỗi (Docker không chạy, cụm không lên, mạng): chuyển sang sim mode bằng binary đã build sẵn, `./bin/demo` (không cần mạng; `go run ./cmd/demo` có thể phải tải module). Kịch bản 5 bước giống hệt, không cần Docker. Trước khi chuyển nhớ `experiments/cluster/down.sh` để giải phóng cổng.
2. Sim mode lỗi (hiếm): mở `/overview` ở demo đang chạy (nếu web server còn chạy), hoặc mở thẳng file `results/dashboard.html` bằng trình duyệt. Đây là dashboard offline, không cần mạng và không cần server.
3. Dashboard cũng không mở được: dùng các hình PNG trong `results/figs/` (ví dụ `e1_tps_vs_clients.png`, `e2_tps_vs_n.png`, `e6_lat_by_fault_n4.png`), kèm `results/figs/rq3_summary.csv`.

## 6. Ghi chú kỹ thuật

### Dựng lại dashboard kết quả

Dashboard (`results/dashboard.html`) và trang `/overview` của demo (`demo/web/static/overview.html`) được sinh từ cùng một lệnh, đọc `results/final/*.csv` và `results/figs/rq3_summary.csv`:

```bash
python3 experiments/plot/build_dashboard.py \
    --final results/final \
    --rq3 results/figs/rq3_summary.csv \
    --out results/dashboard.html \
    --out demo/web/static/overview.html
```

`--out` có thể lặp lại; mọi file nhận cùng nội dung. Chỉ dùng thư viện chuẩn của Python. Sau khi sinh lại `overview.html`, build lại demo (`go run ./cmd/demo` tự biên dịch lại) vì file được nhúng vào binary bằng `go:embed`.

### Smoke test cần Playwright

`node demo/web/smoke.cjs` chạy `cmd/demo --mode=sim`, bấm qua các bước kịch bản trên Chromium và kiểm tra không có lỗi console. `node experiments/plot/smoke_dashboard.cjs` mở dashboard và chuyển qua mọi tab. Cả hai đều `require('playwright')`, nên Playwright (và Chromium) phải tìm thấy được từ `node`: cài global (`npm i -g playwright`) và nếu cần đặt `NODE_PATH="$(npm root -g)"` trước lệnh `node`. Các smoke test này không nằm trong `go test` hay CI.
