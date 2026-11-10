# Giai đoạn 3: tiến độ và bàn giao

Đọc cùng: plan `docs/superpowers/plans/2026-11-09-phase3-accountability.md`, spec `docs/superpowers/specs/2026-11-09-phase3-accountability-design.md`.
Nhánh `claude/wizardly-robinson-fdaa8d` (worktree `.claude/worktrees/wizardly-robinson-fdaa8d`), dựa trên phase 2 `claude/charming-hopper-hy6x4e`. **Chưa push.**
Cập nhật: 2026-11-10 (Task 7 xong, smoke Task 8 đạt, E7 dừng do máy quá tải).

## Task đã xong

| Task | Commit | Ghi chú |
|---|---|---|
| Plan, spec | `3da8460`, `426fc3b`, `3a18f4f` | `3a18f4f`: lệnh test Go dùng bộ gói của README, không dùng `./...` |
| 1 (build, test, consistency) | `7d5ded1`, `83a8aa3` | Docker VM 7,65 GiB → chỉ N=4. Build đạt; test Go ok; consistency 4 backend CONSISTENT |
| 3 (F6 readlie) | `84d47ca` | test Go qua |
| 4 (certifier AddResult, skip) | `5d5a085` | |
| 5 + 6 (Accountant, eviction, signed reads, evidence) | `0a3ad5b` | một commit chung (xem "Khác plan") |
| 8 (harness) | `1f78ebf`, `e5dc2af` | smoke Step 4 đạt (NOTES "smoke test truy vết") |
| 9 (driver E10–E12, rq6.py) | `fa5f62d` | |
| 7 phần 1 (recorder cột ops, driver API) | `0a14006` | |
| 7 phần 2 (`reader.go`, cờ ycsbbench) | xem `git log` | `go vet` + `go test -race ./veritas/...` qua |

Trang Notion tiến độ (memory `notion-progress-page`): id `413fc8af-e746-8111-9bc0-fa78f79150a2`, đã cập nhật tới Task 7.

## Task 7 (client): xong

- `veritas/benchmark/ycsbbench/reader.go`: `reader` (một cho mỗi worker vòng kín; vòng mở một cho mỗi khe driver, có mutex), driver dùng chung mỗi node một cái. Chỉ dùng khi `--read-mode=quorum`, `--failover` hoặc `--sign-reads != off`.
- Quyết định: lỗi chứng chỉ (`ErrCertInvalid`) chỉ chuyển node, không đánh dấu nghi ngờ (node có thể chậm); lỗi proof đánh dấu nghi ngờ. Đọc lại có ký trả đúng (F6E) → trả giá trị đó, node vẫn bị nghi ngờ. `cert_timeouts` chỉ đếm khi context của lượt chạy còn sống. Không có `--failover` thì reader luôn ở node chính. `--sign-reads != off` cần `--read-mode=certified`.
- Quorum: lỗi `DataLoss`/`PermissionDenied` của một node trong bộ → loại node đó, thử bộ kế tiếp; lỗi khác trả ngay. Hai bộ khác nhau → `ErrQuorumMismatch`.

## Sau Task 7

- ~~Task 8 Step 4 (smoke)~~: đạt 2026-11-10 (`EXP=smoke-p3`): node 4 bị loại `bad_read` cùng height 378 trên 4 node, `wrong_gets` = 0, `evidence_sent` = 2, `failovers` = 4.
- Task 2, E7 xong (2026-11-10, `51a4c72`, `0d17ae6`): raft và bft2 `sat_rps` 1000, `slo_rps` 600, **`E9_RATE_RAFT=480`, `E9_RATE_BFT2=480`** (`results/e7/saturation.csv`). Làm tiếp sau review: build + test Go mục 5–8, fast-forward nhánh `-p3fix` vào nhánh chính, build ảnh, rồi E9 (75 lượt, ~6 giờ).
- Task 2 (lần đầu, dừng 2026-11-10): E7 lần 1 (`EXP=e7`, lưới mặc định) dừng ở 41/42 + rep thêm. Từ ~02:43 UTC máy quá tải (Teams, `kernel_task` ~150%, VM Docker ~600% CPU kể cả khi không có cụm, load 18–32): rep 3 và bft2 3000/4000 rep 2 chỉ còn ~một nửa thông lượng → **dữ liệu `results/raw/e7*` không dùng được**. Hàng sạch (rep 1–2, trước 02:30): raft và bft2 giữ 500 req/s; ở 1000 thông lượng đạt nhưng write p99 > 2 s (raft 2,3–3,6 s, bft2 2,6–4,3 s); đỉnh raft ~1360–1400 tps, bft2 ~1150–1270 tps.
  - Làm tiếp: người dùng khởi động lại Docker Desktop, máy nguội (kiểm `uptime` load < ~4, VM Docker gần 0% khi không có cụm); chuyển `results/raw/e7.csv`, `e7.params`, `e7/` sang `e7-noisy*`; chạy lại `EXP=e7 BACKENDS="raft bft2" RATES="250 500 600 700 800 900 1000 1500 2000" caffeinate -i experiments/exp/e7_rate.sh` (54 lượt, ~1,7 giờ) trong Terminal panel; phân tích bằng `MPLCONFIGDIR=$TMPDIR/mpl ~/anaconda3/bin/python3 experiments/plot/saturation.py results/raw/e7.csv --ops-dir results/raw --out results/e7` (python hệ thống không có pandas).
  - Rồi `E9_RATE_RAFT`/`E9_RATE_BFT2` = 80% bão hòa → E9 (75 lượt, ~6 giờ); ghi NOTES; tag `p7-openloop`, `p10-attest`.
  - bft2 thỉnh thoảng lỗi khởi động (replica nối nhau trước khi peer lắng nghe; health fail, driver tự chạy lại).
- Task 10: chạy E10, E11, E12 (`E1x_RATE_*` hoặc E9's qua `exp_rate_for`), `rq6.py`, NOTES, tag `p11-accountability`.
- Task 11 (tùy chọn): Kafka approve theo quorum.

## Rà soát 10 điểm (2026-11-10, từ phiên gốc)

Làm trên nhánh `claude/wizardly-robinson-fdaa8d-p3fix` (worktree phụ `/tmp/claude-501/p3fix`) trong lúc E7 chạy: harness ghi `git describe --dirty`, nên sửa file được theo dõi trong worktree chính sẽ làm các hàng E7 sau đó thành `7461028-dirty`, còn chạy test Go trong Docker sẽ làm nhiễu E7. Sau E7: build ảnh, `go vet` + `go test -race`, `test_faults.sh`, rồi fast-forward nhánh này vào `claude/wizardly-robinson-fdaa8d` trước E9.

1. **Guard nhiễu host** (`0251c81`): load average không tách được nhiễu (cụm sạch đã giữ load1 ~10–13; lượt hỏng 13–20). Tín hiệu dùng: %CPU (ps) của tiến trình ngoài Docker VM (~130% khi máy rảnh, lượt hỏng ~300% vì Teams) và swap-out của host (lượt hỏng swap ~66k trang/phút, hiện 0). `run_one.sh` chờ tối đa `HOST_BUSY_WAIT_SECS` rồi thoát 4 (driver dừng batch); lấy mẫu `<tag>.host` mỗi 5 s; ghi `results/raw/<EXP>/host-runs.csv` (load1_max, CPU ngoài Docker, swapouts_delta, verdict); `noisy` → exit 3, không có hàng. CSV chính giữ 23 cột (ycsbbench ghi header; thêm cột sẽ khóa các `<EXP>.csv` cũ). `host.log` cũ vẫn ghi. `test_faults.sh` 464/0.
2. **So rep với rep 1**: E7 lại xong 16:01 UTC, 56 hàng, không lượt hỏng; thông lượng CV ≤ 0,005 (trừ bft2 2000: 0,098, 5 rep); write p99 lệch giữa rep ở 700–1000 nhưng host sạch (swap đứng yên, load1 ≤ 13 trừ bft2 2000) nên không dừng sớm (NOTES "E7 quét tốc độ trên M3"). `e7-noisy` cả rep 1–2 cũng không dùng (raft 1000 req/s khi đó p99 2,3–3,6 s, nay < 1 s; compressor host 6,5 GB so với 4 GB).
3. **Tốc độ E9 theo độ trễ** (`4c86bc4`): `saturation.py` thêm `slo_rps` (rate cao nhất mà nó và mọi rate thấp hơn đều đạt tps ≥ 0,95R, write p99 ≤ `--slo-ms` 1000, không drop) và `e9_rate` = 80% `slo_rps`. `sat_rps` giữ định nghĩa giai đoạn 2.
4. **Abort**: không sửa code. `.ops.csv` đã tách `errors`/`aborted`; chỉ cột `errors` của CSV chính gộp cả hai. raft (và kafka) chạy `VersionClient`: phiên bản TSO, Set có phiên bản ≤ phiên bản đã lưu bị abort (stale) khi thứ tự đồng thuận khác thứ tự TSO, tăng theo tải; bft2/tendermint chạy `VersionOrdered` (phiên bản = thứ tự block), theo thiết kế không bao giờ abort (`veritas/core/executor.go`). Ghi trong NOTES.
5. **Khử trùng bằng chứng**: khóa = (culprit, `sigs.ReadDigest` = sha256 của các byte được ký), không theo (culprit, height, key): hai read khác nhau cùng key/height đều phải giữ (khử theo key có thể bỏ đúng read sai). attestor nhớ 4096 khóa gần nhất (gửi lại → ack, bỏ), hàng đợi tối đa 256 (`ResourceExhausted`). Test `evidence_queue_test.go`.
6. **RecheckHeld**: held theo height, chữ ký kiểm một lần khi giữ; RecheckHeld chỉ tra cert theo height (tăng dần, cùng thứ tự trên mọi node) và kiểm proof Merkle; height dưới cửa sổ bị bỏ; khi đầy bỏ height thấp nhất; bản sao đã giữ bị từ chối.
7. **Quorum sau khi loại**: đúng (quorum cố định, node bị loại chỉ bị bỏ qua). Không đổi giao thức (cần mọi node đổi cùng vị trí block, client đổi theo `--attest-quorum`); ghi giới hạn ở spec 3.3 và 11, test `TestQuorumFixedAfterEviction`. Cấu hình E11 kết hợp (F6 rồi một node chậm) cần harness gắn nhiều loại fault mỗi lượt: để sau.
8. **failover_s**: ycsbbench ghi `first_failover_unix_ms` (cột cuối `.ops.csv`, đồng hồ client, 0 = không có); `rq6.py` tính `failover_s` = nó − thời điểm tiêm, ghi chú nếu failover trước khi tiêm; có trong summary.
9. **Race khởi động bft2**: chưa sửa. `Start` đã chờ TCP của mọi peer (`waitTCP`) trước `Connect`; lần lỗi duy nhất (02:43, lúc máy quá tải) cả 4 node lỗi trong cùng ~0,3 ms (mỗi node bị một node khác từ chối): một node `Connect` lỗi → `log.Fatalf` → listener đóng → các node khác lỗi dây chuyền; không xác định được lỗi đầu. `hscfg` gắn vào core lúc Build nên không thử lại trong tiến trình được. Đề xuất: `bft2-node` thoát mã riêng (75) khi `Start` lỗi và script khởi động container chạy lại có giới hạn; cần thử trên cụm thật (sau E9).
10. **Log ở gốc worktree** (`d83bb45`): `.gitignore` thêm `/*.log` và `results/consistency/`.

## Môi trường và cách chạy lệnh

- Sandbox Bash của session **chặn Docker** (`/Users/yuji/.docker/run/docker.sock`); máy không có Go. Người dùng **cho phép chạy lệnh Docker trong Terminal panel** (`mcp__terminal__run_in_terminal`) ở session này. Mẫu: `docker run --rm -v "$PWD":/src -w /src hbdb-build sh -c 'go vet <pkgs> && go test -race <pkgs>' > taskN.log 2>&1; echo "taskN done: $?"`, rồi đọc log (Monitor chờ dòng ok/FAIL).
- Bộ gói test: `./consensus/... ./veritas/... ./cmd/auditor ./cmd/veritas`; `bft2node`: `-v "$PWD"/bft2node:/src ... hbdb-bft2-build go test -race ./...`.
- Sinh proto: `scripts/genproto_docker.sh` (đã chạy cho Task 5+6; proto đã có mọi message giai đoạn 3).
- Python trên host không có pytest/pandas: chạy test qua shim trong scratchpad (`$TMPDIR/pyshim/pytest.py`, `runtests.py`); shell test: `experiments/test_faults.sh` (451/0), `experiments/exp/test_exp.sh` (282/0, chạy > 2 phút → chạy nền).
- `gofmt -l` luôn liệt kê 5 file gốc không sửa (`veritas/benchmark/util.go`, `veritas/server.go`, `cmd/kafkabench`, `cmd/mongodb`, `cmd/veritastm-mongodb`).
- Dữ liệu YCSB: `results/ycsb/*.dat` chép từ checkout chính (gitignore).
- File log chưa commit trong worktree (`task*.log`, `gotest.log`, `consistency.log`, `results/consistency/`): không commit.
- Không push, không mở PR khi người dùng chưa đồng ý.

## Khác plan/spec

- `Accountant.Skip` cắt theo **thứ tự block** (mọi attestation của node bị loại xử lý sau khi loại đều bỏ), không theo height được ký: cắt theo height làm cả hai phía bị loại ở F5-F4 quorum f+1 (đã sửa spec 3.3 và plan Task 5).
- Proto của Task 5 và 6 sinh một lần; Task 5 và 6 commit chung.
- Node bị loại trả `PermissionDenied` trước `DataLoss`; hai test giai đoạn 2 chấp nhận cả hai.
- `run_one.sh` truyền `--attest-quorum=<q>` cho cả `ycsbbench` (client kiểm chứng chỉ cùng quorum với node).
- `rq6.py` chỉ dùng thư viện chuẩn (chạy được trên host không pandas); `failover_s` luôn rỗng (ycsbbench không ghi thời điểm chuyển node).
- Driver E10–E12: `ATTEST_EVERY=1` cho mọi chế độ của E10; E12 dùng `FAILOVER=1`; tốc độ lấy `exp_rate_for E1x <backend>` (rơi về `E9_RATE_*`).
- F6E (`readlie-evasive`) là kind riêng trong `faults.sh` (`F6E:node4@60`).
