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
- Task 2 (đang dừng, 2026-11-10): E7 lần 1 (`EXP=e7`, lưới mặc định) dừng ở 41/42 + rep thêm. Từ ~02:43 UTC máy quá tải (Teams, `kernel_task` ~150%, VM Docker ~600% CPU kể cả khi không có cụm, load 18–32): rep 3 và bft2 3000/4000 rep 2 chỉ còn ~một nửa thông lượng → **dữ liệu `results/raw/e7*` không dùng được**. Hàng sạch (rep 1–2, trước 02:30): raft và bft2 giữ 500 req/s; ở 1000 thông lượng đạt nhưng write p99 > 2 s (raft 2,3–3,6 s, bft2 2,6–4,3 s); đỉnh raft ~1360–1400 tps, bft2 ~1150–1270 tps.
  - Làm tiếp: người dùng khởi động lại Docker Desktop, máy nguội (kiểm `uptime` load < ~4, VM Docker gần 0% khi không có cụm); chuyển `results/raw/e7.csv`, `e7.params`, `e7/` sang `e7-noisy*`; chạy lại `EXP=e7 BACKENDS="raft bft2" RATES="250 500 600 700 800 900 1000 1500 2000" caffeinate -i experiments/exp/e7_rate.sh` (54 lượt, ~1,7 giờ) trong Terminal panel; phân tích bằng `MPLCONFIGDIR=$TMPDIR/mpl ~/anaconda3/bin/python3 experiments/plot/saturation.py results/raw/e7.csv --ops-dir results/raw --out results/e7` (python hệ thống không có pandas).
  - Rồi `E9_RATE_RAFT`/`E9_RATE_BFT2` = 80% bão hòa → E9 (75 lượt, ~6 giờ); ghi NOTES; tag `p7-openloop`, `p10-attest`.
  - bft2 thỉnh thoảng lỗi khởi động (replica nối nhau trước khi peer lắng nghe; health fail, driver tự chạy lại).
- Task 10: chạy E10, E11, E12 (`E1x_RATE_*` hoặc E9's qua `exp_rate_for`), `rq6.py`, NOTES, tag `p11-accountability`.
- Task 11 (tùy chọn): Kafka approve theo quorum.

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
