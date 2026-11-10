# Giai đoạn 3: tiến độ và bàn giao

Đọc cùng: plan `docs/superpowers/plans/2026-11-09-phase3-accountability.md`, spec `docs/superpowers/specs/2026-11-09-phase3-accountability-design.md`.
Nhánh `claude/wizardly-robinson-fdaa8d` (worktree `.claude/worktrees/wizardly-robinson-fdaa8d`), dựa trên phase 2 `claude/charming-hopper-hy6x4e`. **Chưa push.**
Cập nhật: 2026-11-10 (Task 7 xong, smoke Task 8 đạt).

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
- Task 2: E7 (raft, bft2) → `E9_RATE_*` = 80% bão hòa → E9; ghi NOTES; tag `p7-openloop`, `p10-attest`.
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
