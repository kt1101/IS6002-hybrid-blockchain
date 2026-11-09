# Giai đoạn 3: tiến độ và bàn giao

Đọc cùng: plan `docs/superpowers/plans/2026-11-09-phase3-accountability.md`, spec `docs/superpowers/specs/2026-11-09-phase3-accountability-design.md`.
Nhánh `claude/wizardly-robinson-fdaa8d` (worktree `.claude/worktrees/wizardly-robinson-fdaa8d`), dựa trên phase 2 `claude/charming-hopper-hy6x4e`. **Chưa push.**
Cập nhật: 2026-11-10.

## Task đã xong

| Task | Commit | Ghi chú |
|---|---|---|
| Plan, spec | `3da8460`, `426fc3b`, `3a18f4f` | `3a18f4f`: lệnh test Go dùng bộ gói của README, không dùng `./...` |
| 1 (build, test, consistency) | `7d5ded1`, `83a8aa3` | Docker VM 7,65 GiB → chỉ N=4. Build đạt; test Go ok; consistency 4 backend CONSISTENT |
| 3 (F6 readlie) | `84d47ca` | test Go qua |
| 4 (certifier AddResult, skip) | `5d5a085` | |
| 5 + 6 (Accountant, eviction, signed reads, evidence) | `0a3ad5b` | một commit chung (xem "Khác plan") |
| 8 (harness) | `1f78ebf`, `e5dc2af` | chưa smoke run trên Docker (Step 4) |
| 9 (driver E10–E12, rq6.py) | `fa5f62d` | |
| 7 phần 1 (recorder cột ops, driver API) | `0a14006` | `reader.go` chưa viết |

Trang Notion tiến độ (memory `notion-progress-page`): id `413fc8af-e746-8111-9bc0-fa78f79150a2`, đã cập nhật tới Task 6.

## Task đang làm: Task 7 (client)

Đã có (`0a14006`):
- `veritas/benchmark/recorder.go`: `OpsExtra` + 6 cột cuối `.ops.csv`: `rejected_reads,quorum_errors,failovers,wrong_before_failover,evidence_sent,cert_timeouts` (thêm `cert_timeouts` so với plan, cho RQ7).
- `veritas/driver/driver.go`: `ReadQuorum` (`--read-mode=quorum`; `Get` của driver là Get thường), `GetCertifiedResp(ctx, key, signed)` (trả cả phản hồi khi proof/cert sai), `Evicted`, `ReportEvidence`.

Còn làm (Step 1, 3–5 của Task 7):
1. `veritas/benchmark/ycsbbench/reader.go` + `reader_test.go` theo Interfaces của plan (`nodeAPI`, `readerConfig`, `newReader`, `Read`, `ErrNoNode`, `readStats`). Quyết định đã chọn:
   - Một bộ driver dùng chung **mỗi node một driver** (`ReadMode` plain, `NodeKeys`/`Quorum` cho certified), mỗi worker một `reader` với node chính = node của driver worker (`addrs[i%len(addrs)]` → chỉ số trong `uniqueStrings(addrs)`). Reader có mutex (vòng mở dùng chung), gọi mạng ngoài lock.
   - Chỉ dùng reader khi `--read-mode=quorum` hoặc `--failover` hoặc `--sign-reads != off`; còn lại giữ đường `clis[i].Get` cũ (không đổi hiệu năng phase 2).
   - Bằng chứng chỉ gửi khi lỗi là `ErrProofInvalid` (không gửi khi chỉ cert sai). `cert_timeouts` = `DeadlineExceeded` ở đọc certified.
   - Ghi vẫn gửi node chính của worker.
2. `main.go`: cờ `--failover`, `--sign-reads=off|on-failure|always`, `--read-mode` thêm `quorum`; vòng kín và `runOpenLoop` gọi `reader.Read`; điền 6 trường mới của `OpsExtra`.
3. Test: `go vet` + `go test -race ./veritas/...` trong `hbdb-build`; commit `bench: reader with failover, signed reads, evidence reports and quorum reads`; cập nhật Notion.

## Sau Task 7

- Task 8 Step 4 (smoke): `BACKEND=raft N=4 ATTEST_EVERY=1 READ_MODE=certified FAILOVER=1 SIGN_READS=on-failure FAULT=F6:node4@20 DURATION=40s experiments/run_one.sh` (cần `EXP`, `CLIENTS`, `RATE` theo `run_one.sh`; build lại ảnh trước: `experiments/build.sh`).
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
