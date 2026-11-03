# Giai đoạn 2: so sánh công bằng và chi phí của tính kiểm chứng được — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Đo lại RQ1/RQ2 ở thông lượng bão hòa với tài nguyên cân bằng và adapter đã sửa, rồi hiện thực và đo chữ ký Ed25519, đọc có kiểm chứng (RQ4) và xác nhận root trong giao thức (RQ5).

**Architecture:** Tải vòng mở nằm trong `veritas/benchmark` và `ycsbbench`. Tài nguyên và tham số đi qua `run_one.sh` → `up.sh` → `gen_compose.py` / `tm_config.py`. Sửa adapter nằm trong `bft2node/` và `consensus/tendermint`. Chữ ký và proof là một gói mới `veritas/sigs` cùng RPC `GetVerified`. Attestation là một `Certifier` trong `veritas/core`, gửi qua `Consensus.Submit` nên không phụ thuộc backend.

**Tech Stack:** Go 1.20 (module chính), Go 1.23 (`bft2node/`), `crypto/ed25519`, protobuf APIv1 (`github.com/golang/protobuf` v1.5.3, `protoc-gen-go` v1.3.5 với `plugins=grpc`), bash, Python 3 (pandas, matplotlib), Docker.

**Spec:** `docs/superpowers/specs/2026-11-03-phase2-fair-comparison-and-verifiability-design.md`

## Global Constraints

- Module chính giữ `go 1.20` và golang:1.20 trong `hbdb-build`; `bft2node/` giữ module riêng golang:1.23.
- Mọi thí nghiệm giai đoạn 2: `COMMIT_MODE=sync`, `BATCH_FLUSH=100ms`, `MEM=1g`, `CPUS=0.5`, 3 rep, quy tắc CV > 10% → tối đa 2 rep thêm.
- CSV mới có 23 cột: 22 cột cũ + `offered_rps` (cuối). Đọc CSV 22 cột cũ phải vẫn chạy (`offered_rps` = 0).
- Ngưỡng bão hòa: tps ≥ 0,95 × tốc độ gửi, write p99 ≤ 2000 ms, `dropped` = 0.
- Quorum attestation `Q = 2f+1`, `f = floor((N-1)/3)`; giữ attestation của 1024 height gần nhất; chờ xác nhận tối đa 5 s; idle fallback 100 ms.
- bft2: cổng chuyển tiếp 7202; requeue sau 8 view; khử trùng 65536 id. Tendermint: hàng đợi Deliver 128 MiB.
- Bytes chuẩn hóa để ký Set: `key 0x00 value 0x00 version(int64 big-endian, 8 byte)`; ký attestation: `"veritas-attest" 0x00 height(8 byte BE) root`.
- Mặc định của mọi cờ mới phải giữ hành vi giai đoạn 1 (`--rate=0`, `--verify-sigs=none`, `--read-mode=plain`, `--attest-every=0` = tắt).
- Không đổi file đã commit trong `results/final/` và `results/cloud-bf100ms/`.

## Review Focus

1. **Proof khớp root:** `GetVerified` trong lúc đang apply block phải trả value/version/proof/root của cùng một height. Test: `TestGetVerifiedConsistentUnderApply` (Task 7).
2. **Tốc độ vòng mở khi server chậm:** bộ lập lịch không được tự giảm tốc độ khi request chậm. Thao tác vượt `--max-inflight` phải bị bỏ và đếm, không được xếp hàng. Test: `TestOpenLoopDropsAboveInflightCap` (Task 1).
3. **Block trùng ở bft2:** hai leader liên tiếp đề xuất cùng một block chờ thì block đó chỉ được `Deliver` một lần. Test: `TestCommitLogSkipsDuplicateIDs` (Task 4).
4. **Attestation giả hoặc trùng:** cùng một node gửi 3 attestation cho cùng height không được tính là quorum. Chữ ký của khóa không có trong cấu hình bị bỏ. Test: `TestCertifierIgnoresDuplicateSigner`, `TestCertifierRejectsUnknownKey` (Task 8).
5. **CSV cũ:** `plot.py`, `assemble_final.py`, `experiments/cloud/assemble.py` đọc được cả `results/final/*.csv` (22 cột) lẫn CSV mới (23 cột). Test: `test_load_csvs_accepts_old_and_new_header` (Task 1).

---

## P7 — Tải vòng mở và tài nguyên công bằng

### Task 1: Tải vòng mở trong `ycsbbench` và cột `offered_rps`

**Files:**
- Create: `veritas/benchmark/openloop.go`, `veritas/benchmark/openloop_test.go`
- Modify: `veritas/benchmark/recorder.go` (cột `offered_rps`, bộ đếm `Dropped`), `veritas/benchmark/recorder_test.go`, `veritas/benchmark/ycsbbench/main.go`, `experiments/lib.sh:40` (`HBDB_CSV_HEADER`), `experiments/run_one.sh` (env `RATE`, `MAX_INFLIGHT`), `experiments/plot/plot.py` (`EXPECTED_COLUMNS`, `_read_one`), `experiments/plot/test_plot.py`

**Interfaces:**
- Produces:
  - `type OpenLoop struct{ Rate float64; MaxInflight int }`.
  - `func (o OpenLoop) Run(ctx context.Context, next func() *Request, do func(ctx context.Context, r *Request, scheduled time.Time)) (sent, dropped int64)`: phát `next()` ở thời điểm `start + i/Rate`. Nếu số `do` đang chạy ≥ `MaxInflight` thì bỏ thao tác và tăng `dropped`. `do` chạy trong goroutine riêng.
  - `Recorder.ObserveDropped()` và `Summary.Dropped int64`.
  - Cột CSV `offered_rps`, lấy từ tag `offered_rps`.
  - `ycsbbench --rate=FLOAT` (0 = vòng kín như cũ), `--max-inflight=INT` (mặc định 4096). Độ trễ = `time.Since(scheduled)`.

- [ ] **Step 1: Viết test hỏng**
  - `TestOpenLoopRate`: Rate=1000, 2 s, `do` trả về ngay → `sent` ∈ [1900, 2100], `dropped == 0`.
  - `TestOpenLoopDropsAboveInflightCap`: Rate=1000, MaxInflight=10, `do` chặn 1 s → trong 0,5 s thì `dropped >= 400`, và số `do` chạy đồng thời tối đa ≤ 10.
  - `TestOpenLoopLatencyFromSchedule`: `do` ghi `time.Since(scheduled)`; khi MaxInflight=1 và `do` mất 50 ms, độ trễ ghi nhận của các thao tác không bị bỏ phải ≥ 50 ms.
  - `TestAppendCSVRowOfferedRps`: header có 23 cột, cột cuối là `offered_rps`, và giá trị bằng tag.
  - Python `test_load_csvs_accepts_old_and_new_header`: một file 22 cột cho `offered_rps == 0`, một file 23 cột giữ nguyên giá trị.
- [ ] **Step 2: Chạy, xác nhận FAIL.** Lệnh: `docker run --rm -v $PWD:/src -w /src hbdb-build go test ./veritas/benchmark/ -run 'OpenLoop|OfferedRps' -v`, rồi `python3 -m pytest experiments/plot -q -k old_and_new`.
- [ ] **Step 3: Cài đặt.**
  - Bộ lập lịch dùng `time.Timer` theo thời điểm tuyệt đối (không cộng dồn `Sleep`) và một semaphore kênh cỡ `MaxInflight`.
  - Trong `main.go`: nếu `--rate > 0` thì thay vòng worker vòng kín bằng `OpenLoop.Run`. Driver được chọn vòng tròn theo số thứ tự thao tác.
  - `HBDB_CSV_HEADER` thêm `,offered_rps`. `run_one.sh` truyền `offered_rps=${RATE:-0}` qua `--tags`, và `--rate`/`--max-inflight` khi `RATE` > 0. Tag của lượt thêm `_rate<RATE>` khi `RATE` > 0.
  - `plot.py` chấp nhận cả hai header.
- [ ] **Step 4: Chạy test, xác nhận PASS**, cả bộ cũ: `go test ./veritas/benchmark/...`, `python3 -m pytest experiments -q`, `experiments/exp/test_exp.sh`.
- [ ] **Step 5: Thử thật.** Lệnh: `EXP=p7smoke BACKEND=raft MEM=1g BATCH_FLUSH=100ms RATE=500 DURATION=20s experiments/run_one.sh`. Kỳ vọng: rc=0, cột `offered_rps` = 500, tps trong khoảng 480–520.
- [ ] **Step 6: Commit** `bench: open-loop load (--rate, --max-inflight) and offered_rps column`.

### Task 2: Tài nguyên broker và `timeout-commit` của tendermint

**Files:**
- Modify: `experiments/cluster/gen_compose.py` (`--kafka-cpus`, `--kafka-mem`; thay hằng `KAFKA_CPUS, KAFKA_MEM` ở dòng 110 thành mặc định), `experiments/cluster/tm_config.py` (`--timeout-commit`, ghi `timeout-commit` trong `[consensus]` qua `edit_toml`), `experiments/cluster/up.sh`, `experiments/run_one.sh` (env `KAFKA_CPUS`, `KAFKA_MEM`, `TM_TIMEOUT_COMMIT`), `experiments/exp/exp_lib.sh` (bảo vệ `.params` thêm 5 biến: `KAFKA_CPUS KAFKA_MEM TM_TIMEOUT_COMMIT RATE MAX_INFLIGHT`), các test tương ứng.

**Interfaces:**
- Produces: env `KAFKA_CPUS` (mặc định `2`), `KAFKA_MEM` (`1536m`), `TM_TIMEOUT_COMMIT` (rỗng = mặc định Tendermint). Tag của lượt thêm `_kc<..>`/`_tc<..>` khi khác mặc định.

- [ ] **Step 1: Viết test hỏng.**
  - `test_gen_compose.py::test_kafka_resources_flag`: `--kafka-cpus 0.5 --kafka-mem 768m` → service `kafka` có `cpus == 0.5` và `mem_limit == "768m"`; không truyền cờ → `2` và `1536m`.
  - `test_tm_config.py::test_timeout_commit`: `--timeout-commit 100ms` → mọi `config.toml` có `timeout-commit = "100ms"` trong `[consensus]`; không truyền cờ → file không đổi.
  - `test_exp.sh`: chạy lần hai cùng EXP với `KAFKA_CPUS` khác → driver dừng (exit 1) và nhắc tới `.params`.
- [ ] **Step 2: Chạy, xác nhận FAIL.** Lệnh: `python3 -m pytest experiments/cluster -q`, `experiments/exp/test_exp.sh`.
- [ ] **Step 3: Cài đặt** theo Interfaces.
- [ ] **Step 4: Chạy test, xác nhận PASS.**
- [ ] **Step 5: Commit** `harness: kafka broker resources and tendermint timeout-commit knobs`.

### Task 3: Driver E7/E7f và phân tích điểm bão hòa

**Files:**
- Create: `experiments/exp/e7_rate.sh`, `experiments/plot/saturation.py`, `experiments/plot/test_saturation.py`
- Modify: `experiments/exp/test_exp.sh`, `README.md` (dòng E7 trong bảng thí nghiệm)

**Interfaces:**
- Consumes: `RATE`, `KAFKA_CPUS`, `KAFKA_MEM`, `TM_TIMEOUT_COMMIT` (Task 1–2); `exp_cfg` của `exp_lib.sh`.
- Produces:
  - `e7_rate.sh [--dry-run]`. Env: `RATES` (mặc định `250 500 1000 1500 2000 3000 4000`), `TM_RATES` (`50 100 200 300 400 600`), `BACKENDS`. Cố định N=4, block 100, `WORKLOAD=a`, `MEM=1g`, `BATCH_FLUSH=100ms`; CLIENTS = 256 (số worker gRPC).
  - `saturation.py <csv...> --ops-dir DIR --out DIR`: in và ghi `saturation.csv` gồm `backend,config,sat_rps,tps_at_sat,write_p99_ms_at_sat,reps`. `config` ghép từ các hậu tố tag `_kc`/`_tc`.
  - Mỗi lượt cần ghi `dropped` vào `.ops.csv` (Task 1), để saturation.py đọc được.

- [ ] **Step 1: Viết test hỏng.**
  - `test_saturation.py::test_sat_point`: fixture có tốc độ gửi 500/1000/1500 và tps 499/990/1200 (write p99 300/800/2500) → `sat_rps == 1000`.
  - `test_saturation.py::test_no_sat_point`: điểm thấp nhất đã vượt ngưỡng → `sat_rps` rỗng, và có ghi chú.
  - `test_exp.sh`: `e7_rate.sh --dry-run` → 81 dòng `DRY_RUN` (3 backend × 7 mức + tendermint 6 mức, × 3 rep); với `KAFKA_CPUS=0.5 BACKENDS=kafka` → 21 dòng.
- [ ] **Step 2: Chạy, xác nhận FAIL.**
- [ ] **Step 3: Cài đặt.**
- [ ] **Step 4: Chạy test, xác nhận PASS.**
- [ ] **Step 5: Chạy E7 và E7f** trên máy đo:
  - `EXP=e7 experiments/exp/e7_rate.sh`
  - `EXP=e7f-kafka KAFKA_CPUS=0.5 KAFKA_MEM=768m BACKENDS=kafka experiments/exp/e7_rate.sh`
  - `EXP=e7f-tm100 TM_TIMEOUT_COMMIT=100ms BACKENDS=tendermint experiments/exp/e7_rate.sh`
  - `EXP=e7f-tm0 TM_TIMEOUT_COMMIT=0s BACKENDS=tendermint experiments/exp/e7_rate.sh`
  - Rồi chạy `saturation.py`. Kỳ vọng: mỗi backend có `sat_rps`.
- [ ] **Step 6: Commit** code (`exp: E7 rate sweep and saturation analysis`), rồi kết quả (`results: E7/E7f`). Tag `p7-openloop`.

## P8 — Sửa adapter

### Task 4: bft2 hàng đợi chung, `inflight`, khử trùng

**Files:**
- Modify: `bft2node/node/queue.go` (thêm `inflight`), `bft2node/node/adapter.go`, `bft2node/node/commitlog.go`, `bft2node/node/node.go` (`Config.ForwardAddr`, `Config.Peers[i].ForwardAddr`, `Config.RequeueViews` = 8, `Config.DedupWindow` = 65536), `bft2node/api/api.go` (`POST /forward` trên cổng 7202), `bft2node/wire/wire.go` (`ForwardPath`), `bft2node/main.go`, `experiments/cluster/gen_compose.py` (cổng 7202 và địa chỉ peer cho bft2-node), test trong `bft2node/node/*_test.go`.

**Interfaces:**
- Produces:
  - `func cmdID(cmd []byte) [32]byte` (SHA-256).
  - `cmdQueue.markInflight(id, view)`, `cmdQueue.commit(id)`, `cmdQueue.requeueStale(curView)`.
  - `commitLog.appendOnce(id, cmd) bool` (false = trùng, không ghi).
  - `/submit` trên một replica gửi `POST /forward` (cùng body) tới mọi peer, bỏ qua lỗi và ghi log. `/forward` chỉ đưa vào hàng đợi, không chuyển tiếp tiếp.

- [ ] **Step 1: Viết test hỏng.**
  - `TestQueueInflightRequeue`: đánh dấu inflight ở view 10, gọi `requeueStale(19)` → id về đầu hàng đợi.
  - `TestCommitLogSkipsDuplicateIDs`: `appendOnce` cùng id hai lần → lần hai trả false, log có 1 mục.
  - `TestForwardedBlockCommitsFromAnyLeader` (4 replica trong tiến trình, như `node_test.go`): 100 block chỉ submit ở replica 2 → mọi replica commit đủ 100 block, mỗi block một lần. Đếm view mà leader khác replica 2 đề xuất block của replica 2: phải > 0.
  - `TestForwardSurvivesOneCrash`: dừng replica 4 sau 20 block → các block submit ở replica 1 vẫn commit hết.
- [ ] **Step 2: Chạy, xác nhận FAIL.** Lệnh: `docker run --rm -v $PWD:/src -w /src/bft2node hbdb-bft2-build go test ./... -race`.
- [ ] **Step 3: Cài đặt** theo Interfaces và mục A3 của spec.
- [ ] **Step 4: Chạy test, xác nhận PASS** (gồm `-race` và bộ test cũ).
- [ ] **Step 5: Kiểm tra trên cụm.** Lệnh: `experiments/build.sh`, rồi `experiments/check_consistency.sh bft2`. Kỳ vọng: `CONSISTENT`.
- [ ] **Step 6: Commit** `bft2node: shared pending queue with forward, inflight requeue and dedup`.

### Task 5: tendermint `DeliverTx` không chặn

**Files:**
- Modify: `consensus/tendermint/app.go` (`DeliverTx`, `CheckTx`, `Deliver`), `consensus/tendermint/app_test.go`

**Interfaces:**
- Produces:
  - `const DeliverQueueBytes = 128 << 20`.
  - `App.Deliver()` giữ chữ ký cũ. Một goroutine bơm từ hàng đợi nội bộ sang kênh Deliver.
  - Khi vượt giới hạn byte: `App` gọi `fail(err)`, đóng Deliver, và log `tendermint: deliver queue overflow`.

- [ ] **Step 1: Viết test hỏng.**
  - `TestDeliverTxDoesNotBlockWhenConsumerStalls`: không đọc Deliver, gọi `DeliverTx` 10 000 lần với tx 1 KB → xong trong < 2 s; trong lúc đó `CheckTx` trả trong < 100 ms.
  - `TestDeliverOrderPreserved`: 1000 tx → đọc ra đúng thứ tự.
  - `TestDeliverQueueOverflowStops`: giới hạn test 1 MiB, không đọc → Deliver bị đóng và có log.
- [ ] **Step 2: Chạy, xác nhận FAIL.** Lệnh: `go test ./consensus/tendermint/ -run 'DeliverTx|DeliverOrder|Overflow' -race -v` (trong `hbdb-build`).
- [ ] **Step 3: Cài đặt.**
- [ ] **Step 4: Chạy test, xác nhận PASS**; rồi `experiments/check_consistency.sh tendermint` cho `CONSISTENT`.
- [ ] **Step 5: Commit** `consensus/tendermint: non-blocking DeliverTx with a byte-bounded queue`.

### Task 6: Chạy lại E6' cho bft2 và tendermint

**Files:**
- Modify: `experiments/exp/e6_faults.sh` (env `E6_RATE`: nếu đặt thì truyền `RATE` cho mọi lượt E6)
- Modify: `experiments/exp/test_exp.sh`, `experiments/NOTES.md` (mục E6')

**Interfaces:**
- Consumes: `sat_rps` của Task 3. `E6_RATE` = 80% `sat_rps`, làm tròn tới 50, tính riêng từng backend; truyền qua `E6_RATE_<BACKEND>` (vd. `E6_RATE_BFT2=1300`).

- [ ] **Step 1: Viết test hỏng.** `test_exp.sh`: `E6_RATE_BFT2=1300 BACKENDS=bft2 E6_KINDS="F1 F2" F5_KINDS=F5-F2 E6_LEADER=0 e6_faults.sh --dry-run` → mọi dòng có `RATE=1300`, tổng 18 dòng.
- [ ] **Step 2–4:** FAIL → cài đặt → PASS.
- [ ] **Step 5: Chạy.** Lệnh: `EXP=e6p MEM=1g BATCH_FLUSH=100ms BACKENDS="bft2 tendermint" E6_KINDS="F1 F2" F5_KINDS=F5-F2 E6_LEADER=0 E6_RATE_BFT2=<..> E6_RATE_TENDERMINT=<..> experiments/exp/e6_faults.sh`.
- [ ] **Step 6: Phân tích.** Dùng tỉ lệ ghi sau/trước lỗi như mục cloud của `NOTES.md`. Ghi so sánh với đợt cloud vào `NOTES.md`, trả lời: tendermint F2 có còn mất liveness không? Tỉ lệ ghi của bft2 F1 là bao nhiêu?
- [ ] **Step 7: Commit** code và kết quả. Tag `p8-adapters`.

## P9 — Chữ ký và đọc có kiểm chứng (RQ4)

### Task 7: `veritas/sigs`, Set có chữ ký, RPC `GetVerified`

**Files:**
- Create: `veritas/sigs/sigs.go`, `veritas/sigs/sigs_test.go`, `scripts/genproto_docker.sh` (protoc 3.21.12 + `protoc-gen-go` v1.3.5 trong golang:1.20, sinh lại `proto/veritas/veritas.pb.go`)
- Modify: `proto/veritas/veritas.proto` (`SetRequest.client_id = 5`, `SetRequest.sig = 6`; `GetVerifiedRequest`, `GetVerifiedResponse`, `rpc GetVerified`), `proto/veritas/veritas.pb.go` (sinh lại), `veritas/core/executor.go` (`LastHeight`, `ProveValue`), `veritas/core/server.go` (`GetVerified`, `--verify-sigs` ở `Set`/`BatchSet`), `veritas/core/executor.go` (`all` ở `Apply`), `veritas/driver/driver.go` (`Options{ClientID, Key, ReadMode}`, `GetVerified`), `cmd/veritas/main.go` (`--verify-sigs`, `--key-seed`), `veritas/benchmark/ycsbbench/main.go` (`--sign`, `--read-mode`, `--key-seed`), `experiments/run_one.sh` và `gen_compose.py` (env `VERIFY_SIGS`, `READ_MODE`, `SIGN`, `KEY_SEED` mặc định `hbdb-phase2`), test tương ứng.

**Interfaces:**
- Produces:
  - `sigs.KeyFor(seed, id string) ed25519.PrivateKey`, `sigs.SetBytes(key, value string, version int64) []byte`, `sigs.SignSet(...)`, `sigs.VerifySet(pub, key, value, version, sig) bool`.
  - `type SigMode int` (`SigNone`, `SigIngress`, `SigAll`); `ServerConfig.Sigs SigMode`, `ServerConfig.ClientKeys func(id string) (ed25519.PublicKey, bool)` (khóa sinh từ seed, client id dạng `client<k>`).
  - `func (e *Executor) ProveValue(key []byte) (value string, version int64, height int64, root []byte, proof merkletree.SparseMerkleProof, err error)`: một lần giữ khóa (như `Prove`). Height do `Apply` lưu.
  - `driver.(*Driver).GetVerified(ctx, key) (string, error)`: trả `ErrProofInvalid` nếu `merkletree.VerifyProof` sai. Recorder gom lỗi này thành loại `proof`.

- [ ] **Step 1: Viết test hỏng.**
  - `TestKeyForDeterministic`: cùng seed và id thì cùng khóa; khác id thì khác khóa.
  - `TestSignVerifySet`: chữ ký đúng thì true; đổi value hoặc version thì false.
  - `TestSetRejectsBadSigIngress` (core, memnet): `SigIngress`, chữ ký sai → `codes.Unauthenticated`; chữ ký đúng → commit.
  - `TestApplyDropsBadSigAll`: `SigAll`, block chứa một Set sai chữ ký → seq vào `aborted`; root như nhau trên 3 executor.
  - `TestGetVerifiedConsistentUnderApply`: một goroutine apply 200 block, goroutine khác gọi `GetVerified` 1000 lần → mọi phản hồi qua `VerifyProof`.
  - `TestDriverGetVerifiedRejectsTamperedValue`: server giả trả value khác → `ErrProofInvalid`.
- [ ] **Step 2: Chạy, xác nhận FAIL.** Lệnh: `go test ./veritas/... -run 'KeyFor|SignVerify|BadSig|GetVerified' -race -v`.
- [ ] **Step 3: Cài đặt.** Sinh lại proto bằng `scripts/genproto_docker.sh`; diff của `veritas.pb.go` chỉ được thêm, không đổi các message cũ.
- [ ] **Step 4: Chạy test, xác nhận PASS**, cả bộ cũ, rồi build lại. Lệnh: `experiments/build.sh`, `VERIFY_SIGS=all SIGN=1 READ_MODE=verified experiments/check_consistency.sh raft` → `CONSISTENT`, 0 lỗi `proof`.
- [ ] **Step 5: Commit** `core: ed25519 signed sets (--verify-sigs) and verifiable reads (GetVerified)`.

### Task 8: Driver E8 và chạy RQ4

**Files:**
- Create: `experiments/exp/e8_verify.sh`
- Modify: `experiments/exp/test_exp.sh`, `veritas/benchmark/ycsbbench/main.go` (ghi `read_bytes_mean` và `verify_cpu_us_mean` vào `.ops.csv`), `experiments/plot/plot.py` (hình E8: tps và write/read p99 theo cấu hình)

**Interfaces:**
- Consumes: Task 7; `sat_rps` của Task 3 (`E8_RATE_<BACKEND>`).
- Produces: `e8_verify.sh`. Ma trận: backend ∈ {raft, bft2}, workload ∈ {a, c}, `VERIFY_SIGS` ∈ {none, ingress, all}, `READ_MODE` ∈ {plain, verified}, N=4, 3 rep → 72 lượt. Mỗi cặp (`VERIFY_SIGS`, `READ_MODE`) ghi vào EXP con riêng `e8-<sigs>-<read>`, vì CSV không có cột cho chúng.

- [ ] **Step 1: Viết test hỏng.** `test_exp.sh`: `e8_verify.sh --dry-run` → 72 dòng và 6 EXP khác nhau.
- [ ] **Step 2–4:** FAIL → cài đặt → PASS.
- [ ] **Step 5: Chạy E8**, vẽ hình, ghi kết quả RQ4 vào `NOTES.md`: giảm tps và tăng p99 so với `none/plain`; byte đọc; CPU kiểm proof.
- [ ] **Step 6: Commit** code và kết quả. Tag `p9-verify`.

## P10 — Xác nhận root trong giao thức (RQ5)

### Task 9: `Certifier`, attestation, trạng thái `diverged`

**Files:**
- Create: `veritas/core/certifier.go`, `veritas/core/certifier_test.go`
- Modify: `proto/veritas/veritas.proto` (`message RootAttestation{int64 height=1; bytes root=2; int32 node_id=3; bytes sig=4;}`, `Block.attestations = 4`), `proto/veritas/veritas.pb.go`, `veritas/sigs/sigs.go` (`AttestBytes`, `SignAttest`, `VerifyAttest`), `veritas/core/server.go` (vòng apply gọi certifier; batcher gắn attestation; idle fallback 100 ms; `Get`/`GetVerified` trả `codes.DataLoss` khi `diverged`), `veritas/core/batcher.go`, `cmd/veritas/main.go` (`--attest-every`, `--attest-quorum`, `--n`), `experiments/cluster/gen_compose.py` và `run_one.sh` (env `ATTEST_EVERY`), test tương ứng.

**Interfaces:**
- Produces:
  - `type Certifier struct{...}`; `NewCertifier(n, quorum int, keys func(nodeID int32) (ed25519.PublicKey, bool), keep int) *Certifier`.
  - `(c *Certifier) Add(a *pbv.RootAttestation) (certified bool, root []byte)`.
  - `(c *Certifier) Certified(h int64) ([]byte, []*pbv.RootAttestation, bool)`.
  - `(c *Certifier) Wait(ctx, h int64) ([]byte, []*pbv.RootAttestation, error)`.
  - `quorum == 0` nghĩa là `2*((n-1)/3)+1`.
  - `Server` có `Diverged() (height int64, ok bool)`, và log đúng chuỗi `core: diverged at height <h>`.
  - Metrics thêm `attest_sent`, `attest_invalid`, `certify_lag_ms` (từ lúc apply h tới lúc h được xác nhận).

- [ ] **Step 1: Viết test hỏng.**
  - `TestCertifierQuorum`: N=4, 2 attestation khớp → chưa xác nhận; thêm attestation thứ ba → xác nhận đúng root.
  - `TestCertifierIgnoresDuplicateSigner`: cùng node gửi 3 lần → không xác nhận.
  - `TestCertifierRejectsUnknownKey`: chữ ký của khóa node 9 → bỏ qua, `attest_invalid` = 1.
  - `TestCertifierConflictingRoots`: 2 root khác nhau, mỗi root 2 phiếu (N=4) → không xác nhận; thêm phiếu thứ ba cho root A → xác nhận A.
  - `TestCertifierEvictsOld`: keep=4, xác nhận h=1..10 → `Certified(1)` trả false.
  - `TestCorruptNodeDiverges` (memnet, 4 server, `ATTEST_EVERY=1`, node 4 `--fault=corrupt` từ đầu): sau 20 block, node 4 `Diverged()` ok và `Get` trả `codes.DataLoss`; ba node còn lại không diverged.
  - `TestAttestOffByDefault`: `--attest-every=0` → không có block chỉ chứa attestation; root và height giống hệt khi chưa có tính năng.
- [ ] **Step 2: Chạy, xác nhận FAIL.** Lệnh: `go test ./veritas/core/ -run 'Certifier|Diverges|AttestOff' -race -v`.
- [ ] **Step 3: Cài đặt** theo mục B2 của spec. Certifier chỉ được gọi từ vòng apply, nên kết quả tất định trên mọi node.
- [ ] **Step 4: Chạy test, xác nhận PASS**, cả bộ cũ; rồi `ATTEST_EVERY=1 experiments/check_consistency.sh` cho cả 4 backend → `CONSISTENT`.
- [ ] **Step 5: Commit** `core: root attestations, certifier and diverged fail-stop`.

### Task 10: Đọc có xác nhận (`--read-mode=certified`)

**Files:**
- Modify: `veritas/core/server.go` (`GetVerified` chờ `Certifier.Wait` tối đa 5 s khi request có `certified=true`), `proto/veritas/veritas.proto` (`GetVerifiedRequest.certified = 2`; `GetVerifiedResponse.attestations` dùng `RootAttestation`), `veritas/driver/driver.go` (kiểm ≥ Q chữ ký hợp lệ trên `(height, root)`; lỗi `ErrCertInvalid`), `veritas/benchmark/ycsbbench/main.go`, test tương ứng.

**Interfaces:**
- Consumes: `Certifier.Wait`, `sigs.VerifyAttest` (Task 9); `GetVerified` (Task 7).
- Produces: `READ_MODE=certified`. Recorder gom `ErrCertInvalid` thành loại `cert`.

- [ ] **Step 1: Viết test hỏng.**
  - `TestCertifiedReadFromCorruptNodeRejected` (memnet, node 4 corrupt): mọi lượt đọc từ node 4 trả lỗi (`DataLoss` hoặc `ErrCertInvalid`); không lượt nào trả giá trị có `CorruptMarker`.
  - `TestCertifiedReadWaitsForCertification`: chặn attestation của 2 node (N=4) → `GetVerified(certified)` trả `DeadlineExceeded` sau ~5 s.
  - `TestDriverRejectsForgedAttestations`: server giả trả 3 attestation cùng ký bởi một khóa → `ErrCertInvalid`.
- [ ] **Step 2–4:** FAIL → cài đặt → PASS.
- [ ] **Step 5: Commit** `core: certified reads`.

### Task 11: Driver E9 và chạy RQ5

**Files:**
- Create: `experiments/exp/e9_attest.sh`, `experiments/plot/rq5.py`, `experiments/plot/test_rq5.py`
- Modify: `experiments/run_one.sh` (khi `ATTEST_EVERY` > 0: chép mọi dòng `core: diverged at height` của log node vào `<tag>.diverged`, kèm timestamp docker), `experiments/exp/test_exp.sh`

**Interfaces:**
- Consumes: Task 9–10, `.faults` (thời điểm bật lỗi), `.audit`.
- Produces:
  - `e9_attest.sh`. Ma trận: backend ∈ {raft, bft2}, `ATTEST_EVERY` ∈ {0, 1, 10}, `FAULT` ∈ {none, F4:node4@60, F5-F4:node4,node3@60}, `READ_MODE` ∈ {plain, certified}, N=4. Ràng buộc: `certified` chỉ khi `ATTEST_EVERY` > 0; F5-F4 chỉ cho bft2.
  - `rq5.py`: mỗi lượt một dòng với `detect_protocol_s` (dòng `diverged` đầu tiên trừ thời điểm bật lỗi), `detect_auditor_s`, `wrong_gets`, `rejected_gets` (`proof` + `cert` + `DataLoss`), `tps`, `write_p99_ms`. Kèm bảng tổng hợp theo cấu hình.

- [ ] **Step 1: Viết test hỏng.**
  - `test_rq5.py::test_detect_protocol`: fixture có `.faults` bật ở epoch 1000 ms và `.diverged` ở 1450 ms → `detect_protocol_s == 0.45`.
  - `test_rq5.py::test_no_divergence`: không có `.diverged` → giá trị rỗng, có ghi chú.
  - `test_exp.sh`: `e9_attest.sh --dry-run` → đúng số dòng theo ràng buộc. Đếm: raft 3 attest × 2 lỗi (none, F4) × read (1 cho attest 0, 2 cho attest > 0) = 10 cấu hình; bft2 = 15 cấu hình; tổng 25 × 3 rep = **75**.
- [ ] **Step 2–4:** FAIL → cài đặt → PASS.
- [ ] **Step 5: Chạy E9** (`MEM=1g BATCH_FLUSH=100ms`, tốc độ = 80% `sat_rps`), chạy `rq5.py`. Ghi kết quả vào `NOTES.md`: kiểm tra tiêu chí thành công 3; chi phí attestation K=1 so với K=10.
- [ ] **Step 6: Commit** code và kết quả. Tag `p10-attest`.

### Task 12: Tài liệu giai đoạn 2

**Files:**
- Modify: `README.md` (phần giai đoạn 2: cờ và env mới, E7–E9, cách chạy), `experiments/NOTES.md` (mục "Giai đoạn 2": môi trường, sai lệch, kết quả RQ1/2, RQ3', RQ4, RQ5)

- [ ] **Step 1:** Cập nhật hai file. Mọi số trích trong README phải trùng với `NOTES.md`.
- [ ] **Step 2:** Chạy `python3 -m pytest experiments -q`, `experiments/exp/test_exp.sh`, `go test ./consensus/... ./veritas/... ./cmd/auditor ./cmd/veritas` (trong `hbdb-build`) và bộ test của `bft2node`. Tất cả phải PASS.
- [ ] **Step 3: Commit** `docs: phase 2 results and how to run E7-E9`.
