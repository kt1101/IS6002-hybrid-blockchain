# Giai đoạn 3: truy vết node lỗi, đọc sai không đổi trạng thái và ngưỡng xác nhận — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Chạy giai đoạn 2 trên M3 làm mốc, rồi thêm truy vết trong giao thức (loại node tất định qua attestation mâu thuẫn và bằng chứng đọc có chữ ký), lỗi F6 ở đường đọc, chuyển node phía client, đọc từ quorum và thí nghiệm ngưỡng xác nhận (RQ6, RQ7).

**Architecture:** Mọi quyết định loại node được tính trong vòng apply từ dữ liệu đã sắp thứ tự: `Certifier` báo attestation mâu thuẫn, `Accountant` (mới, `veritas/core/accountability.go`) giữ tập `evicted` và kiểm bằng chứng đọc đi qua `Block.evidence` giống `Block.attestations`. Client (`ycsbbench`) có bộ chọn node mới (`reader.go`) lo chuyển node, đọc có chữ ký, gửi bằng chứng và đọc từ quorum. Adapter đồng thuận không đổi.

**Tech Stack:** Go 1.20 (module chính, ảnh `hbdb-build`), Go 1.23 (`bft2node/`), `crypto/ed25519`, protobuf APIv1 (`protoc-gen-go` v1.3.2 qua `scripts/genproto_docker.sh`), bash, Python 3 (pandas, matplotlib), Docker.

**Spec:** `docs/superpowers/specs/2026-11-09-phase3-accountability-design.md` (xây trên `docs/superpowers/specs/2026-11-03-phase2-fair-comparison-and-verifiability-design.md`).

## Global Constraints

- Nhánh dựa trên `claude/charming-hopper-hy6x4e` (giai đoạn 2, `0cc4828`). Không sửa file đã commit trong `results/final/` và `results/cloud-bf100ms/`.
- Module chính giữ `go 1.20`; `bft2node/` giữ golang:1.23. Sinh proto chỉ bằng `scripts/genproto_docker.sh`.
- Test Go chạy trong container (sandbox host không mở được cổng): `docker run --rm -v $PWD:/src -w /src hbdb-build go test -race <pkg>`.
- Mặc định của mọi cờ mới giữ hành vi giai đoạn 2: `--failover=false`, `--sign-reads=off`, `--read-mode=plain`, `--kafka-quorum=all`, không fault đọc.
- Thí nghiệm giai đoạn 3: `COMMIT_MODE=sync`, `BATCH_FLUSH=100ms`, `MEM=1g`, `CPUS=0.5`, `ATTEST_EVERY=1`, vòng mở ở 80% thông lượng bão hòa E7 của **M3** (`E*_RATE_<BACKEND>`), 3 rep, quy tắc CV > 10% → tối đa 2 rep thêm; backend raft và bft2.
- N=7 chỉ khi Docker VM ≥ 10 GiB; ngược lại chỉ N=4 (ghi NOTES).
- Cửa sổ `Certifier`/bằng chứng: 1024 height (`DefaultAttestKeep`). Bằng chứng chờ gửi đi kèm block kế tiếp, hoặc block rỗng sau `AttestIdle` (100 ms), như attestation.
- Bytes ký phản hồi đọc: `"veritas-read" 0x00 key 0x00 value 0x00 version(8B BE) height(8B BE) root sha256(side_nodes nối liền ‖ non_membership_leaf_data)`.
- Log máy đọc được (harness phụ thuộc, giữ đúng chữ): `core: evicted node <c> at height <h> (<reason>)` với reason ∈ {`divergent_root`, `bad_read`}; `core: certified height <h> root <hex>` (chỉ height có attestation mâu thuẫn).
- Node bị loại trả `codes.PermissionDenied` cho `Get` và `GetVerified`.

## Review Focus

1. **Bằng chứng giả để loại node trung thực:** một node (hoặc client) đưa vào block một `Evidence` có chữ ký giả, hoặc chữ ký thật trên phản hồi đúng; không node nào bị loại, bộ đếm `EvidenceRejected` tăng. Test: `TestForgedEvidenceEvictsNobody`, `TestEvidenceOfCorrectReadEvictsNobody` (Task 6).
2. **Báo trùng:** 50 client cùng gửi bằng chứng về một node; node bị loại một lần, `epoch` tăng đúng 1. Test: `TestDuplicateEvidenceEvictsOnce` (Task 6).
3. **Mọi node đều bị loại hoặc nghi ngờ:** client không quay vòng vô hạn; đọc trả lỗi `ErrNoNode` và worker nghỉ theo backoff. Test: `TestReaderAllNodesExcluded` (Task 7).
4. **Attestation của node đã bị loại ở các height sau:** không được đếm vào quorum, nhưng attestation của nó ở height trước height loại vẫn giữ nguyên chứng chỉ đã có. Test: `TestEvictedNodeNotCounted` (Task 4).
5. **Bằng chứng cho height chưa xác nhận hoặc đã ra khỏi cửa sổ:** chưa xác nhận → giữ lại và xử lý khi xác nhận; quá cửa sổ → bỏ và đếm. Test: `TestEvidenceHeldUntilCertified`, `TestEvidenceOutsideWindowDropped` (Task 6).

---

## P2 — Chạy giai đoạn 2 trên M3 (tuần 1–3)

### Task 1: Build, test và kiểm tra nhất quán giai đoạn 2

**Files:**
- Modify: `experiments/NOTES.md` (mục "Giai đoạn 2: chạy trên M3")

- [ ] **Step 1: Kiểm tra RAM Docker VM.** Chạy `docker info --format '{{.MemTotal}}'`. Ghi giá trị vào NOTES; < 10 GiB thì ghi "N=7 bỏ" và mọi task sau dùng N=4.
- [ ] **Step 2: Build ảnh.** Chạy `experiments/build.sh`. Kỳ vọng: exit 0, các ảnh `hbdb-*` có thời điểm build mới.
- [ ] **Step 3: Chạy toàn bộ test Go trong container.** `docker run --rm -v $PWD:/src -w /src hbdb-build go test -race ./...` và `docker run --rm -v $PWD/bft2node:/src -w /src hbdb-bft2-build go test -race ./...`. Kỳ vọng: PASS, kể cả `TestForwardedBlockCommitsFromAnyLeader`, `TestForwardSurvivesOneCrash`. Lỗi thì sửa theo superpowers:systematic-debugging, mỗi sửa một commit `fix(phase2): ...`.
- [ ] **Step 4: Kiểm tra nhất quán.** Với `b` ∈ {kafka, raft, tendermint, bft2}: `ATTEST_EVERY=1 MEM=1g BATCH_FLUSH=100ms experiments/check_consistency.sh $b`. Kỳ vọng: `CONSISTENT` cả 4, không dòng `core: diverged`.
- [ ] **Step 5: Commit NOTES.** `git commit -m "docs: phase 2 build, tests and consistency on M3"`.

### Task 2: E7 (raft, bft2) và E9 trên M3

**Files:**
- Modify: `experiments/NOTES.md`

- [ ] **Step 1: E7.** `BACKENDS="raft bft2" experiments/exp/e7_rate.sh` rồi `python3 experiments/plot/saturation.py results/raw/e7*.csv`. Kỳ vọng: mỗi backend có thông lượng bão hòa (định nghĩa giai đoạn 2). Ghi `E9_RATE_RAFT`, `E9_RATE_BFT2` = 80% giá trị đó vào NOTES; mọi driver giai đoạn 3 dùng các giá trị này.
- [ ] **Step 2: E9.** `E9_RATE_RAFT=<…> E9_RATE_BFT2=<…> experiments/exp/e9_attest.sh`, rồi `python3 experiments/plot/rq5.py results/raw/e9-*.csv --raw-dir results/raw --out results/e9`. Kỳ vọng: 75 lượt; với F4 + `certified`: `wrong_gets` = 0.
- [ ] **Step 3: Tag và commit.** Ghi kết quả E7/E9 vào NOTES; `git tag p7-openloop`, `git tag p10-attest` (tag P8/P9 chỉ khi E6'/E8 cũng đã chạy; không thì ghi trong NOTES là chưa chạy). Commit `docs: phase 2 E7/E9 on M3`.

---

## P11 — Truy vết (tuần 4–7)

### Task 3: Lỗi F6 ở đường đọc

**Files:**
- Modify: `veritas/core/fault.go`, `veritas/core/fault_test.go`, `veritas/core/server.go` (`Get`, `proveResp`), `experiments/faults.sh`, `experiments/cluster/gen_compose.py` (`_fault_args`), `experiments/cluster/test_gen_compose.py`, `experiments/test_faults.py`

**Interfaces:**
- Produces:
  - `type ReadFault interface { OnRead(key, val string, signed bool) string }` (tùy chọn, kiểm bằng type assertion trên `FaultPolicy`; `FaultPolicy` không đổi để không phá các triển khai hiện có).
  - Hằng `FaultReadLie = "readlie"`, `FaultReadLieEvasive = "readlie-evasive"`; `ParseFault` nhận cả hai. Khi đang bật: `readlie` trả `val + benchmark.CorruptMarker` bất kể `signed`; `readlie-evasive` trả `val` khi `signed`, ngược lại như `readlie`.
  - `Server.Get` và `GetVerified` (mọi chế độ) áp dụng `OnRead` lên giá trị trả về, **sau** khi lấy proof (proof, root, version là của giá trị thật).
  - `faults.sh`: `F6:node<i>@<t>` → `FAULT_POLICY=readlie`, `F6E:node<i>@<t>` → `readlie-evasive`; tối đa f node; mọi backend core.

- [ ] **Step 1: Viết test hỏng.**
  - `TestParseFaultReadLie`: trước kích hoạt `OnRead("k","v",false) == "v"`; sau `Activate()` → `"v"+CorruptMarker`; `OnApply("k","v") == "v"`.
  - `TestReadLieEvasiveHonestWhenSigned`: sau kích hoạt `OnRead("k","v",true) == "v"`, `OnRead("k","v",false) == "v"+CorruptMarker`.
  - `TestReadLieKeepsRootsEqual` (memnet, 4 node, node 4 `readlie` đã bật, `AttestEvery=1`): sau 20 Set, mọi `RootDigest()` bằng nhau, không node nào `Diverged()`; `getValue(node4,"k0")` có hậu tố `CorruptMarker`.
  - `TestCertifiedReadFromReadLieRejectedByClient`: `GetVerified(certified)` trên node 4 trả lỗi nil nhưng `merkletree.VerifyProof` trên `value+"-"+version` thất bại.
  - Python: `test_fault_parse_f6` (`F6:node4@60` → `FAULT_POLICY=readlie`; `F6:node3,node4@60` với N=4 → lỗi "at most f"); `test_fault_args_readlie` (`_fault_args("readlie") == " --fault=readlie --fault-after=87600h"`).
- [ ] **Step 2: Chạy, xác nhận FAIL.** `docker run --rm -v $PWD:/src -w /src hbdb-build go test -race ./veritas/core -run 'ReadLie|ParseFaultReadLie' -v`; `python3 -m pytest experiments -q -k 'f6 or readlie'`.
- [ ] **Step 3: Cài đặt** theo Interfaces. `delayedFault` thêm nhánh cho hai kind mới; `cmd/veritas` không cần sửa (đã truyền `--fault` vào `ParseFault`).
- [ ] **Step 4: Chạy lại, xác nhận PASS** (hai lệnh Step 2, cộng `go test -race ./veritas/core` toàn gói).
- [ ] **Step 5: Commit** `core: read-path fault F6 (readlie, readlie-evasive)`.

### Task 4: Certifier báo attestation mâu thuẫn và bỏ qua node bị loại

**Files:**
- Modify: `veritas/core/certifier.go`, `veritas/core/certifier_test.go`, `veritas/core/attest.go` (`afterApply`, `onCertified`)

**Interfaces:**
- Consumes: `Certifier.Add` của giai đoạn 2.
- Produces:
  - `type AddResult struct { Certified bool; Root []byte; Divergent []int32 }`.
  - `func (c *Certifier) Add(a *pbv.RootAttestation) AddResult` (thay chữ ký cũ; cập nhật mọi chỗ gọi). `Divergent` = node ký root ≠ root xác nhận của height đó: khi height vừa được xác nhận, mọi node đã ký root khác (theo thứ tự đến); sau khi đã xác nhận, node vừa ký root khác.
  - `func NewCertifier(n, quorum int, keys func(int32) (ed25519.PublicKey, bool), keep int, skip func(nodeID int32, height int64) bool) *Certifier`: attestation có `skip(node, h) == true` bị bỏ qua (không đếm, không báo mâu thuẫn). `skip` nil = không bỏ ai.
  - Khi một height có `Divergent` khác rỗng lần đầu, `attestor` ghi log `core: certified height <h> root <hex>`.

- [ ] **Step 1: Viết test hỏng** (`certifier_test.go`, 4 node, quorum 3 trừ khi ghi khác).
  - `TestDivergentAfterCertification`: node 1–3 ký R, node 4 ký R′ sau đó → lần Add của node 4 trả `Divergent == []int32{4}`, `Certified == false`.
  - `TestDivergentBeforeCertification`: thứ tự 4(R′), 1(R), 2(R), 3(R) → Add của node 3 trả `Certified`, `Root == R`, `Divergent == []int32{4}`.
  - `TestQuorumF1WrongRootFirst`: quorum 2; thứ tự 3(R′), 4(R′), 1(R), 2(R) → Add của node 4 xác nhận R′; Add của node 1 trả `Divergent == []int32{1}`, của node 2 trả `[]int32{2}`.
  - `TestEvictedNodeNotCounted`: `skip` trả true cho node 4 khi h ≥ 5; ở h=5, node 1, 2, 4 ký R → chưa xác nhận (quorum 3); ở h=4 cùng thứ tự → xác nhận.
  - `TestNoDivergentOnHonestCluster`: 4 node cùng R → `Divergent` luôn rỗng.
- [ ] **Step 2: Chạy, xác nhận FAIL.** `... go test -race ./veritas/core -run 'Divergent|QuorumF1|EvictedNodeNotCounted|NoDivergent' -v`.
- [ ] **Step 3: Cài đặt.** `heightVotes` giữ thứ tự đến của mọi attestation hợp lệ (không chỉ theo root) để tính `Divergent` tất định. Sau khi đã xác nhận vẫn kiểm root (giai đoạn 2 `return` sớm). Cập nhật `attestor.afterApply` dùng `AddResult`; `Divergent` chưa được dùng ở task này ngoài log.
- [ ] **Step 4: Chạy lại toàn gói, xác nhận PASS** (gồm các test giai đoạn 2 trong `attest_test.go`).
- [ ] **Step 5: Commit** `core: certifier reports divergent attestations and skips evicted nodes`.

### Task 5: `Accountant`, loại node do root mâu thuẫn, RPC `Evicted`

**Files:**
- Create: `veritas/core/accountability.go`, `veritas/core/accountability_test.go`
- Modify: `proto/veritas/veritas.proto`, `proto/veritas/veritas.pb.go` (sinh lại), `veritas/core/server.go`, `veritas/core/attest.go`, `veritas/core/metrics.go` (`Breakdown`)

**Interfaces:**
- Consumes: `AddResult.Divergent`, `NewCertifier(..., skip)` (Task 4).
- Produces:
  - Proto: `GetVerifiedRequest.signed = 3`; `GetVerifiedResponse` thêm `evicted_epoch = 8`, `node_sig = 9`, `node_id = 10`; `rpc Evicted(EvictedRequest) returns (EvictedResponse)` với `EvictedResponse { int64 epoch = 1; repeated int32 nodes = 2; }`.
  - `type Accountant struct` với:
    - `func NewAccountant(self int32) *Accountant`
    - `func (a *Accountant) Evict(node int32, height int64, reason string) bool`: lần đầu với node đó → ghi log `core: evicted node <c> at height <h> (<reason>)`, `epoch++`, trả true; lần sau → false.
    - `func (a *Accountant) Skip(node int32, height int64) bool`: true khi node đã bị loại ở height < `height` (dùng làm `skip` của `Certifier`).
    - `func (a *Accountant) Epoch() int64`, `func (a *Accountant) Nodes() []int32` (tăng dần), `func (a *Accountant) SelfEvicted() bool`.
  - Vòng apply: với mỗi `Divergent` → `Evict(node, h_block, "divergent_root")`, `h_block` = height của block đang apply.
  - `Get`/`GetVerified` trên node có `SelfEvicted()` → `codes.PermissionDenied`. `GetVerifiedResponse.EvictedEpoch` = `Epoch()`. `Server.Evicted` trả `Epoch()`, `Nodes()`.
  - `Breakdown` thêm `Evictions int64`.

- [ ] **Step 1: Viết test hỏng.**
  - `TestAccountantEvictOnce`: `Evict(4,10,"divergent_root")` → true, epoch 1; lần hai → false, epoch 1; `Skip(4,10) == false`, `Skip(4,11) == true`.
  - `TestCorruptNodeEvictedEverywhereSameHeight` (memnet, 4 node, `AttestEvery=1`, node 4 `suffixFault{}`): sau 20 Set, mọi node có `Evicted` = `{4}` và cùng height loại (đọc qua log hook hoặc `Breakdown`); `getValue(node4,…)` trả `PermissionDenied`; `Evicted` RPC trên node 1 trả epoch 1, nodes `[4]`.
  - `TestHonestClusterEvictsNobody`: như `TestHonestClusterCertifies` của giai đoạn 2, thêm `Breakdown().Evictions == 0` trên mọi node.
  - `TestF5F4QuorumF1OneSideEvicted` (memnet, node 3 và 4 `suffixFault{}`, `AttestQuorum=2`): sau 20 Set, mọi node có cùng tập `Evicted`, và tập đó là `{3,4}` hoặc `{1,2}` (tùy root nào được xác nhận trước).
  - `TestF5F4Quorum2f1NoCertNoEviction` (như trên, `AttestQuorum=3`): không node nào bị loại; `GetVerified(certified)` trên node 1 trả `codes.DeadlineExceeded` (đặt `CertifiedWait` 200 ms trong tweak).
- [ ] **Step 2: Chạy, xác nhận FAIL.** `scripts/genproto_docker.sh` rồi `... go test -race ./veritas/core -run 'Accountant|EvictedEverywhere|EvictsNobody' -v`.
- [ ] **Step 3: Cài đặt** theo Interfaces. Height loại là height của block làm lộ mâu thuẫn, nên giống nhau trên mọi node (vòng apply tất định). Chế độ `diverged` của giai đoạn 2 giữ nguyên; `PermissionDenied` ưu tiên trước `DataLoss`.
- [ ] **Step 4: Chạy lại toàn gói, xác nhận PASS.**
- [ ] **Step 5: Commit** `core: accountant evicts nodes with divergent root attestations`.

### Task 6: Đọc có chữ ký và bằng chứng `bad_read` qua block

**Files:**
- Modify: `veritas/sigs/sigs.go`, `veritas/sigs/sigs_test.go`, `proto/veritas/veritas.proto` (+ sinh lại), `veritas/core/server.go` (`GetVerified`, `ReportEvidence`, batcher hook), `veritas/core/attest.go` (`take`/`putBack`/`idleBlock` mang thêm bằng chứng), `veritas/core/accountability.go`, `veritas/core/accountability_test.go`

**Interfaces:**
- Consumes: `Accountant` (Task 5), `Certifier.Certified(h)` (giai đoạn 2).
- Produces:
  - `func ReadBytes(key, value string, version, height int64, root []byte, sideNodes [][]byte, nonMember []byte) []byte` (định dạng ở Global Constraints), `func SignRead(k ed25519.PrivateKey, r *pbv.SignedRead) []byte`, `func VerifyRead(pub ed25519.PublicKey, r *pbv.SignedRead) bool`.
  - Proto: `message SignedRead { key=1; value=2; version=3; height=4; root=5; side_nodes=6; non_membership_leaf_data=7; node_id=8; sig=9 }`, `message Evidence { int32 culprit = 1; int64 height = 2; SignedRead read = 3; }`, `Block.evidence = 5`, `rpc ReportEvidence(Evidence) returns (EvidenceAck)`.
  - `GetVerified` với `signed=true`: điền `node_id`, `node_sig = SignRead(...)` trên đúng giá trị trả về (sau `OnRead`).
  - `ReportEvidence`: kiểm `VerifyRead` bằng khóa của `culprit`; sai → `codes.InvalidArgument`; đúng → xếp vào hàng chờ, đi kèm block kế tiếp của node như attestation.
  - Vòng apply, mỗi `Evidence` trong block: `func (a *Accountant) CheckRead(ev *pbv.Evidence, cert func(h int64) ([]byte, bool), keys func(int32) (ed25519.PublicKey, bool), minHeight int64, curHeight int64)`:
    - chữ ký sai, hoặc culprit đã bị loại → bỏ, `EvidenceRejected++`;
    - `ev.height < minHeight` (ra khỏi cửa sổ) → bỏ, `EvidenceDropped++`;
    - height chưa xác nhận → giữ; mỗi lần có height mới được xác nhận, xét lại các bằng chứng đang giữ;
    - root đã ký ≠ root xác nhận, hoặc proof của `value-version` không khớp root xác nhận → `Evict(culprit, curHeight, "bad_read")`;
    - ngược lại (phản hồi đúng) → bỏ, `EvidenceRejected++`.
  - `Breakdown` thêm `EvidenceRejected`, `EvidenceDropped`.

- [ ] **Step 1: Viết test hỏng.**
  - `TestSignVerifyRead` và `TestVerifyReadRejectsTamperedValue` (`sigs_test.go`).
  - `TestBadReadEvidenceEvictsEverywhere` (memnet, node 4 `readlie` bật): lấy `GetVerified(certified, signed)` từ node 4, gửi `ReportEvidence` tới node 1 → mọi node `Evicted` = `{4}`, cùng height, reason `bad_read`.
  - `TestForgedEvidenceEvictsNobody`: `SignedRead` với `sig` ngẫu nhiên gửi thẳng vào block (gọi `Submit` của memnet với `Block{Evidence: …}`) → không ai bị loại, `EvidenceRejected == 1` trên mọi node.
  - `TestEvidenceOfCorrectReadEvictsNobody`: phản hồi đúng có chữ ký của node 2 → không ai bị loại.
  - `TestDuplicateEvidenceEvictsOnce`: 50 bằng chứng giống nhau → `Evictions == 1`, `Epoch() == 1`.
  - `TestEvidenceHeldUntilCertified`: bằng chứng cho height chưa xác nhận (chặn attestation của 2 node bằng tweak) → chưa loại; thả attestation → loại.
  - `TestEvidenceOutsideWindowDropped`: `minHeight` = height bằng chứng + 1 → `EvidenceDropped == 1`, không loại.
- [ ] **Step 2: Chạy, xác nhận FAIL.** `scripts/genproto_docker.sh`; `... go test -race ./veritas/sigs ./veritas/core -run 'Read|Evidence' -v`.
- [ ] **Step 3: Cài đặt** theo Interfaces. Hàng chờ bằng chứng dùng chung cơ chế `pending`/`pendingSince`/`putBack` của `attestor`; `idleBlock` gửi block rỗng khi bằng chứng chờ ≥ `AttestIdle`.
- [ ] **Step 4: Chạy lại `./veritas/...`, xác nhận PASS.**
- [ ] **Step 5: Commit** `core: signed reads and bad_read evidence ordered through blocks`.

### Task 7: Client: chuyển node, đọc có chữ ký, gửi bằng chứng, đọc từ quorum

**Files:**
- Create: `veritas/benchmark/ycsbbench/reader.go`, `veritas/benchmark/ycsbbench/reader_test.go`
- Modify: `veritas/driver/driver.go`, `veritas/driver/driver_test.go`, `veritas/benchmark/recorder.go` (cột `.ops.csv`), `veritas/benchmark/recorder_test.go`, `veritas/benchmark/ycsbbench/main.go` (dùng `reader` cho cả vòng kín và `runOpenLoop`)

**Interfaces:**
- Consumes: proto của Task 5–6; `sigs.VerifyRead`.
- Produces:
  - `driver`: `func (d *Driver) GetCertifiedResp(ctx context.Context, key string, signed bool) (*pbv.GetVerifiedResponse, error)` (đã kiểm proof và chứng chỉ; lỗi bọc `ErrProofInvalid`/`ErrCertInvalid` như `GetCertified`, kèm phản hồi), `func (d *Driver) Evicted(ctx context.Context) (epoch int64, nodes []int32, err error)`, `func (d *Driver) ReportEvidence(ctx context.Context, ev *pbv.Evidence) error`, `ReadMode` thêm `ReadQuorum`.
  - `reader.go`:
    - `type nodeAPI interface { Get(ctx, key) (string, error); GetCertifiedResp(ctx, key string, signed bool) (*pbv.GetVerifiedResponse, error); Evicted(ctx) (int64, []int32, error); ReportEvidence(ctx, *pbv.Evidence) error }` (`*driver.Driver` thỏa mãn).
    - `type readerConfig struct { Mode driver.ReadMode; Failover bool; SignReads string /* off|on-failure|always */; F int }`.
    - `func newReader(nodes []nodeAPI, primary int, cfg readerConfig, stats *readStats) *reader`, `func (r *reader) Read(ctx context.Context, key string) (string, error)`.
    - `var ErrNoNode = errors.New("ycsbbench: every node excluded")`.
    - `type readStats struct { Rejected, QuorumErrors, Failovers, WrongBeforeFailover, EvidenceSent atomic.Int64 }`.
  - Quy tắc `Read`:
    - `quorum`: `Get` song song trên node hiện tại và F node kế tiếp (bỏ node bị loại/nghi ngờ); mọi giá trị bằng nhau → trả; khác → `QuorumErrors++`, thử bộ kế tiếp một lần, rồi lỗi.
    - `certified`: `GetCertifiedResp(signed = SignReads=="always")`. Lỗi proof/cert: `Rejected++`; nếu `on-failure` thì đọc lại cùng node với `signed=true`, phản hồi có chữ ký vẫn sai → `ReportEvidence` tới node kế tiếp (`EvidenceSent++`), phản hồi đúng hoặc từ chối ký → đánh dấu nghi ngờ. `EvictedEpoch` lớn hơn epoch đã biết → gọi `Evicted` và cập nhật tập loại.
    - `Failover`: chuyển sang node kế tiếp (vòng tròn, bỏ node bị loại/nghi ngờ) khi gặp `DataLoss`, `PermissionDenied`, lỗi proof/cert, node bị loại hoặc nghi ngờ; không chuyển khi `DeadlineExceeded`/`Unavailable`. Mỗi lần chuyển `Failovers++`. Không còn node nào → `ErrNoNode`.
    - `WrongBeforeFailover`: số giá trị có `CorruptMarker` được chấp nhận từ node mà sau đó worker đã rời đi (chỉ có ý nghĩa ở `plain`).
  - Cờ `ycsbbench`: `--failover`, `--sign-reads=off|on-failure|always`, `--read-mode` thêm `quorum`; `.ops.csv` thêm cột cuối `rejected_reads,quorum_errors,failovers,wrong_before_failover,evidence_sent`.

- [ ] **Step 1: Viết test hỏng** (`reader_test.go` dùng `fakeNode` cài `nodeAPI`).
  - `TestReaderFailoverOnPermissionDenied`: node 0 trả `PermissionDenied` → `Read` trả giá trị của node 1, `Failovers == 1`, lần `Read` sau đi thẳng node 1.
  - `TestReaderNoFailoverOnUnavailable`: node 0 trả `Unavailable` → `Read` trả lỗi, vẫn ở node 0.
  - `TestReaderSignOnFailureReportsEvidence`: node 0 trả lỗi proof, rồi phản hồi có chữ ký vẫn sai → `ReportEvidence` được gọi trên node 1 đúng 1 lần, `EvidenceSent == 1`, node 0 bị tránh.
  - `TestReaderEvasiveNodeSuspected`: node 0 lỗi proof, lần ký trả đúng → không gửi bằng chứng, node 0 bị tránh.
  - `TestReaderQuorumMismatch`: F=1, node 0 và 1 trả khác nhau → `QuorumErrors >= 1`; bộ (1,2) khớp → trả giá trị đó.
  - `TestReaderAllNodesExcluded`: mọi node bị loại → `errors.Is(err, ErrNoNode)`, không gọi node nào thêm.
  - `TestReaderRefreshesEvictedOnEpoch`: phản hồi có `EvictedEpoch=1` → gọi `Evicted` một lần, node 2 trong danh sách bị bỏ.
  - `TestOpsCSVPhase3Columns`: header `.ops.csv` kết thúc bằng 5 cột mới.
- [ ] **Step 2: Chạy, xác nhận FAIL.** `... go test -race ./veritas/benchmark/... ./veritas/driver -run 'Reader|Phase3' -v`.
- [ ] **Step 3: Cài đặt** theo Interfaces; vòng kín và vòng mở gọi `reader.Read` thay cho `clis[i].Get`; ghi vẫn gửi node chính.
- [ ] **Step 4: Chạy lại `./veritas/...`, xác nhận PASS.**
- [ ] **Step 5: Commit** `bench: reader with failover, signed reads, evidence reports and quorum reads`.

### Task 8: Harness: quorum, fault F5 trên raft, sidecar `.evict`/`.certs`, cổng hợp lệ

**Files:**
- Modify: `experiments/run_one.sh`, `experiments/cluster/up.sh`, `experiments/cluster/gen_compose.py` (`core_flags`), `experiments/cluster/test_gen_compose.py`, `experiments/faults.sh`, `experiments/test_faults.py`, `experiments/lib.sh` (`hbdb_attest_quorum`)

**Interfaces:**
- Produces:
  - Env `ATTEST_QUORUM=auto|f+1|2f+1` (mặc định `auto` = cờ 0 như giai đoạn 2); `run_one.sh` đổi ra số theo N và truyền `--attest-quorum`. `core_flags(..., attest_quorum=0)` thêm ` --attest-quorum=<q>` khi q > 0.
  - Env `FAILOVER=0|1`, `SIGN_READS=off|on-failure|always` → cờ `ycsbbench`.
  - `faults.sh`: F5-F4 cho mọi backend core khi `ATTEST_EVERY > 0`; F6/F6E (Task 3).
  - Sidecar `<tag>.evict` (dòng `core: evicted node …` của mọi node, kèm tên node và timestamp docker) và `<tag>.certs` (dòng `core: certified height …`), thu như `collect_diverged`.
  - Cổng hợp lệ (FAULT=none): có dòng `evicted` → rc=3; `rejected_reads > 0` → rc=3.

- [ ] **Step 1: Viết test hỏng.**
  - `test_core_flags_attest_quorum` (`core_flags(attest_every=1, n=4, attest_quorum=2) == ' --attest-every=1 --n=4 --attest-quorum=2'`).
  - `test_fault_parse_f5_f4_raft_with_attest` (`BACKEND=raft ATTEST_EVERY=1 FAULT=F5-F4:node3,node4@60` hợp lệ; `ATTEST_EVERY=0` → lỗi như cũ).
  - `test_attest_quorum_to_number` trong `experiments/test_faults.py` (gọi bash `source experiments/lib.sh; hbdb_attest_quorum N spec`): (4,f+1)→2, (4,2f+1)→3, (7,f+1)→3, (7,2f+1)→5, (4,auto)→0; spec khác → exit ≠ 0.
- [ ] **Step 2: Chạy, xác nhận FAIL.** `python3 -m pytest experiments -q -k 'attest_quorum or f5_f4_raft'`.
- [ ] **Step 3: Cài đặt.** Thu sidecar theo mẫu `collect_diverged`; cổng hợp lệ đọc cột `rejected_reads` của `.ops.csv`.
- [ ] **Step 4: Chạy lại, xác nhận PASS.** Smoke: `BACKEND=raft N=4 ATTEST_EVERY=1 READ_MODE=certified FAILOVER=1 SIGN_READS=on-failure FAULT=F6:node4@20 DURATION=40s experiments/run_one.sh`. Kỳ vọng: có dòng CSV, `<tag>.evict` có `node4 … bad_read` trên 4 node cùng height, `wrong_gets` = 0.
- [ ] **Step 5: Commit** `harness: attestation quorum, F5-F4 with attestations, evict/certs sidecars, validity gate`.

### Task 9: `rq6.py` và driver E10–E12

**Files:**
- Create: `experiments/plot/rq6.py`, `experiments/plot/test_rq6.py`, `experiments/exp/e10_readcost.sh`, `experiments/exp/e11_account.sh`, `experiments/exp/e12_quorum.sh`
- Modify: `experiments/exp/exp_lib.sh` (nếu cần hàm chung)

**Interfaces:**
- Consumes: sidecar Task 8, `.ops.csv` Task 7, `rq3.py`/`rq5.py` (hàm đọc `.faults`, `.audit`).
- Produces:
  - Driver theo mẫu `e9_attest.sh` (mỗi tổ hợp một EXP, tự gọi lại, `--dry-run`, chạy tiếp, `.params`):
    - `e10_readcost.sh`: EXP `e10-<read>` với read ∈ {plain, certified, certified-signall, quorum}; WORKLOADS "a c"; BACKENDS "raft bft2"; N=4; FAULT=none.
    - `e11_account.sh`: EXP `e11-<fault>-<mode>` với fault ∈ {F4, F6} × mode ∈ {plain, cert-fo, cert-fo-sign}, cộng `e11-F6E-cert-fo-sign`; N=4; f=1 node cuối; lỗi ở giây 60, `DURATION=180s`.
    - `e12_quorum.sh`: EXP `e12-q<f1|2f1>-<read>` với read ∈ {certified, quorum}; FAULT=F5-F4; NS mặc định "4" (thêm 7 khi Docker VM ≥ 10 GiB).
    - Tốc độ: `E1x_RATE_<BACKEND>` hoặc `E1x_RATE` (từ Task 2).
  - `rq6.py <csv...> --raw-dir results/raw --out <dir>` → `rq6_runs.csv`, `rq6_summary.csv` với cột: `detect_evidence_s` (lỗi → dòng `.evict` đầu tiên), `evict_all_s` (lỗi → khi mọi node trung thực có dòng `.evict` cho culprit), `same_height` (bool), `failover_s` (lỗi → khi `failovers` của mọi worker > 0, lấy từ `.ops.csv` theo thời gian nếu có, ngược lại để trống kèm ghi chú), `wrong_gets`, `rejected_reads`, `evidence_sent`, `wrong_root_heights` (từ `.certs`, root khác root của đa số node trung thực), `honest_evicted`, `cert_timeouts`.

- [ ] **Step 1: Viết test hỏng** (`test_rq6.py` dùng fixture dưới `experiments/plot/fixtures/rq6/`).
  - `test_detect_and_evict_times`: `.faults` tại t=60, `.evict` node1..3 lúc 60,8/60,9/61,0 cùng height 812 → `detect_evidence_s == 0.8`, `evict_all_s == 1.0`, `same_height == True`.
  - `test_honest_evicted_counted`: `.evict` loại node1 (không có trong FAULT) → `honest_evicted == 1`.
  - `test_no_evict_line`: không có `.evict` → các cột thời gian rỗng, có ghi chú.
  - `test_driver_dry_run_counts` (pytest gọi `--dry-run` của ba driver): E10 48 dòng `DRY_RUN:`, E11 42, E12 24 khi NS="4".
- [ ] **Step 2: Chạy, xác nhận FAIL.** `python3 -m pytest experiments/plot/test_rq6.py -q`.
- [ ] **Step 3: Cài đặt.**
- [ ] **Step 4: Chạy lại, xác nhận PASS.**
- [ ] **Step 5: Commit** `exp: E10-E12 drivers and rq6.py`.

---

## Chạy và ghi kết quả (tuần 8–10)

### Task 10: Chạy E10–E12, kiểm tra nhất quán với truy vết bật, NOTES, tag

**Files:**
- Modify: `experiments/NOTES.md`; Create: `results/phase3/` (CSV cuối, hình, `rq6_summary.csv`)

- [ ] **Step 1: Build lại ảnh** (`experiments/build.sh`) và `check_consistency.sh` cho 4 backend với `ATTEST_EVERY=1`. Kỳ vọng: `CONSISTENT`, không dòng `evicted`.
- [ ] **Step 2: Chạy E10, E11, E12** với tốc độ của Task 2. Kỳ vọng: đủ dòng theo `--dry-run`; FAULT=none qua cổng hợp lệ.
- [ ] **Step 3: Phân tích.** `python3 experiments/plot/rq6.py results/raw/e11-*.csv results/raw/e12-*.csv --raw-dir results/raw --out results/phase3`; `plot.py` cho E10. Kiểm tiêu chí spec mục 1: F4/F6 `cert-fo(-sign)`: `wrong_gets` = 0, `same_height` = True; E10 certified so với E9 cùng cấu hình: tps ghi giảm ≤ 5%.
- [ ] **Step 4: NOTES** (môi trường, sai lệch, bảng kết quả, đối chiếu dự đoán F6/F6E/F5-F4 của spec mục 3–5). `git tag p11-accountability`. Commit `results: phase 3 E10-E12`.

---

## Tùy chọn (tuần 10, cắt đầu tiên)

### Task 11: Kafka approve theo quorum và E13

**Files:**
- Modify: `consensus/kafka/tracker.go`, `consensus/kafka/tracker_test.go`, `consensus/kafka/kafka.go` (`Config.Quorum`), `cmd/veritas/main.go` (`--kafka-quorum`), `experiments/cluster/gen_compose.py`, `experiments/run_one.sh` (`KAFKA_QUORUM`)

**Interfaces:**
- Produces: `kafka.Config.Quorum int` (0 = N, approve-all như cũ). Tracker: block commit khi số Approve ≥ Quorum; Abort khi số Abort > N − Quorum; phiếu đến sau khi đã quyết bị bỏ. `--kafka-quorum=all|2f+1`.

- [ ] **Step 1: Test hỏng** `TestTrackerQuorumCommitsWithoutOneVoter` (N=4, quorum 3: 3 Approve → commit, phiếu thứ 4 bị bỏ), `TestTrackerQuorumAbort` (N=4, quorum 3: 2 Abort → abort), `TestTrackerDefaultIsApproveAll` (Quorum 0: 3 Approve chưa commit).
- [ ] **Step 2: Chạy, xác nhận FAIL** (`... go test -race ./consensus/kafka -run Quorum -v`).
- [ ] **Step 3: Cài đặt**; thứ tự quyết định vẫn theo thứ tự đọc từ topic.
- [ ] **Step 4: PASS**, `check_consistency.sh kafka` với `KAFKA_QUORUM=2f+1` → `CONSISTENT`.
- [ ] **Step 5: E13**: `BACKEND=kafka` F1/F2 × `KAFKA_QUORUM` {all, 2f+1}, N=4, 3 rep qua `e6_faults.sh` với `E6_KINDS="F1 F2"` và EXP `e13-<q>`; ghi NOTES. Commit `consensus/kafka: quorum approve and E13`.
