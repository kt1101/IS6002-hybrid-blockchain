#!/usr/bin/env python3
"""Generate the docker compose file for one experiment cluster.

The file is written as JSON, which is valid YAML, so only the standard library
is needed. Layout (network ``hbdb``, subnet 172.28.0.0/16):

    kafka   172.28.0.2     broker + zookeeper   (backends using Kafka)
    tso     172.28.0.3     timestamp oracle     (backends using a TSO)
    client  172.28.0.4     runs ycsbbench / hbdbprobe via `docker compose exec`
    node$i  172.28.0.(10+i) redis + veritas server (+ tendermint for tm-orig, tendermint)

Implemented backends: kafka-orig, tm-orig (the original Veritas code, started
with the same flags as scripts/start_veritas_{kafka,tendermint}.sh) and kafka
(the same veritas-kafka binary with --consensus=kafka: core.Server over the
deterministic consensus/kafka backend, with the kafka-orig Kafka/TSO/redis
wiring; its nodes also mount the results dir on /results for the breakdown
JSON written at shutdown). raft is the same binary with --consensus=raft
(consensus/raft): no Kafka, a TSO like kafka, and on every node besides redis
and veritas the raft transport on RAFT_PORT and the submit-forwarding HTTP
endpoint on RAFT_FORWARD_PORT (node$i: 172.28.0.(10+i):7100 and :7101); node1
bootstraps the full static peer set, the others wait to be contacted.
tendermint is the same binary with --consensus=tendermint
(consensus/tendermint) and --version-mode=ordered: no Kafka, no TSO, and on
every node a tendermint v0.35.0 process (the testnet config from up.sh, as
tm-orig, with the tendermint values of tm_config.py) whose --proxy-app is the
server's ABCI socket; one ABCI tx is one Veritas block. bft2 is accepted by
the CLI but rejected until its task adds it.

The veritas server of every node runs with GOMEMLIMIT = 70% of the node's
memory cap (768m -> 537MiB), so the Go GC works harder before the cap is hit
instead of letting the heap overshoot into an OOM kill.

Every service runs with ``init: true`` (so SIGTERM reaches the real process)
and every container of the hbdb-node image mounts a named volume on /data
(the redis base image declares VOLUME /data), so ``docker compose down -v``
leaves no data behind.
"""

import argparse
import decimal
import json
import os
import re
import sys

ALL_BACKENDS = ("kafka-orig", "tm-orig", "kafka", "raft", "tendermint", "bft2")
IMPLEMENTED = ("kafka-orig", "tm-orig", "kafka", "raft", "tendermint")
COMMIT_MODES = ("sync", "async")

PROJECT = "hbdb"
NETWORK = "hbdb"
SUBNET = "172.28.0.0/16"
KAFKA_IP = "172.28.0.2"
TSO_IP = "172.28.0.3"
CLIENT_IP = "172.28.0.4"

NODE_IMAGE = "hbdb-node"
KAFKA_IMAGE = "hbdb-kafka"

VERITAS_PORT = 1990
TSO_PORT = 7070
KAFKA_PORT = 9092
KAFKA_TOPIC = "shared-log"
TM_P2P_PORT = 26656
# raft backend: raft transport (hashicorp/raft TCP) and the forward endpoint
# (POST /submit: a follower hands a block to the leader), on every node.
RAFT_PORT = 7100
RAFT_FORWARD_PORT = 7101
# Same sockets as scripts/start_veritas_tendermint.sh (TMSOCK1 / TMSOCK2).
TM_ABCI_LISTEN = "tcp://0.0.0.0:26658"  # veritas-tm ABCI server
TM_ABCI_DIAL = "tcp://127.0.0.1:26658"  # what tendermint connects to
TM_RPC = "tcp://127.0.0.1:26657"  # tendermint RPC used by veritas-tm

# Resource caps of the non-node services (caps, not reservations).
# Kafka: broker heap 512 MB + zookeeper heap 256 MB + topic tool 128 MB.
KAFKA_CPUS, KAFKA_MEM = 2.0, "1536m"
TSO_CPUS, TSO_MEM = 0.5, "256m"
# The load generator should never be the bottleneck.
CLIENT_CPUS, CLIENT_MEM = 4.0, "2g"

KAFKA_READY_FILE = "/tmp/hbdb-ready"

# Share of the node memory cap given to the veritas server as GOMEMLIMIT.
GOMEMLIMIT_SHARE = decimal.Decimal("0.7")

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def node_ip(i):
    """IP of node i (1-based)."""
    return "172.28.0.%d" % (10 + i)


def uses_kafka(backend):
    return backend in ("kafka-orig", "kafka")


def uses_tso(backend):
    # kafka and raft run --version-mode=client: Set versions come from the
    # TSO, as in kafka-orig (without a TSO every write would carry version 0
    # and abort).
    return backend in ("kafka-orig", "kafka", "raft")


def gomemlimit(mem):
    """GOMEMLIMIT for a node memory cap like '768m' or '2g': 70%, in whole MiB.

    >>> gomemlimit("768m")
    '537MiB'
    """
    m = re.match(r"^([0-9]+(?:\.[0-9]+)?)([mMgG])$", str(mem))
    if not m:
        raise ValueError("invalid --mem %r: want a number with suffix m or g, e.g. 768m or 2g" % (mem,))
    mib = decimal.Decimal(m.group(1))
    if m.group(2) in "gG":
        mib *= 1024
    limit = int(mib * GOMEMLIMIT_SHARE)  # rounds down
    if limit < 1:
        raise ValueError("invalid --mem %r: too small for a GOMEMLIMIT of at least 1MiB" % (mem,))
    return "%dMiB" % limit


def _esc(script):
    """Escape '$' so compose does not interpolate it."""
    return script.replace("$", "$$")


def _bash(lines):
    return ["bash", "-c", _esc("\n".join(lines) + "\n")]


def _service(image, name, ip, cpus, mem):
    return {
        "image": image,
        "hostname": name,
        "init": True,
        "stop_grace_period": "5s",
        "cpus": cpus,
        "mem_limit": mem,
        "networks": {NETWORK: {"ipv4_address": ip}},
    }


REDIS_START = [
    # Snapshots/AOF off: the original ran redis with default (snapshotting)
    # settings, but a background fork of the dataset inside a 768 MB cap
    # would perturb the measurement; persistence is not part of the experiment.
    "redis-server --bind 127.0.0.1 --port 6379 --save '' --appendonly no --dir /data &",
    "until redis-cli -h 127.0.0.1 -p 6379 ping >/dev/null 2>&1; do sleep 0.2; done",
]


def _exit_when_any_exits(procs):
    """Tail of a node script whose long-lived processes all run in the
    background: exit (non-zero) as soon as any of them dies, so an OOM-killed
    or crashed process shows up as an exited container (same pattern as the
    kafka service; bash >= 4.3 for `wait -n`, the node image has bash 5.2)."""
    return [
        "wait -n || true",
        "echo 'hbdb: one of %s exited' >&2" % ", ".join(procs),
        "exit 1",
    ]


def _kafka_orig_node(i, n, block_size, memlimit):
    parties = ",".join("node%d" % j for j in range(1, n + 1))
    return [
        "set -eu",
    ] + REDIS_START + [
        "GOMEMLIMIT=%s veritas-kafka --signature=node%d --parties=%s --blk-size=%d --addr=:%d"
        " --kafka-addr=%s:%d --kafka-group=%d --kafka-topic=%s"
        " --redis-addr=127.0.0.1:6379 --redis-db=0 --ledger-path=/data/veritas%d &"
        % (memlimit, i, parties, block_size, VERITAS_PORT, KAFKA_IP, KAFKA_PORT, i, KAFKA_TOPIC, i),
    ] + _exit_when_any_exits(["redis-server", "veritas-kafka"])


def _kafka_node(i, n, block_size, commit_mode, memlimit):
    """Node of the deterministic Kafka backend (consensus/kafka + core.Server).

    Same Kafka/TSO/redis wiring as kafka-orig; the signature node<i> is the
    node's consumer group (one group per node, every node reads the whole
    topic). The breakdown JSON goes to /results (the results dir is mounted).

    tini (init: true) forwards SIGTERM only to bash, so bash traps it and
    passes it on to veritas-kafka, which then closes the server and writes
    its breakdown before the 5 s stop grace period ends.
    """
    parties = ",".join("node%d" % j for j in range(1, n + 1))
    return [
        "set -eu",
    ] + REDIS_START + _forward_sigterm(
        "GOMEMLIMIT=%s veritas-kafka --consensus=kafka --commit-mode=%s --version-mode=client --node-id=%d"
        " --signature=node%d --parties=%s --blk-size=%d --addr=:%d"
        " --kafka-addr=%s:%d --kafka-topic=%s"
        " --redis-addr=127.0.0.1:6379 --redis-db=0 --ledger-path=/data/veritas%d"
        " --breakdown-out=/results/breakdown-node%d.json &"
        % (memlimit, commit_mode, i, i, parties, block_size, VERITAS_PORT, KAFKA_IP, KAFKA_PORT,
           KAFKA_TOPIC, i, i)
    ) + _exit_when_any_exits(["redis-server", "veritas-kafka"])


def _forward_sigterm(start):
    """Lines that start veritas (start ends with ' &') so that SIGTERM/SIGINT
    reaching bash is passed on to it and bash waits for it (core backends:
    the server writes its breakdown on SIGTERM).

    The trap comes first, so no SIGTERM can arrive between the start and the
    trap; until veritas runs the trap only exits.
    """
    return [
        "veritas_pid=",
        "trap 'if [ -n \"$veritas_pid\" ]; then kill -TERM \"$veritas_pid\" 2>/dev/null || true;"
        " wait \"$veritas_pid\" || true; fi; exit 143' TERM INT",
        start,
        "veritas_pid=$!",
    ]


def raft_peers(n):
    """--raft-peers of an n-node raft cluster: node<i>=RAFTADDR/FORWARDADDR.

    >>> raft_peers(2)
    'node1=172.28.0.11:7100/172.28.0.11:7101,node2=172.28.0.12:7100/172.28.0.12:7101'
    """
    return ",".join(
        "node%d=%s:%d/%s:%d" % (j, node_ip(j), RAFT_PORT, node_ip(j), RAFT_FORWARD_PORT)
        for j in range(1, n + 1))


def _raft_node(i, n, block_size, commit_mode, memlimit):
    """Node of the Raft backend (consensus/raft + core.Server).

    Same binary, TSO and redis wiring as kafka, no Kafka. The raft server ID
    is the signature node<i>; the raft log and snapshots live in /data/raft
    (the node's named volume). node1 alone gets --raft-bootstrap (it
    bootstraps the full static peer set; the others wait to be contacted).
    The raft transport binds to all interfaces and advertises the node IP.
    SIGTERM is forwarded as for kafka, so the breakdown reaches /results.
    """
    parties = ",".join("node%d" % j for j in range(1, n + 1))
    return [
        "set -eu",
    ] + REDIS_START + _forward_sigterm(
        "GOMEMLIMIT=%s veritas-kafka --consensus=raft --commit-mode=%s --version-mode=client --node-id=%d"
        " --signature=node%d --parties=%s --blk-size=%d --addr=:%d"
        " --raft-peers=%s --raft-bind=0.0.0.0:%d --raft-dir=/data/raft%s"
        " --redis-addr=127.0.0.1:6379 --redis-db=0 --ledger-path=/data/veritas%d"
        " --breakdown-out=/results/breakdown-node%d.json &"
        % (memlimit, commit_mode, i, i, parties, block_size, VERITAS_PORT,
           raft_peers(n), RAFT_PORT, " --raft-bootstrap" if i == 1 else "", i, i)
    ) + _exit_when_any_exits(["redis-server", "veritas-kafka"])


def _tendermint_node(i, n, block_size, commit_mode, memlimit):
    """Node of the Tendermint backend (consensus/tendermint + core.Server).

    Same layout as tm-orig (tendermint home copied from the read-only /tmcfg
    into /data/tm, redis, then the veritas server, then tendermint once the
    ABCI socket listens), but the server is veritas-kafka --consensus=
    tendermint with --version-mode=ordered (no TSO). SIGTERM is forwarded to
    the server as for kafka/raft, so the breakdown reaches /results; the
    server closes its ABCI socket, tendermint is killed with the container.
    """
    parties = ",".join("node%d" % j for j in range(1, n + 1))
    return [
        "set -eu",
        "mkdir -p /data/tm && cp -R /tmcfg/. /data/tm/",
    ] + REDIS_START + _forward_sigterm(
        # GOMEMLIMIT applies to the veritas server only, not to tendermint.
        "GOMEMLIMIT=%s veritas-kafka --consensus=tendermint --commit-mode=%s --version-mode=ordered --node-id=%d"
        " --signature=node%d --parties=%s --blk-size=%d --addr=:%d"
        " --tm-abci-socket=%s --tm-rpc-addr=%s"
        " --redis-addr=127.0.0.1:6379 --redis-db=0 --ledger-path=/data/veritas%d"
        " --breakdown-out=/results/breakdown-node%d.json &"
        % (memlimit, commit_mode, i, i, parties, block_size, VERITAS_PORT,
           TM_ABCI_LISTEN, TM_RPC, i, i)
    ) + [
        # tendermint exits if the ABCI app is not listening yet.
        "until (echo > /dev/tcp/127.0.0.1/26658) 2>/dev/null; do sleep 0.2; done",
        "tendermint start --home /data/tm --proxy-app=%s &" % TM_ABCI_DIAL,
    ] + _exit_when_any_exits(["redis-server", "veritas-kafka", "tendermint"])


def _tm_orig_node(i, n, block_size, memlimit):
    parties = ",".join("node%d" % j for j in range(1, n + 1))
    return [
        "set -eu",
        # Tendermint home lives in the node's named volume; /tmcfg is the
        # read-only config generated by up.sh (`tendermint testnet`).
        "mkdir -p /data/tm && cp -R /tmcfg/. /data/tm/",
    ] + REDIS_START + [
        # GOMEMLIMIT applies to veritas-tm only, not to tendermint.
        "GOMEMLIMIT=%s veritas-tm --signature=node%d --parties=%s --blk-size=%d --addr=:%d"
        " --redis-addr=127.0.0.1:6379 --redis-db=0 --ledger-path=/data/veritas%d"
        " --tendermint-socket=%s --abci-socket=%s &"
        % (memlimit, i, parties, block_size, VERITAS_PORT, i, TM_ABCI_LISTEN, TM_RPC),
        # tendermint exits if the ABCI app is not listening yet.
        "until (echo > /dev/tcp/127.0.0.1/26658) 2>/dev/null; do sleep 0.2; done",
        "tendermint start --home /data/tm --proxy-app=%s &" % TM_ABCI_DIAL,
    ] + _exit_when_any_exits(["redis-server", "veritas-tm", "tendermint"])


def _kafka_service():
    s = _service(KAFKA_IMAGE, "kafka", KAFKA_IP, KAFKA_CPUS, KAFKA_MEM)
    s["working_dir"] = "/opt/kafka"
    s["command"] = _bash([
        "set -u",
        "rm -f %s" % KAFKA_READY_FILE,
        "KAFKA_HEAP_OPTS='-Xms256m -Xmx256m' bin/zookeeper-server-start.sh config/zookeeper.properties &",
        "until (echo > /dev/tcp/127.0.0.1/2181) 2>/dev/null; do sleep 0.5; done",
        # Same change as scripts/start_veritas_kafka.sh: advertise the broker IP.
        "KAFKA_HEAP_OPTS='-Xms512m -Xmx512m' bin/kafka-server-start.sh config/server.properties"
        " --override advertised.listeners=PLAINTEXT://%s:%d &" % (KAFKA_IP, KAFKA_PORT),
        "until KAFKA_HEAP_OPTS='-Xmx128m' bin/kafka-topics.sh --create --if-not-exists"
        " --topic %s --bootstrap-server 127.0.0.1:%d; do sleep 1; done" % (KAFKA_TOPIC, KAFKA_PORT),
        "touch %s" % KAFKA_READY_FILE,
        "echo 'hbdb: kafka ready, topic %s exists'" % KAFKA_TOPIC,
        # Exit (and show up in `docker compose ps`) as soon as zk or kafka dies.
        "wait -n",
        "echo 'hbdb: zookeeper or kafka exited' >&2",
        "exit 1",
    ])
    s["healthcheck"] = {
        "test": ["CMD", "test", "-f", KAFKA_READY_FILE],
        "interval": "2s",
        "timeout": "5s",
        "retries": 90,
    }
    return s


def gen(backend, n, block_size, commit_mode, cpus, mem,
        tm_dir=None, ycsb_dir=None, results_dir=None):
    """Return the compose document (a dict) for one cluster.

    commit_mode is accepted for every backend; the original backends have a
    fixed behaviour (kafka-orig async, tm-orig sync) and ignore it; the new
    backends pass it to the server as --commit-mode.
    tm_dir is the `tendermint testnet` output (node0..node{n-1}), mounted
    read-only into node1..nodeN (tm-orig and tendermint).
    """
    if backend not in ALL_BACKENDS:
        raise ValueError("unknown backend %r" % backend)
    if backend not in IMPLEMENTED:
        raise NotImplementedError("backend %s not implemented yet" % backend)
    if n < 1:
        raise ValueError("n must be >= 1")
    if block_size < 1:
        raise ValueError("block_size must be >= 1")
    if commit_mode not in COMMIT_MODES:
        raise ValueError("commit_mode must be one of %s" % (COMMIT_MODES,))
    memlimit = gomemlimit(mem)

    gen_dir = os.path.join(REPO_ROOT, "experiments", "cluster", "generated")
    tm_dir = os.path.abspath(tm_dir or os.path.join(gen_dir, "tm"))
    ycsb_dir = os.path.abspath(ycsb_dir or os.path.join(REPO_ROOT, "results", "ycsb"))
    results_dir = os.path.abspath(results_dir or os.path.join(REPO_ROOT, "results", "raw"))

    services = {}
    volumes = {}

    if uses_kafka(backend):
        services["kafka"] = _kafka_service()

    if uses_tso(backend):
        tso = _service(NODE_IMAGE, "tso", TSO_IP, TSO_CPUS, TSO_MEM)
        tso["working_dir"] = "/data"  # the oracle writes orc.log into its cwd
        tso["command"] = ["veritas-tso", "--addr=:%d" % TSO_PORT]
        tso["volumes"] = ["tso-data:/data"]
        volumes["tso-data"] = {}
        services["tso"] = tso

    for i in range(1, n + 1):
        name = "node%d" % i
        s = _service(NODE_IMAGE, name, node_ip(i), cpus, mem)
        s["cap_add"] = ["NET_ADMIN"]
        vol = "%s-data" % name
        volumes[vol] = {}
        s["volumes"] = ["%s:/data" % vol]
        if backend == "kafka-orig":
            s["command"] = _bash(_kafka_orig_node(i, n, block_size, memlimit))
            s["depends_on"] = {"kafka": {"condition": "service_healthy"}}
        elif backend == "kafka":
            s["command"] = _bash(_kafka_node(i, n, block_size, commit_mode, memlimit))
            s["depends_on"] = {"kafka": {"condition": "service_healthy"}}
            s["volumes"].append("%s:/results" % results_dir)
        elif backend == "raft":
            s["command"] = _bash(_raft_node(i, n, block_size, commit_mode, memlimit))
            s["volumes"].append("%s:/results" % results_dir)
        elif backend == "tendermint":
            s["command"] = _bash(_tendermint_node(i, n, block_size, commit_mode, memlimit))
            s["volumes"].append("%s:/tmcfg:ro" % os.path.join(tm_dir, "node%d" % (i - 1)))
            s["volumes"].append("%s:/results" % results_dir)
        elif backend == "tm-orig":
            s["command"] = _bash(_tm_orig_node(i, n, block_size, memlimit))
            s["volumes"].append("%s:/tmcfg:ro" % os.path.join(tm_dir, "node%d" % (i - 1)))
        services[name] = s

    client = _service(NODE_IMAGE, "client", CLIENT_IP, CLIENT_CPUS, CLIENT_MEM)
    client["command"] = ["sleep", "infinity"]
    client["volumes"] = [
        "client-data:/data",
        "%s:/ycsb:ro" % ycsb_dir,
        "%s:/results" % results_dir,
    ]
    volumes["client-data"] = {}
    services["client"] = client

    return {
        "name": PROJECT,
        "networks": {NETWORK: {"driver": "bridge", "ipam": {"config": [{"subnet": SUBNET}]}}},
        "volumes": volumes,
        "services": services,
    }


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--backend", required=True, choices=ALL_BACKENDS)
    p.add_argument("--n", type=int, required=True, help="number of veritas nodes")
    p.add_argument("--block-size", type=int, default=100)
    p.add_argument("--commit-mode", default="sync", choices=COMMIT_MODES)
    p.add_argument("--cpus", type=float, default=0.5, help="CPU cap per node")
    p.add_argument("--mem", default="768m", help="memory cap per node")
    p.add_argument("--out", default="-", help="output file ('-' = stdout)")
    p.add_argument("--tm-dir", default=None, help="tendermint testnet dir (tm-orig, tendermint)")
    p.add_argument("--ycsb-dir", default=None, help="YCSB data dir, mounted at /ycsb in client")
    p.add_argument("--results-dir", default=None, help="results dir, mounted at /results in client")
    a = p.parse_args(argv)

    cpus = int(a.cpus) if a.cpus == int(a.cpus) else a.cpus
    try:
        doc = gen(a.backend, a.n, a.block_size, a.commit_mode, cpus, a.mem,
                  tm_dir=a.tm_dir, ycsb_dir=a.ycsb_dir, results_dir=a.results_dir)
    except NotImplementedError as e:
        sys.stderr.write("gen_compose: %s\n" % e)
        return 2
    except ValueError as e:
        sys.stderr.write("gen_compose: %s\n" % e)
        return 1

    text = json.dumps(doc, indent=2, sort_keys=False) + "\n"
    if a.out == "-":
        sys.stdout.write(text)
    else:
        tmp = a.out + ".tmp"
        with open(tmp, "w") as f:
            f.write(text)
        os.replace(tmp, a.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
