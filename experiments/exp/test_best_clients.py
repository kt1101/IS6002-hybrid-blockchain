import pytest

from best_clients import best_clients, main

HEADER = ("ts,commit,exp,backend,n,f,clients,block_size,workload,rtt_ms,fault,rep,"
          "commit_mode,tps,lat_mean_ms,lat_p50_ms,lat_p99_ms,errors,wrong_gets,"
          "host_cpu_max_pct,saturated,net_bytes_per_block")


def row(backend, n, clients, rep, tps):
    return ("2026-10-29T00:00:00Z,abc1234,p0,%s,%s,0,%s,100,a,0,none,%s,sync,%s,"
            "1,1,1,0,0,10,0,") % (backend, n, clients, rep, tps)


def write_csv(tmp_path, rows):
    p = tmp_path / "p0.csv"
    p.write_text("\n".join([HEADER] + rows) + "\n")
    return str(p)


def test_normal_pick_uses_mean_over_reps(tmp_path):
    # clients=16 has the single highest rep (900) but the lower mean (500);
    # clients=64 has the highest mean (600).
    path = write_csv(tmp_path, [
        row("kafka-orig", 4, 4, 1, 100), row("kafka-orig", 4, 4, 2, 120),
        row("kafka-orig", 4, 16, 1, 900), row("kafka-orig", 4, 16, 2, 100),
        row("kafka-orig", 4, 64, 1, 600), row("kafka-orig", 4, 64, 2, 600),
        row("kafka-orig", 4, 256, 1, 300),
    ])
    assert best_clients(path, "kafka-orig") == 64


def test_tie_picks_smaller_clients(tmp_path):
    path = write_csv(tmp_path, [
        row("tm-orig", 4, 64, 1, 50), row("tm-orig", 4, 16, 1, 40),
        row("tm-orig", 4, 16, 2, 60), row("tm-orig", 4, 256, 1, 50),
    ])
    assert best_clients(path, "tm-orig") == 16


def test_other_backends_and_other_n_are_ignored(tmp_path):
    path = write_csv(tmp_path, [
        row("tm-orig", 4, 16, 1, 20),
        row("tm-orig", 4, 64, 1, 10),
        row("kafka-orig", 4, 256, 1, 9999),   # other backend
        row("tm-orig", 8, 256, 1, 9999),      # other N
        row("tm-orig", 16, 256, 1, 9999),
    ])
    assert best_clients(path, "tm-orig") == 16


def test_no_rows_for_backend_is_an_error(tmp_path):
    path = write_csv(tmp_path, [row("kafka-orig", 4, 16, 1, 100), row("tm-orig", 8, 16, 1, 5)])
    with pytest.raises(ValueError, match="tm-orig"):
        best_clients(path, "tm-orig")


def test_header_only_csv_is_an_error(tmp_path):
    with pytest.raises(ValueError):
        best_clients(write_csv(tmp_path, []), "kafka-orig")


def test_non_numeric_tps_rows_are_skipped(tmp_path):
    path = write_csv(tmp_path, [row("kafka-orig", 4, 4, 1, ""), row("kafka-orig", 4, 16, 1, 7)])
    assert best_clients(path, "kafka-orig") == 16


def test_cli_prints_one_integer(tmp_path, capsys):
    path = write_csv(tmp_path, [row("kafka-orig", 4, 64, 1, 5), row("kafka-orig", 4, 4, 1, 1)])
    assert main(["best_clients.py", path, "kafka-orig"]) == 0
    assert capsys.readouterr().out == "64\n"


def test_cli_errors_exit_nonzero_with_message(tmp_path, capsys):
    path = write_csv(tmp_path, [row("kafka-orig", 4, 4, 1, 1)])
    assert main(["best_clients.py", path, "tm-orig"]) == 1
    assert "no N=4 rows" in capsys.readouterr().err
    assert main(["best_clients.py", str(tmp_path / "missing.csv"), "tm-orig"]) == 1
    assert main(["best_clients.py"]) == 1


def test_reordered_columns_are_read_by_name(tmp_path):
    header = "tps,clients,n,backend,rep"
    p = tmp_path / "p0.csv"
    p.write_text("\n".join([
        header,
        "10,4,4,kafka-orig,1",
        "50,64,4,kafka-orig,1",
        "99,64,8,kafka-orig,1",   # other N
        "70,16,4,tm-orig,1",      # other backend
    ]) + "\n")
    assert best_clients(str(p), "kafka-orig") == 64


@pytest.mark.parametrize("missing", ["tps", "clients"])
def test_missing_column_is_a_clear_error(tmp_path, capsys, missing):
    cols = [c for c in ("backend", "n", "clients", "tps") if c != missing]
    p = tmp_path / "p0.csv"
    p.write_text(",".join(cols) + "\n" + ",".join(["tm-orig", "4", "16", "5"][:len(cols)]) + "\n")
    with pytest.raises(ValueError, match="no column '%s'" % missing):
        best_clients(str(p), "tm-orig")
    assert main(["best_clients.py", str(p), "tm-orig"]) == 1
    assert "'%s'" % missing in capsys.readouterr().err


def test_extra_reps_all_count_in_the_mean(tmp_path):
    # clients=4: reps 100,100,100 -> 100; clients=16: reps 90,90,90 then extra
    # reps 200,200 -> mean 134 > 100.
    path = write_csv(tmp_path, [
        row("tm-orig", 4, 4, 1, 100), row("tm-orig", 4, 4, 2, 100), row("tm-orig", 4, 4, 3, 100),
        row("tm-orig", 4, 16, 1, 90), row("tm-orig", 4, 16, 2, 90), row("tm-orig", 4, 16, 3, 90),
        row("tm-orig", 4, 16, 4, 200), row("tm-orig", 4, 16, 5, 200),
    ])
    assert best_clients(path, "tm-orig") == 16
