import statistics

import pytest

from cv import config_cv, cv_of, main
from test_best_clients import row, write_csv


def test_cv_of_is_sample_std_over_mean():
    vals = [90.0, 100.0, 110.0]
    assert cv_of(vals) == pytest.approx(statistics.stdev(vals) / 100.0)
    assert cv_of(vals) == pytest.approx(0.1)


def test_cv_undefined_for_one_value_or_zero_mean():
    assert cv_of([5.0]) is None
    assert cv_of([]) is None
    assert cv_of([0.0, 0.0]) is None


def test_config_cv_uses_only_that_configuration(tmp_path):
    path = write_csv(tmp_path, [
        row("tm-orig", 4, 16, 1, 90), row("tm-orig", 4, 16, 2, 100), row("tm-orig", 4, 16, 3, 110),
        row("tm-orig", 4, 64, 1, 1), row("tm-orig", 4, 64, 2, 1000),   # other clients
        row("tm-orig", 8, 16, 1, 1),                                    # other N
        row("kafka-orig", 4, 16, 1, 5000),                              # other backend
    ])
    cv, rows = config_cv(path, "tm-orig", 4, 16)
    assert rows == 3
    assert cv == pytest.approx(0.1)


def test_extra_reps_are_included(tmp_path):
    base = [row("tm-orig", 4, 16, r, 100) for r in (1, 2, 3)]
    assert config_cv(write_csv(tmp_path, base), "tm-orig", 4, 16) == (0.0, 3)
    cv, rows = config_cv(write_csv(tmp_path, base + [row("tm-orig", 4, 16, 4, 300)]), "tm-orig", 4, 16)
    assert rows == 4 and cv > 0.5


def test_cli_output(tmp_path, capsys):
    path = write_csv(tmp_path, [row("tm-orig", 4, 16, 1, 90), row("tm-orig", 4, 16, 2, 110)])
    assert main(["cv.py", path, "tm-orig", "4", "16"]) == 0
    assert capsys.readouterr().out == "0.1414 2\n"
    assert main(["cv.py", path, "tm-orig", "4", "64"]) == 0
    assert capsys.readouterr().out == "NA 0\n"


def test_cli_errors(tmp_path, capsys):
    assert main(["cv.py"]) == 1
    assert main(["cv.py", str(tmp_path / "missing.csv"), "tm-orig", "4", "16"]) == 1
    assert main(["cv.py", str(tmp_path / "x.csv"), "tm-orig", "four", "16"]) == 1


def _wide_row(block, workload, rtt, fault, rep, tps, f=0):
    # Same layout as test_best_clients.HEADER, with the E3-E6 columns varied.
    return ("2026-10-29T00:00:00Z,abc1234,e3,raft,4,%s,16,%s,%s,%s,%s,%s,sync,%s,"
            "1,1,1,0,0,10,0,") % (f, block, workload, rtt, fault, rep, tps)


def test_where_filters_separate_configurations_that_share_backend_n_clients(tmp_path):
    path = write_csv(tmp_path, [
        _wide_row(10, "a", 0, "none", 1, 100), _wide_row(10, "a", 0, "none", 2, 100),
        _wide_row(100, "a", 0, "none", 1, 500), _wide_row(100, "a", 0, "none", 2, 900),
        _wide_row(100, "b", 0, "none", 1, 1),
        _wide_row(100, "a", 20, "none", 1, 7),
        _wide_row(100, "a", 0, "F1:node4@60", 1, 3),
    ])
    assert config_cv(path, "raft", 4, 16, {"block_size": "10"}) == (0.0, 2)
    cv, rows = config_cv(path, "raft", 4, 16, {"block_size": 100, "workload": "a", "rtt_ms": "0", "fault": "none"})
    assert rows == 2 and cv == pytest.approx(statistics.stdev([500, 900]) / 700)
    assert config_cv(path, "raft", 4, 16, {"fault": "F1:node4@60"})[1] == 1
    # no filter: every row of (raft, 4, 16)
    assert config_cv(path, "raft", 4, 16)[1] == 7


def test_where_unknown_column_is_a_clear_error(tmp_path):
    path = write_csv(tmp_path, [row("raft", 4, 16, 1, 1)])
    with pytest.raises(ValueError, match="no column 'nope'"):
        config_cv(path, "raft", 4, 16, {"nope": "1"})


def test_cli_filters(tmp_path, capsys):
    path = write_csv(tmp_path, [_wide_row(10, "a", 0, "none", 1, 90), _wide_row(10, "a", 0, "none", 2, 110),
                                _wide_row(100, "a", 0, "none", 1, 5)])
    assert main(["cv.py", path, "raft", "4", "16", "block_size=10", "fault=none"]) == 0
    assert capsys.readouterr().out == "0.1414 2\n"
    assert main(["cv.py", path, "raft", "4", "16", "block_size=1000"]) == 0
    assert capsys.readouterr().out == "NA 0\n"
    assert main(["cv.py", path, "raft", "4", "16", "block_size"]) == 1
    assert main(["cv.py", path, "raft", "4", "16", "nope=1"]) == 1
