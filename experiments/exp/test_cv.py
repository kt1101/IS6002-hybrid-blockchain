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
