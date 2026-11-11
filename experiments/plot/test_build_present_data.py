"""Tests for experiments/plot/build_present_data.py and the presentation
pages it feeds (demo/web/static/present/).

Run: cd experiments/plot && python3 -m pytest -q test_build_present_data.py

The parser tests are hermetic. The tests marked "source" need the results
commit (PRESENT_REF, default the tag p11-accountability) in the local git repo and are skipped
without it: data.js must be up to date, and every number the pages print
must appear in one of the source files of that commit.
"""
import html.parser
import json
import os
import re
import subprocess
from pathlib import Path

import pytest

import build_present_data as bp

ROOT = Path(__file__).resolve().parents[2]
PRESENT = ROOT / "demo" / "web" / "static" / "present"
DATA_JS = PRESENT / "data.js"
REF = os.environ.get("PRESENT_REF", "p11-accountability")
PAGES = ["index.html", "1-bai-toan.html", "2-kien-truc.html", "3-co-che.html",
         "4-ket-qua.html", "5-gioi-han.html", "6-kich-ban.html"]

NOTES = """# T

## A
text
| x | y |
|---|---|
| 1 | 2,5 |

| p | q |
|:--|--:|
| `a` | **b** |

### A.1
| s |
|---|
| inner |

## B
| z |
|---|
| 9 |
"""


def test_cells_strip_pipes_and_spaces():
    assert bp._cells("| a | b c |") == ["a", "b c"]
    assert bp._cells("a|b") == ["a", "b"]


def test_section_stops_at_same_level_and_keeps_subsections():
    lines = bp.section(NOTES, "## A")
    assert "### A.1" in lines
    assert "## B" not in lines
    assert bp.section(NOTES, "### A.1")[1] == "|---|"


def test_tables_split_and_drop_rule():
    t = bp.tables(bp.section(NOTES, "## A"))
    assert [x["header"] for x in t] == [["x", "y"], ["p", "q"], ["s"]]
    assert t[0]["rows"] == [["1", "2,5"]]
    assert t[1]["rows"] == [["`a`", "**b**"]]


def test_notes_table_index_and_missing():
    assert bp.notes_table(NOTES, "## B", 0)["rows"] == [["9"]]
    with pytest.raises(KeyError):
        bp.notes_table(NOTES, "## A", 5)
    with pytest.raises(KeyError):
        bp.notes_table(NOTES, "## C", 0)


def test_read_csv_keeps_strings():
    rows = bp.read_csv("a,b\n1.230,\n")
    assert rows == [{"a": "1.230", "b": ""}]


def _data():
    s = DATA_JS.read_text(encoding="utf-8")
    return json.loads(s[s.index("=") + 1:s.rstrip().rindex(";")])


def test_data_js_has_every_dataset_the_pages_use():
    d = _data()
    pages = "".join((PRESENT / p).read_text(encoding="utf-8") for p in PAGES)
    for key in re.findall(r'data-notes(?:-chart)?="([^"]+)"', pages):
        assert key in d["notes"], key
    for key in re.findall(r'data-csv="([^"]+)"', pages):
        assert key in d["csv"], key
    for ds in list(d["notes"].values()) + list(d["csv"].values()):
        assert ds["source"] and ds["commit"] == d["commit_short"]


def test_pages_link_the_same_assets_and_each_other():
    for p in PAGES:
        s = (PRESENT / p).read_text(encoding="utf-8")
        assert '<html lang="vi">' in s, p
        for asset in ('href="../css/app.css"', 'href="present.css"', 'src="data.js"', 'src="present.js"'):
            assert asset in s, (p, asset)
        assert 'href="../"' in s and 'href="../overview"' in s, p
    js = (PRESENT / "present.js").read_text(encoding="utf-8")
    for p in PAGES:
        assert "'%s'" % p in js, p


def _have_ref():
    r = subprocess.run(["git", "rev-parse", "--verify", "-q", REF + "^{commit}"], cwd=ROOT,
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return r.returncode == 0


source = pytest.mark.skipif(not _have_ref(), reason="results commit %s not in this repo" % REF)


@source
def test_data_js_is_up_to_date():
    cwd = os.getcwd()
    try:
        os.chdir(ROOT)
        assert bp.main(["--ref", REF, "--out", str(DATA_JS), "--check"]) == 0
    finally:
        os.chdir(cwd)


class _Text(html.parser.HTMLParser):
    """Visible text of a page (no script/style)."""

    def __init__(self):
        super().__init__()
        self.out, self._skip = [], 0

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style"):
            self._skip += 1

    def handle_endtag(self, tag):
        if tag in ("script", "style"):
            self._skip -= 1

    def handle_data(self, data):
        if not self._skip:
            self.out.append(data)


# Files whose text may back a number printed on the pages.
SOURCE_FILES = ["experiments/NOTES.md", "docs/superpowers/specs/2026-11-09-phase3-accountability-design.md",
                "docs/superpowers/specs/2026-11-03-phase2-fair-comparison-and-verifiability-design.md",
                "experiments/exp/exp_lib.sh", "README.md", "docs/demo.md"] + list(bp.CSV_FILES.values())


def _numbers(text):
    """Numeric tokens as printed: 0,29–0,46 -> 0,29 and 0,46; 12.790 stays.
    Commit hashes (7+ hex characters with a digit and a letter) are not numbers."""
    text = re.sub(r"\b(?=[0-9a-f]*[a-f])(?=[0-9a-f]*\d)[0-9a-f]{7,40}\b", " ", text)
    return re.findall(r"\d+(?:[.,]\d+)*", text)


@source
def test_every_number_on_the_pages_is_in_a_source_file():
    corpus = []
    for f in SOURCE_FILES:
        try:
            corpus.append(bp.git_show(REF, f))
        except subprocess.CalledProcessError:
            corpus.append((ROOT / f).read_text(encoding="utf-8"))
    corpus = "\n".join(corpus)
    known = set(_numbers(corpus))
    missing = {}
    for p in PAGES:
        t = _Text()
        t.feed((PRESENT / p).read_text(encoding="utf-8"))
        for n in _numbers(" ".join(t.out)):
            if n not in known:
                missing.setdefault(p, []).append(n)
    assert not missing, missing
