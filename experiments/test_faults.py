"""pytest entry point for the bash unit tests of faults.sh / net.sh (test_faults.sh).

Runs them under /bin/bash (3.2 on macOS, the shell the harness targets) and
under `bash` from PATH when that is a different binary.
"""
import os
import shutil
import subprocess

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(HERE, "test_faults.sh")

SHELLS = ["/bin/bash"]
_path_bash = shutil.which("bash")
if _path_bash and os.path.realpath(_path_bash) != os.path.realpath("/bin/bash"):
    SHELLS.append(_path_bash)


@pytest.mark.parametrize("shell", SHELLS)
def test_faults_sh(shell):
    p = subprocess.run([shell, SCRIPT], capture_output=True, text=True)
    assert p.returncode == 0, p.stdout + p.stderr
    assert "0 failed" in p.stdout
