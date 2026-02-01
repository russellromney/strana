"""Enable `python -m graphd` to run the graphd server binary."""
from __future__ import annotations

import os
import sys
import sysconfig


def find_graphd_bin() -> str:
    """Locate the graphd binary installed by pip."""
    exe = "graphd" + (sysconfig.get_config_var("EXE") or "")

    # Standard scripts directory (pip install)
    scripts = os.path.join(sysconfig.get_path("scripts"), exe)
    if os.path.isfile(scripts):
        return scripts

    # User install (pip install --user)
    user_scripts = os.path.join(sysconfig.get_path("scripts", "posix_user"), exe)
    if os.path.isfile(user_scripts):
        return user_scripts

    raise FileNotFoundError(
        f"Could not find graphd binary. Searched:\n  {scripts}\n  {user_scripts}"
    )


if __name__ == "__main__":
    graphd = find_graphd_bin()
    if sys.platform == "win32":
        import subprocess
        raise SystemExit(subprocess.call([graphd, *sys.argv[1:]]))
    else:
        os.execvp(graphd, [graphd, *sys.argv[1:]])
