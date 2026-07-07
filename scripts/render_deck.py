"""Wrapper around marp-cli for rendering Marp `.slides.md` decks.

Solves the "HPC has no system browser" problem by resolving chrome from the
puppeteer download cache populated by ``pixi run -e marp marp-setup``, then
setting ``CHROME_PATH`` + ``MARP_USER=root`` (so marp-cli accepts the
sandboxless chrome that caldera-style environments require — no setuid
sandbox, no user namespace). With those env vars set, ``npx --yes
@marp-team/marp-cli`` rendering to PDF/PPTX/PNG works the same way on HPC
as it does on a workstation with Chrome installed.

The script appends ``--allow-local-files`` automatically (Marp decks reference
figures via relative paths and refuse to read them without this flag) and
otherwise passes through every argument to marp-cli verbatim.

Usage::

    # PDF render of one deck
    pixi run -e marp render-deck docs/presentations/<file>.slides.md --pdf

    # HTML render (no browser needed; chrome resolution is skipped)
    pixi run -e marp render-deck docs/presentations/<file>.slides.md --html

    # Live-reload server (open the printed URL in any browser; works through
    # an SSH port-forward on HPC)
    pixi run -e marp render-deck docs/presentations/ --server
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

#: Minimum bytes for a chrome binary to be considered a successful install.
#: Real chrome-headless-shell is ~150 MB; full chrome is ~200 MB. Anything
#: smaller than 1 MB is almost certainly a partial-extract from an
#: interrupted ``puppeteer browsers install`` (NFS quota, SLURM timeout,
#: network drop). Skip such candidates so a half-populated cache doesn't
#: short-circuit a re-install. Issue raised by silent-failure-hunter
#: review of #209.
_MIN_CHROME_BYTES = 1_000_000


def _is_usable_chrome(path: Path) -> bool:
    """True if *path* looks like a complete chrome binary (not a partial download)."""
    try:
        return path.is_file() and path.stat().st_size >= _MIN_CHROME_BYTES
    except OSError:
        return False


def _find_chromium() -> str:
    """Locate a chromium/chrome binary, preferring the puppeteer-installed one.

    Discovery order:
    1. ``CHROME_PATH`` env var (if the user explicitly set one).
    2. Puppeteer's download cache (``~/.cache/puppeteer/chrome-headless-shell/<rev>/...``
       and ``.../chrome/<rev>/...``, populated by ``pixi run -e marp marp-setup``).
       Honors ``PUPPETEER_CACHE_DIR`` override if set.
    3. ``$CONDA_PREFIX/bin/chrome*`` — forward compatibility for a future world
       where conda-forge ships chrome (it doesn't today).
    4. System PATH — workstation Chrome / Edge / Chromium installs (macOS, Windows,
       desktop Linux). Note: marp-cli uses puppeteer-core internally which only
       drives Chromium-family browsers; Firefox won't work even if found.

    Raises ``SystemExit`` with the install hint if none found. Candidates whose
    file size is suspiciously small are skipped (partial-extract guard).
    """
    # 1. Explicit env override
    explicit = os.environ.get("CHROME_PATH", "").strip()
    if explicit and _is_usable_chrome(Path(explicit)):
        return explicit

    # 2. Puppeteer cache — where `npx puppeteer browsers install ...` writes.
    # Default cache dir is ~/.cache/puppeteer on Linux/macOS, %LOCALAPPDATA%\puppeteer
    # on Windows. PUPPETEER_CACHE_DIR overrides.
    cache_root = os.environ.get("PUPPETEER_CACHE_DIR", "").strip()
    if not cache_root:
        if sys.platform == "win32":
            cache_root = os.path.join(
                os.environ.get("LOCALAPPDATA", str(Path.home())), "puppeteer"
            )
        else:
            cache_root = str(Path.home() / ".cache" / "puppeteer")
    # Search both chrome-headless-shell (the smaller variant marp-setup pulls
    # by default — works on bare HPC) and chrome (full GUI variant — operator
    # may have installed it manually for a workstation render path). Prefer
    # headless-shell since marp-cli is happy with either.
    for browser_dir, layout_candidates in (
        (
            "chrome-headless-shell",
            (
                Path("chrome-headless-shell-linux64") / "chrome-headless-shell",
                Path("chrome-headless-shell-mac-arm64") / "chrome-headless-shell",
                Path("chrome-headless-shell-mac-x64") / "chrome-headless-shell",
                Path("chrome-headless-shell-win64") / "chrome-headless-shell.exe",
            ),
        ),
        (
            "chrome",
            (
                Path("chrome-linux64") / "chrome",
                Path("chrome-mac-arm64") / "Google Chrome for Testing.app"
                / "Contents" / "MacOS" / "Google Chrome for Testing",
                Path("chrome-mac-x64") / "Google Chrome for Testing.app"
                / "Contents" / "MacOS" / "Google Chrome for Testing",
                Path("chrome-win64") / "chrome.exe",
            ),
        ),
    ):
        browser_root = Path(cache_root) / browser_dir
        if not browser_root.is_dir():
            continue
        # Each install lands under a revision-tagged subdir; pick the newest
        # so a later `marp-setup` re-run automatically supersedes the old one.
        revisions = sorted(
            (p for p in browser_root.iterdir() if p.is_dir()),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        for rev in revisions:
            for relative in layout_candidates:
                candidate = rev / relative
                if _is_usable_chrome(candidate):
                    return str(candidate)

    # 3. Pixi env (forward compatibility — conda-forge has no chrome today)
    env_prefix = os.environ.get("CONDA_PREFIX", "")
    if env_prefix:
        for name in ("chromium", "chrome", "google-chrome"):
            candidate = Path(env_prefix) / "bin" / name
            if _is_usable_chrome(candidate):
                return str(candidate)

    # 4. System PATH (workstation Chromium-family browsers)
    for name in ("chromium", "chromium-browser", "chrome", "google-chrome", "msedge"):
        path = shutil.which(name)
        if path and _is_usable_chrome(Path(path)):
            return path

    raise SystemExit(
        "ERROR: No chrome/chromium binary found (or all candidates were "
        f"smaller than {_MIN_CHROME_BYTES:_} bytes, suggesting a partial "
        "install).\n\n"
        "On HPC (or any fresh pixi checkout): run the two-step install:\n"
        "  pixi install -e marp\n"
        "  pixi run -e marp marp-setup\n\n"
        "The second step downloads chrome (~150 MB) via puppeteer into\n"
        "~/.cache/puppeteer/. Or set CHROME_PATH=/path/to/chrome explicitly.\n"
        "If the install died mid-extract, `rm -rf ~/.cache/puppeteer/chrome*` "
        "and re-run marp-setup."
    )


def _chrome_version(chrome_path: str) -> str:
    """Best-effort ``chrome --version`` for the resolved binary. Empty on failure."""
    try:
        out = subprocess.run(
            [chrome_path, "--version"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        return (out.stdout or out.stderr).strip()
    except (OSError, subprocess.TimeoutExpired):
        return ""


#: marp-cli output flags whose backend requires a browser. ``--server`` and
#: ``--watch`` open a live-reload HTTP server (HTML only — no chrome needed
#: for the page itself; the user's browser handles rendering).
_BROWSER_FLAGS = ("--pdf", "--pdf-notes", "--pptx", "--png", "--jpeg", "--preview")


def _needs_browser(args: list[str]) -> bool:
    """True if any arg starts with a browser-backed marp-cli flag.

    Matches both bare flags (``--pdf``) and the ``--flag=value`` form
    (``--pdf=output.pdf``) — splitting on ``=`` so the membership check
    isn't whitespace/value-fragile (silent-failure-hunter review of #209).
    """
    return any(arg.split("=", 1)[0] in _BROWSER_FLAGS for arg in args)


def main() -> int:
    args = sys.argv[1:]
    if not args:
        print(__doc__, file=sys.stderr)
        return 2

    # Marp refuses to embed local images without this flag, and every deck
    # under docs/presentations/ references figures via relative paths. Adding
    # it unconditionally is harmless for HTML/server output (no images would
    # break either way).
    if "--allow-local-files" not in args:
        args.append("--allow-local-files")

    env = dict(os.environ)
    # Only browser-backed outputs (PDF/PPTX/PNG/JPEG/preview) need chrome.
    # HTML and the live-reload server work without it; skip the resolve so
    # the script is useful even on a fresh pixi install before chrome has
    # been pulled.
    if _needs_browser(args):
        chrome_path = _find_chromium()
        env["CHROME_PATH"] = chrome_path
        # MARP_USER=root tells marp-cli "yes, I know this chrome has no
        # setuid sandbox; render anyway." Required on HPC where user
        # namespaces and chrome's normal sandbox can't run. The variable
        # name is misleading (we're not root) but is what marp-cli checks.
        sandbox_mode_log = ""
        if env.get("MARP_USER") != "root":
            env["MARP_USER"] = "root"
            sandbox_mode_log = " (MARP_USER=root — sandboxless mode, HPC default)"
        # Log the resolved chrome + sandbox mode so operators debugging a
        # hung render know what was launched. Silent fallbacks were called
        # out by silent-failure-hunter review.
        version = _chrome_version(chrome_path)
        version_suffix = f" — {version}" if version else ""
        print(
            f"render-deck: chrome at {chrome_path}{version_suffix}{sandbox_mode_log}",
            file=sys.stderr,
        )

    cmd = ["npx", "--yes", "@marp-team/marp-cli", *args]
    try:
        return subprocess.call(cmd, env=env)
    except FileNotFoundError:
        # npx isn't on PATH — common when the operator forgot to run inside
        # the pixi marp env. Friendlier than a bare FileNotFoundError trace.
        raise SystemExit(
            "ERROR: npx not on PATH. The `render-deck` task is designed to "
            "run inside the pixi marp environment:\n"
            "  pixi install -e marp\n"
            "  pixi run -e marp render-deck <slides.md> ...\n"
        )


if __name__ == "__main__":
    raise SystemExit(main())
