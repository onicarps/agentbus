#!/usr/bin/env python3
"""Retag a pure-python wheel with a platform tag (for embedded native binaries)."""
from __future__ import annotations

import argparse
import re
import shutil
import sys
import zipfile
from pathlib import Path


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("wheel", type=Path)
    ap.add_argument(
        "--platform-tag",
        required=True,
        help="e.g. manylinux_2_17_x86_64, macosx_11_0_arm64, win_amd64",
    )
    ap.add_argument("--out-dir", type=Path, default=None)
    args = ap.parse_args()
    wheel: Path = args.wheel
    if not wheel.is_file():
        print(f"missing wheel: {wheel}", file=sys.stderr)
        return 1

    # okf_agentbus-0.11.3-py3-none-any.whl → …-py3-none-<plat>.whl
    name = wheel.name
    m = re.match(r"^(.+)-([^-]+)-py3-none-any\.whl$", name)
    if not m:
        print(f"unexpected wheel name: {name}", file=sys.stderr)
        return 1
    dist, ver = m.group(1), m.group(2)
    new_name = f"{dist}-{ver}-py3-none-{args.platform_tag}.whl"
    out_dir = args.out_dir or wheel.parent
    out_dir.mkdir(parents=True, exist_ok=True)
    dest = out_dir / new_name

    # Rewrite WHEEL metadata platform tag inside the archive
    tmp = out_dir / (new_name + ".tmp")
    with zipfile.ZipFile(wheel, "r") as zin, zipfile.ZipFile(
        tmp, "w", compression=zipfile.ZIP_DEFLATED
    ) as zout:
        for info in zin.infolist():
            data = zin.read(info.filename)
            if info.filename.endswith(".dist-info/WHEEL"):
                text = data.decode("utf-8")
                text = re.sub(
                    r"Tag: py3-none-any\n",
                    f"Tag: py3-none-{args.platform_tag}\n",
                    text,
                )
                # Root-Is-Purelib may stay true; binary is data file
                data = text.encode("utf-8")
            zout.writestr(info, data)
    shutil.move(str(tmp), str(dest))
    print(dest)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
