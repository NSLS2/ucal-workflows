from __future__ import annotations

import prefect
import subprocess
import sys
import tiled

def test(stuff=""):
    if stuff:
        print(f"test: {stuff}")  # noqa: T201
    else:
        print("test: EMPTY")  # noqa: T201


def info():
    print(f"Prefect info: {prefect.__version_info__}")
    print(f"Tiled info: {tiled.__version__}")
    output = subprocess.call("pixi --version")
    print(f"Pixi info: {output}")


if __name__ == "__main__":
    info()
    if len(sys.argv) > 1:
        test(sys.argv[1])
    else:
        test()
