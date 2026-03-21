import subprocess
import sys
from pathlib import Path


VENV_PYTHON = "./venv/Scripts/python.exe"
GOOGLEAPIS = "tron/googleapis"
PROTOS = "tron/protos"
OUT = "tron/generated"

PROTO_DIRS = [
    "tron/protos/api",
    "tron/protos/core",
    "tron/protos/core/contract",
    "tron/protos/core/tron",
    "tron/googleapis/google/api",
]


def generate_stubs():
    out_path = Path(OUT)
    out_path.mkdir(parents=True, exist_ok=True)
    print(f"Output directory: {out_path.resolve()}")

    for proto_dir in PROTO_DIRS:
        dir_path = Path(proto_dir)

        if not dir_path.exists():
            print(f"[SKIP] Directory not found: {proto_dir}")
            continue

        proto_files = list(dir_path.glob("*.proto"))

        if not proto_files:
            print(f"[SKIP] No .proto files found in: {proto_dir}")
            continue

        print(f"\n[GEN] Processing {len(proto_files)} file(s) in {proto_dir} ...")

        cmd = [
            VENV_PYTHON, "-m", "grpc_tools.protoc",
            f"-I{GOOGLEAPIS}",
            f"-I{PROTOS}",
            f"--python_out={OUT}",
            f"--grpc_python_out={OUT}",
            *[str(f) for f in proto_files],
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            print(f"[ERROR] protoc failed for {proto_dir}")
            print(result.stderr)
            sys.exit(result.returncode)
        else:
            for f in proto_files:
                print(f"  OK  {f.name}")

    print("\nDone! Stubs generated in:", str(out_path.resolve()))


if __name__ == "__main__":
    generate_stubs()