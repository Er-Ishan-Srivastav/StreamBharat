#!/usr/bin/env python3
"""Check Docker readiness, Compose configuration, disk headroom and local ports."""
from __future__ import annotations

import shutil
import socket
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PORTS = {
    3000: "Metabase",
    4040: "Spark application UI",
    5432: "PostgreSQL",
    8080: "Kafka UI",
    8081: "Airflow (optional)",
    8082: "Spark master host mapping",
    8083: "Spark worker host mapping",
    8888: "JupyterLab",
    9092: "Kafka external listener",
}


def run(command: list[str]) -> tuple[int, str]:
    try:
        result = subprocess.run(
            command,
            cwd=ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=30,
            check=False,
        )
        return result.returncode, result.stdout.strip()
    except (OSError, subprocess.TimeoutExpired) as exc:
        return 127, str(exc)


def port_is_listening(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.25)
        return sock.connect_ex(("127.0.0.1", port)) == 0


def main() -> int:
    failures = 0
    print("StreamBharat local environment preflight")
    print("=" * 42)

    if not shutil.which("docker"):
        print("FAIL  Docker CLI is not on PATH")
        failures += 1
    else:
        print("PASS  Docker CLI found")

    code, output = run(["docker", "info", "--format", "{{.MemTotal}}"])
    if code != 0:
        print(f"FAIL  Docker engine is not responding: {output}")
        failures += 1
    else:
        try:
            memory_gib = int(output) / (1024 ** 3)
            status = "PASS" if memory_gib >= 8 else "WARN"
            print(f"{status}  Docker engine memory: {memory_gib:.1f} GiB")
            if memory_gib < 8:
                print("      Allocate at least 8 GiB to Docker; keep Airflow off for the demo.")
        except ValueError:
            print(f"WARN  Could not parse Docker memory output: {output}")

    code, output = run(["docker", "compose", "version"])
    if code == 0:
        print(f"PASS  Docker Compose available ({output})")
    else:
        print(f"FAIL  Docker Compose is unavailable: {output}")
        failures += 1

    for compose_file in ("docker-compose.kafka.yml", "docker-compose.airflow.yml"):
        code, output = run(["docker", "compose", "-f", compose_file, "config", "--quiet"])
        if code == 0:
            print(f"PASS  {compose_file} configuration")
        else:
            print(f"FAIL  {compose_file}: {output}")
            failures += 1

    free_gib = shutil.disk_usage(ROOT).free / (1024 ** 3)
    status = "PASS" if free_gib >= 15 else "WARN"
    print(f"{status}  Free space on repository filesystem: {free_gib:.1f} GiB")
    if free_gib < 15:
        print("      Docker image builds may require additional free disk space.")

    occupied = [(port, label) for port, label in PORTS.items() if port_is_listening(port)]
    if occupied:
        print("WARN  These local ports already have listeners:")
        for port, label in occupied:
            print(f"      {port}: {label} (check for an existing service or port conflict)")
    else:
        print("PASS  Expected service ports appear available")

    env_file = ROOT / ".env"
    if env_file.is_file():
        print("PASS  .env exists")
    else:
        print("INFO  .env will be created by scripts/start_local_stack.sh")

    print()
    if failures:
        print(f"Preflight failed with {failures} blocking issue(s).")
        return 1
    print("Preflight passed. Port and resource warnings should be reviewed before startup.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
