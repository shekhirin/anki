# Copyright: Ankitects Pty Ltd and contributors
# License: GNU AGPL, version 3 or later; http://www.gnu.org/licenses/agpl.html

"""Start or inspect a headless RWKV job, suitable for an external cron timer."""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import tempfile
import time
import urllib.request
from pathlib import Path
from typing import Any


def request(
    url: str, path: str, payload: dict[str, Any] | None = None
) -> dict[str, Any]:
    headers = {"Content-Type": "application/json"}
    if key := os.environ.get("ANKI_API_KEY"):
        headers["Authorization"] = f"Bearer {key}"
    http_request = urllib.request.Request(
        url.rstrip("/") + path,
        data=json.dumps(payload, allow_nan=False).encode()
        if payload is not None
        else None,
        headers=headers,
        method="POST" if payload is not None else "GET",
    )
    with urllib.request.urlopen(http_request, timeout=30) as response:
        return json.load(response)


def wait(url: str, job_id: str, timeout: float) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while True:
        job = request(url, "/v1/rwkv/jobs/" + job_id)
        if job["state"] in ("succeeded", "failed"):
            return job
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"RWKV job {job_id} is still running; inspect with --job-id {job_id}. "
                "A client timeout does not cancel the server job."
            )
        time.sleep(1)


def write_result(path: Path, result: dict[str, Any]) -> None:
    # A cron reader never observes a partially written report.
    with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, delete=False) as output:
        temporary = Path(output.name)
        try:
            json.dump(result, output, indent=2, allow_nan=False)
            output.write("\n")
            output.close()
            temporary.replace(path)
        finally:
            temporary.unlink(missing_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:8765")
    parser.add_argument("--query", default="")
    parser.add_argument("--retention", type=float, default=0.9)
    parser.add_argument("--maximum-interval", type=int, default=3650)
    parser.add_argument(
        "--apply", action="store_true", help="commit proposed scheduling changes"
    )
    parser.add_argument(
        "--no-sync", action="store_true", help="explicitly run against local data only"
    )
    parser.add_argument(
        "--job-id", help="inspect an accepted job without starting another"
    )
    parser.add_argument("--wait-timeout", type=float, default=7200)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if not math.isfinite(args.wait_timeout) or args.wait_timeout <= 0:
        parser.error("--wait-timeout must be positive")
    if not 0 < args.retention < 1 or not 1 <= args.maximum_interval <= 10000:
        parser.error(
            "retention must be between 0 and 1; maximum interval must be 1–10000 days"
        )
    job_id = args.job_id
    if job_id is None:
        accepted = request(
            args.url,
            "/v1/rwkv/run",
            {
                "query": args.query,
                "retention": args.retention,
                "maximum_interval": args.maximum_interval,
                "apply": args.apply,
                "sync": not args.no_sync,
            },
        )
        job_id = accepted["job_id"]
    print(f"RWKV job: {job_id}", file=sys.stderr, flush=True)
    result = wait(args.url, job_id, args.wait_timeout)
    if args.output:
        write_result(args.output, result)
    else:
        print(json.dumps(result, indent=2, allow_nan=False))
    if result["state"] != "succeeded":
        print(
            f"RWKV job failed after applying {result['applied_cards']} cards: {result['error']}",
            file=sys.stderr,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
