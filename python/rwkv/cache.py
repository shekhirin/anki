# Copyright: Ankitects Pty Ltd and contributors
# License: GNU AGPL, version 3 or later; http://www.gnu.org/licenses/agpl.html

"""Persistent replay checkpoint with an exact history-prefix identity.

Like the reference scheduler, resume only when the cached reviews are an
unchanged prefix. Changes to earlier history or catalog context rebuild all
shared card/note/deck/preset/user state. Scheduling targets are not cache keys.
"""

from __future__ import annotations

import hashlib
import json
import os
import pickle
import sys
import tempfile
from pathlib import Path

import torch  # type: ignore[import-not-found]

CACHE_VERSION = 1


def engine_identity(model_sha256: str) -> str:
    digest = hashlib.sha256(model_sha256.encode() + torch.__version__.encode())
    root = Path(__file__).parent
    for path in sorted([*root.glob("*.py"), *root.joinpath("inference").glob("*.py")]):
        digest.update(path.name.encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()


def history_identity(rows) -> str:
    return hashlib.sha256(
        json.dumps(rows, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def restore(path, runtime, rows, identity):
    """A missing, obsolete or corrupt checkpoint is a cold-cache miss."""
    if path is None or not path.exists():
        return None, {}
    try:
        state = torch.load(path, map_location="cpu", weights_only=True)
        if not isinstance(state, dict) or not isinstance(state.get("runtime"), dict):
            return None, {}
        count = state["review_count"]
        if (
            state["version"] != CACHE_VERSION
            or state["engine"] != identity
            or type(count) is not int
            or not 0 <= count <= len(rows)
            or state["history"] != history_identity(rows[:count])
            or state["runtime"]["i"] != count
            or not isinstance(state["latest"], dict)
        ):
            return None, {}
        runtime.restore_cache_state(state["runtime"])
        return count, state["latest"]
    except (
        OSError,
        ValueError,
        TypeError,
        KeyError,
        RuntimeError,
        EOFError,
        pickle.UnpicklingError,
    ):
        print("Ignoring invalid RWKV replay cache; rebuilding", file=sys.stderr)
        return None, {}


def save(path, runtime, rows, latest, identity):
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    state = {
        "version": CACHE_VERSION,
        "engine": identity,
        "review_count": len(rows),
        "history": history_identity(rows),
        "runtime": runtime.cache_state(),
        "latest": latest,
    }
    # A killed worker never exposes a partial checkpoint to the next job.
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as stream:
        temporary = Path(stream.name)
        try:
            torch.save(state, stream)
            stream.flush()
            os.fsync(stream.fileno())
            stream.close()
            temporary.replace(path)
        finally:
            temporary.unlink(missing_ok=True)
