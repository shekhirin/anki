# Copyright: Ankitects Pty Ltd and contributors
# License: GNU AGPL, version 3 or later; http://www.gnu.org/licenses/agpl.html

"""Read-only RWKV-Curve scheduling preview for the headless API server."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sys
import tempfile
import urllib.request
from pathlib import Path
from typing import Any

SOURCE_REVISION = "afa298cbe077b61fc42d214b2bf5898e300d3c98"
MODEL_SHA256 = "67ee8e9b0d0169733242c40646e0d2e5637845a25775762002ffc4e6396f9590"
MODEL_URL = (
    f"https://raw.githubusercontent.com/JSchoreels/anki/{SOURCE_REVISION}/"
    "qt/aqt/rwkv_inference/RWKV_trained_on_5000_10000.pth"
)


def download_model(destination: Path) -> None:
    if destination.exists():
        verify_model(destination)
        return
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=destination.parent, delete=False) as output:
        temporary = Path(output.name)
        try:
            with urllib.request.urlopen(MODEL_URL, timeout=120) as response:
                while chunk := response.read(1024 * 1024):
                    output.write(chunk)
            output.close()
            verify_model(temporary)
            temporary.replace(destination)
        finally:
            temporary.unlink(missing_ok=True)


def verify_model(path: Path) -> None:
    if hashlib.sha256(path.read_bytes()).hexdigest() != MODEL_SHA256:
        raise ValueError("model checksum mismatch; use the model from just rwkv-model")


def history_rows(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    """Prepare one chronological collection-wide replay, including deleted cards.

    A reset discards that card's preceding history. Currently-new cards have no
    active memory history. Catalog identities are current/home identities, since
    Anki's review log does not preserve historical deck or preset membership.
    """
    cards = {card["id"]: card for card in snapshot["cards"]}
    history = sorted(snapshot["history"], key=lambda review: review["id"])
    cutoffs = {review["card_id"]: review["id"] for review in history if review["reset"]}
    previous: dict[int, dict[str, Any]] = {}
    rows = []
    for review in history:
        card_id = review["card_id"]
        card = cards.get(card_id, {})
        if (
            review["reset"]
            or review["id"] <= cutoffs.get(card_id, -1)
            or card.get("is_new", False)
            or review["kind"] not in (0, 1, 2, 3)
            or review["rating"] not in (1, 2, 3, 4)
        ):
            continue
        prior = previous.get(card_id)
        rows.append(
            {
                "review_id": review["id"],
                "card_id": card_id,
                "note_id": card.get("note_id"),
                "deck_id": card.get("deck_id"),
                "preset_id": card.get("preset_id"),
                "day_offset": review["day"],
                "elapsed_days": review["day"] - prior["day"] if prior else -1,
                "elapsed_seconds": (review["id"] - prior["id"]) // 1000
                if prior
                else -1,
                "rating": review["rating"],
                "duration": review["duration_millis"],
                "state": review["kind"] + 1 if prior else 0,
            }
        )
        previous[card_id] = review
    return rows


def first_crossing(probabilities: list[float], retention: float) -> tuple[int, bool]:
    """Return the first daily crossing, without assuming a monotonic curve."""
    if not probabilities or any(
        not math.isfinite(value) or not 0 <= value <= 1 for value in probabilities
    ):
        raise ValueError("model returned an invalid probability curve")
    for index, probability in enumerate(probabilities):
        if probability <= retention:
            return index + 1, False
    return len(probabilities), True


def preview(
    snapshot: dict[str, Any], model: Path, retention: float, maximum_interval: int
) -> dict[str, Any]:
    if snapshot.get("version") != 1:
        raise ValueError("unsupported snapshot version")
    if not math.isfinite(retention) or not 0 < retention < 1:
        raise ValueError("retention must be between 0 and 1, exclusive")
    # The reference model's interpolation grid ends at exp(21) seconds.
    if not 1 <= maximum_interval <= 10_000:
        raise ValueError("maximum interval must be between 1 and 10000 days")
    verify_model(model)
    import torch  # type: ignore[import-not-found]

    from .inference import RwkvInferenceProcess

    torch.set_num_threads(1)
    runtime = RwkvInferenceProcess(
        model_path=model, device=torch.device("cpu"), dtype=torch.float32
    )
    rows = history_rows(snapshot)
    eligible = {card["id"]: card for card in snapshot["cards"] if card["eligible"]}
    latest: dict[int, tuple[dict[str, Any], Any]] = {}
    with torch.inference_mode():
        for row in rows:
            curve = runtime.process_row(row)
            if row["card_id"] in eligible:
                latest[row["card_id"]] = (row, curve)
        elapsed = torch.arange(1, maximum_interval + 1, dtype=torch.float32).view(-1, 1)
        elapsed *= 86_400
        changes = []
        old_due_count = new_due_count = 0
        for card_id, card in sorted(eligible.items()):
            if card_id not in latest:
                continue
            row, (ahead, weights) = latest[card_id]
            curve_probs = runtime.rnn.forgetting_curve(weights, elapsed)
            residual = runtime.rnn.interp(ahead.expand(maximum_interval, -1), elapsed)
            probabilities = torch.sigmoid(torch.logit(curve_probs) + residual).tolist()
            interval, capped = first_crossing(probabilities, retention)
            last_review_day = snapshot["today"] - (snapshot["day"] - row["day_offset"])
            # An overdue prediction stays due today; it never gains a fresh
            # full interval from the time the nightly job happens to run.
            due = max(snapshot["today"], last_review_day + interval)
            old_due_count += card["due"] <= snapshot["today"] + 1
            new_due_count += due <= snapshot["today"] + 1
            changes.append(
                {
                    "card_id": card_id,
                    "last_review_id": row["review_id"],
                    "old_due": card["due"],
                    "proposed_due": due,
                    "old_interval": card["interval"],
                    "proposed_interval": interval,
                    "maximum_interval_reached": capped,
                }
            )
    return {
        "dry_run": True,
        "model_sha256": MODEL_SHA256,
        "source_revision": SOURCE_REVISION,
        "snapshot_sha256": hashlib.sha256(
            json.dumps(snapshot, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
        "as_of_millis": snapshot["as_of_millis"],
        "today": snapshot["today"],
        "retention": retention,
        "maximum_interval": maximum_interval,
        "reviews_replayed": len(rows),
        "eligible_cards": len(eligible),
        "skipped_without_history": len(eligible) - len(latest),
        "changed_cards": sum(
            change["old_due"] != change["proposed_due"]
            or change["old_interval"] != change["proposed_interval"]
            for change in changes
        ),
        "eligible_due_by_tomorrow_before": old_due_count,
        "eligible_due_by_tomorrow_after": new_due_count,
        "cards": changes,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--download-model", type=Path)
    parser.add_argument("--model", type=Path)
    parser.add_argument("--url", default="http://127.0.0.1:8765")
    parser.add_argument(
        "--snapshot", type=Path, help="replay a previously saved snapshot"
    )
    parser.add_argument("--save-snapshot", type=Path)
    parser.add_argument(
        "--query", default="", help="Anki search selecting scheduling targets"
    )
    parser.add_argument("--retention", type=float, default=0.9)
    parser.add_argument("--maximum-interval", type=int, default=3650)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.download_model:
        download_model(args.download_model)
        print(f"Verified model saved to {args.download_model}")
        return
    if args.model is None:
        parser.error("--model is required")
    if args.snapshot:
        snapshot = json.loads(args.snapshot.read_text())
    else:
        headers = {"Content-Type": "application/json"}
        if key := os.environ.get("ANKI_API_KEY"):
            headers["Authorization"] = f"Bearer {key}"
        request = urllib.request.Request(
            args.url.rstrip("/") + "/v1/rwkv/snapshot",
            data=json.dumps({"query": args.query}).encode(),
            headers=headers,
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=300) as response:
            snapshot = json.load(response)
    if args.save_snapshot:
        args.save_snapshot.write_text(json.dumps(snapshot))
    report = preview(snapshot, args.model, args.retention, args.maximum_interval)
    rendered = json.dumps(report, indent=2, allow_nan=False) + "\n"
    if args.output:
        args.output.write_text(rendered)
    else:
        sys.stdout.write(rendered)


if __name__ == "__main__":
    main()
