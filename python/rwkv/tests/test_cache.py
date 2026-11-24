# Copyright: Ankitects Pty Ltd and contributors
# License: GNU AGPL, version 3 or later; http://www.gnu.org/licenses/agpl.html

from pathlib import Path

import pytest
import torch  # type: ignore[import-not-found]
from rwkv import cache
from rwkv.preview import preview
from rwkv.tests.test_preview import snapshot

MODEL = Path("out/rwkv/model.bin")


def scheduling(report):
    return {
        key: value
        for key, value in report.items()
        if key not in ("reviews_replayed", "reviews_cached")
    }


def test_cached_and_appended_replay_match_cold_with_new_ids(tmp_path):
    path = tmp_path / "state.pt"
    data = snapshot()
    cold = preview(data, MODEL, 0.9, 30, path)
    warm = preview(data, MODEL, 0.9, 30, path)
    assert scheduling(warm) == scheduling(cold)
    assert (cold["reviews_replayed"], warm["reviews_replayed"]) == (3, 0)
    assert warm["reviews_cached"] == 3
    # Recreate the runtime on every preview, as a restarted pod would.
    card = {**data["cards"][0], "id": 3, "note_id": 13, "deck_id": 3, "preset_id": 3}
    data["cards"].append(card)
    for card_id in (3, 1):
        data["history"].append(
            {
                **data["history"][-1],
                "id": data["history"][-1]["id"] + 86400000,
                "day": data["history"][-1]["day"] + 1,
                "card_id": card_id,
            }
        )
    appended = preview(data, MODEL, 0.9, 30, path)
    assert appended["reviews_cached"] == 3
    assert appended["reviews_replayed"] == 2
    assert scheduling(appended) == scheduling(preview(data, MODEL, 0.9, 30))
    # Retention, date, cap and target selection change scheduling, not memory.
    data["cards"][1]["eligible"] = True
    data["today"] += 1
    data["day"] += 1
    changed = preview(data, MODEL, 0.85, 60, path)
    assert changed["reviews_replayed"] == 0
    assert changed["reviews_cached"] == 5
    assert scheduling(changed) == scheduling(preview(data, MODEL, 0.85, 60))


@pytest.mark.parametrize(
    "mutation",
    [
        "rating",
        "duration_millis",
        "day",
        "reset",
        "delete_review",
        "delete_card",
        "note_id",
        "deck_id",
        "preset_id",
        "is_new",
    ],
)
def test_changed_history_or_context_rebuilds_shared_state(tmp_path, mutation):
    path = tmp_path / "state.pt"
    data = snapshot()
    preview(data, MODEL, 0.9, 30, path)
    if mutation == "delete_review":
        data["history"].pop(0)
    elif mutation == "delete_card":
        data["cards"].pop(0)
    elif mutation in ("note_id", "deck_id", "preset_id", "is_new"):
        data["cards"][0][mutation] = True if mutation == "is_new" else 99
    else:
        data["history"][0][mutation] = True if mutation == "reset" else 1
    rebuilt = preview(data, MODEL, 0.9, 30, path)
    assert rebuilt["reviews_cached"] == 0
    assert scheduling(rebuilt) == scheduling(preview(data, MODEL, 0.9, 30))


@pytest.mark.parametrize("damage", ["truncated", "engine", "version"])
def test_unusable_checkpoint_is_rebuilt_atomically(tmp_path, damage):
    path = tmp_path / "state.pt"
    data = snapshot()
    cold = preview(data, MODEL, 0.9, 30, path)
    if damage == "truncated":
        path.write_bytes(path.read_bytes()[:50])
    else:
        state = torch.load(path, weights_only=True)
        state[damage] = "obsolete"
        torch.save(state, path)
    rebuilt = preview(data, MODEL, 0.9, 30, path)
    assert rebuilt["reviews_cached"] == 0
    assert scheduling(rebuilt) == scheduling(cold)
    assert preview(data, MODEL, 0.9, 30, path)["reviews_replayed"] == 0
    assert list(tmp_path.iterdir()) == [path]


def test_interrupted_checkpoint_write_keeps_previous_state(tmp_path, monkeypatch):
    path = tmp_path / "state.pt"
    data = snapshot()
    preview(data, MODEL, 0.9, 30, path)
    previous = path.read_bytes()
    data["history"][0]["rating"] = 1

    def interrupted(*args, **kwargs):
        raise OSError("interrupted")

    with monkeypatch.context() as patch:
        patch.setattr(cache.torch, "save", interrupted)
        with pytest.raises(OSError, match="interrupted"):
            preview(data, MODEL, 0.9, 30, path)
    assert path.read_bytes() == previous
    assert list(tmp_path.iterdir()) == [path]


def test_empty_history_replaces_invalid_cache(tmp_path):
    data = snapshot()
    data["history"] = []
    path = tmp_path / "state.pt"
    path.write_bytes(b"invalid")
    report = preview(data, MODEL, 0.9, 30, path)
    assert report["reviews_total"] == 0
    assert report["cards"] == []
    assert report["skipped_without_history"] == 1
    assert torch.load(path, weights_only=True)["review_count"] == 0
    assert scheduling(preview(data, MODEL, 0.9, 30, path)) == scheduling(report)
