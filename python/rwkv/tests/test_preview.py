# Copyright: Ankitects Pty Ltd and contributors
# License: GNU AGPL, version 3 or later; http://www.gnu.org/licenses/agpl.html

import copy
from pathlib import Path

import pytest
from rwkv.preview import first_crossing, history_rows, preview


def snapshot():
    return {
        "version": 1,
        "as_of_millis": 1_700_864_000_000,
        "today": 100,
        "day": 20_000,
        "cards": [
            {
                "id": 1,
                "note_id": 11,
                "deck_id": 1,
                "preset_id": 1,
                "eligible": True,
                "is_new": False,
                "due": 110,
                "interval": 30,
            },
            {
                "id": 2,
                "note_id": 12,
                "deck_id": 2,
                "preset_id": 2,
                "eligible": False,
                "is_new": False,
                "due": 110,
                "interval": 30,
            },
        ],
        "history": [
            {
                "id": 1_700_000_000_000,
                "card_id": 1,
                "day": 19_990,
                "rating": 3,
                "duration_millis": 5000,
                "kind": 0,
                "reset": False,
            },
            {
                "id": 1_700_000_010_000,
                "card_id": 2,
                "day": 19_990,
                "rating": 2,
                "duration_millis": 4000,
                "kind": 0,
                "reset": False,
            },
            {
                "id": 1_700_086_400_000,
                "card_id": 1,
                "day": 19_991,
                "rating": 3,
                "duration_millis": 3000,
                "kind": 1,
                "reset": False,
            },
        ],
    }


def test_replay_keeps_other_decks_and_excludes_reset_history():
    data = snapshot()
    rows = history_rows(data)
    assert [row["card_id"] for row in rows] == [1, 2, 1]
    assert rows[0]["state"] == 0
    assert rows[0]["elapsed_seconds"] == -1
    assert rows[2]["elapsed_days"] == 1
    assert rows[2]["elapsed_seconds"] == 86400
    data["history"][0]["reset"] = True
    rows = history_rows(data)
    assert [row["card_id"] for row in rows] == [2, 1]
    assert rows[-1]["state"] == 0
    data["cards"][0]["is_new"] = True
    assert [row["card_id"] for row in history_rows(data)] == [2]


def test_deleted_card_history_has_missing_catalog_context():
    data = snapshot()
    data["cards"] = data["cards"][:1]
    row = history_rows(data)[1]
    assert row["card_id"] == 2
    assert row["note_id"] is None
    assert row["deck_id"] is None


def test_first_crossing_handles_non_monotonic_curves():
    assert first_crossing([0.95, 0.85, 0.97, 0.8], 0.9) == (2, False)
    assert first_crossing([0.95, 0.94], 0.9) == (2, True)
    for values in ([float("nan")], [float("inf")], [-0.1], [1.1], []):
        with pytest.raises(ValueError):
            first_crossing(values, 0.9)


def test_real_model_preview_is_deterministic_and_read_only():
    model = Path("out/rwkv/model.pth")
    if not model.exists():
        pytest.fail("run just rwkv-model before just test-rwkv")
    data = snapshot()
    original = copy.deepcopy(data)
    report = preview(data, model, 0.9, 30)
    assert report == preview(data, model, 0.9, 30)
    assert data == original
    assert report["reviews_replayed"] == 3
    assert report["eligible_cards"] == 1
    assert len(report["cards"]) == 1
    card = report["cards"][0]
    assert card["card_id"] == 1
    assert 1 <= card["proposed_interval"] <= 30
    assert card["proposed_due"] == max(100, 91 + card["proposed_interval"])
    # Compare the vectorized scheduling path with the original scalar predictor.
    import torch  # type: ignore[import-not-found]
    from rwkv.inference import RwkvInferenceProcess

    runtime = RwkvInferenceProcess(
        model_path=model, device=torch.device("cpu"), dtype=torch.float32
    )
    with torch.inference_mode():
        for row in history_rows(data):
            curve = runtime.process_row(row)
        scalar = [
            runtime.predict_func(curve, day * 86400).item() for day in range(1, 31)
        ]
    assert card["proposed_interval"] == first_crossing(scalar, 0.9)[0]


@pytest.mark.parametrize(
    "retention,maximum", [(0, 30), (1, 30), (float("nan"), 30), (0.9, 0), (0.9, 10001)]
)
def test_invalid_scheduling_options(retention, maximum):
    with pytest.raises(ValueError):
        preview(snapshot(), Path("unused"), retention, maximum)
