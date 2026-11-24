# Copyright: Ankitects Pty Ltd and contributors
# License: GNU AGPL, version 3 or later; http://www.gnu.org/licenses/agpl.html

"""Numerical parity with the original SM92 f32 model (tolerance 1e-5).

data/sm92-vectors.json.gz contains the first 64 rows of each reference sequence
from JSchoreels/anki rslib/src/rwkv/testdata/sm92-vectors.json.gz at
afa298cbe077b61fc42d214b2bf5898e300d3c98, under AGPL-3.0-or-later.
"""

import gzip
import json
from pathlib import Path

import torch
from rwkv.inference import RwkvInferenceProcess
from rwkv.inference.id_rng import id_rng


def test_id_features_match_reference_benchmark_stream():
    generator = id_rng()
    values = [generator.getrandbits(32) % 4 - 1.5 for _ in range(40)]
    assert values == [
        0.5,
        -1.5,
        1.5,
        -0.5,
        -1.5,
        -1.5,
        1.5,
        1.5,
        0.5,
        -0.5,
        0.5,
        -0.5,
        1.5,
        -0.5,
        1.5,
        -0.5,
        -0.5,
        -1.5,
        -1.5,
        0.5,
        0.5,
        -0.5,
        0.5,
        1.5,
        1.5,
        1.5,
        -0.5,
        -1.5,
        -0.5,
        1.5,
        -0.5,
        1.5,
        -0.5,
        -1.5,
        -0.5,
        1.5,
        1.5,
        1.5,
        -1.5,
        -0.5,
    ]


def test_sm92_matches_reference_model_vectors():
    torch.set_num_threads(1)
    runtime = RwkvInferenceProcess(
        model_path=Path("out/rwkv/model.bin"),
        device=torch.device("cpu"),
        dtype=torch.float32,
    )
    path = Path(__file__).parent / "data/sm92-vectors.json.gz"
    with gzip.open(path) as stream:
        sequences = json.load(stream)
    with torch.inference_mode():
        for sequence in sequences:
            card_states, deck_states, note_states, preset_states = {}, {}, {}, {}
            global_state = None
            for row in sequence["rows"]:
                card_id, deck_id, note_id, preset_id, _ = row["ids"]
                output = runtime.rnn.review(
                    torch.tensor([row["features"]]),
                    card_states.get(card_id),
                    note_states.get(note_id),
                    deck_states.get(deck_id),
                    preset_states.get(preset_id),
                    global_state,
                )
                power, weights, logits, card, note, deck, preset, shared = output
                ratings = torch.softmax(logits, -1).squeeze(0)
                elapsed = torch.tensor([row["elapsed"], *sequence["sweep"]]).view(-1, 1)
                curve = runtime.curve_probabilities((power, weights), elapsed)
                actual = torch.cat(((1 - ratings[0]).view(1), ratings, curve))
                torch.testing.assert_close(
                    actual, torch.tensor(row["fp32"]), atol=1e-5, rtol=0
                )
                if not row["skip"]:
                    card_states[card_id], note_states[note_id] = card, note
                    deck_states[deck_id], preset_states[preset_id] = deck, preset
                    global_state = shared
