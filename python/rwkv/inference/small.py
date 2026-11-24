# Copyright: Ankitects Pty Ltd and contributors
# License: GNU AGPL, version 3 or later; http://www.gnu.org/licenses/agpl.html

"""SM92, the interval model of the reference fork's Small model family.

Ported from JSchoreels/anki afa298cbe077b61fc42d214b2bf5898e300d3c98,
rslib/src/rwkv/{architecture,engine}.rs. Uses the existing PyTorch RWKV
time/channel mixers with the reference interleaved layer plan and power head.
"""

from __future__ import annotations

import math
import struct
from pathlib import Path

import torch  # type: ignore[import-not-found]
from torch import nn

from .rwkv_model import LoraSimple, RWKV7Config
from .rwkv_rnn_model import RWKV7RNN

LAYER_COUNTS = (2, 4, 1, 3, 3)
LAYER_STEPS = (
    (0, 0),
    (1, 0),
    (2, 0),
    (3, 0),
    (4, 0),
    (0, 1),
    (1, 1),
    (3, 1),
    (4, 1),
    (1, 2),
    (3, 2),
    (4, 2),
    (1, 3),
)
CHANNEL_MIXERS = {(0, 0), (1, 0), (1, 3), (2, 0)}


def load_weights(path: Path) -> dict[str, torch.Tensor]:
    """Read the reference's self-describing little-endian float32 export."""
    data = bytearray(path.read_bytes())
    if data[:13] != b"ARWKVWEIGHTS1":
        raise ValueError("invalid SM92 weights")
    offset = 13

    def unpack(fmt):
        nonlocal offset
        value = struct.unpack_from(fmt, data, offset)
        offset += struct.calcsize(fmt)
        return value

    tensors = {}
    for _ in range(unpack("<I")[0]):
        length = unpack("<H")[0]
        name = data[offset : offset + length].decode()
        offset += length
        shape = unpack("<" + "I" * unpack("<B")[0])
        count = math.prod(shape)
        tensors[name] = (
            torch.frombuffer(data, dtype=torch.float32, count=count, offset=offset)
            .clone()
            .reshape(shape)
        )
        offset += count * 4
    if offset != len(data):
        raise ValueError("invalid SM92 weight length")
    return tensors


class SmallSrsRWKVRnn(nn.Module):
    def __init__(self) -> None:
        super().__init__()
        self.features2card = nn.Sequential(
            nn.Linear(92, 128),
            nn.SiLU(),
            nn.LayerNorm(128),
            nn.Linear(128, 64),
            nn.SiLU(),
        )
        self.rwkv_modules = nn.ModuleList()
        for module, layers in enumerate(LAYER_COUNTS):
            config = RWKV7Config(
                d_model=64,
                n_heads=4,
                n_layers=layers,
                channel_mixer_factor=1,
                layer_offset=0,
                total_layers=max(2, layers),
                decay_lora=4,
                a_lora=4,
                v0_mix_amt_lora=2,
                gate_lora=4,
                dropout=0,
                dropout_layer=0,
            )
            stream = RWKV7RNN(config)
            for layer, block in enumerate(stream.blocks):
                if (module, layer) not in CHANNEL_MIXERS:
                    block.channel_mixer = None
                if layer == 0:
                    # The exported layer-zero value adapter is a 1x1 placeholder;
                    # the time mixer takes its value directly from W_v here.
                    block.time_mixer.v_lora_simple = LoraSimple("v", 1, 1, 0)
            self.rwkv_modules.append(stream)
        self.prehead_norm = nn.LayerNorm(64)
        self.head_w = nn.Sequential(
            nn.Linear(64, 64),
            nn.ReLU(),
            nn.LayerNorm(64),
            nn.Dropout(0),
            nn.Linear(64, 128),
        )
        self.head_p = nn.Sequential(nn.Linear(64, 128), nn.ReLU())
        self.p_linear = nn.Linear(128, 4)
        for head in ("w", "s", "d"):
            self.register_parameter(
                f"gru_{head}_weight", nn.Parameter(torch.empty(3, 128))
            )
            self.register_parameter(f"gru_{head}_bias", nn.Parameter(torch.empty(3)))
        # Unused placeholders retained so loading verifies every exported tensor.
        self.head_ahead_logits = nn.Sequential(nn.Linear(1, 1))
        self.ahead_linear = nn.Linear(1, 1)
        self.w_linear = nn.Linear(1, 1)

    def selective_cast(self, dtype):
        if dtype != torch.float32:
            raise ValueError("SM92 headless inference requires float32")
        return self

    def review(self, features, card, note, deck, preset, global_state):
        features = features.clone()
        features[..., 13:16] = 0  # Leak-free model: no deleted-card flags.
        features[..., 22] = 0  # The small model never uses scaled_state.
        x = self.features2card(features)
        states = (card, deck, note, preset, global_state)
        next_states: list[dict[int, object]] = [{} for _ in LAYER_COUNTS]
        v0 = [None] * 5
        for module, layer in LAYER_STEPS:
            block = self.rwkv_modules[module].blocks[layer]
            previous = states[module].get(layer) if states[module] is not None else None
            time, channel = previous if previous is not None else (None, None)
            x, v0[module], time = block.time_mixer(x, v0[module], time)
            if block.channel_mixer is not None:
                x, channel = block.channel_mixer(x, channel)
            next_states[module][layer] = (time, channel)
        x = self.prehead_norm(x)
        head = self.head_w(x)
        logits = nn.functional.linear(head, self.gru_w_weight, self.gru_w_bias)
        scales = nn.functional.linear(head, self.gru_s_weight, self.gru_s_bias)
        decays = nn.functional.linear(head, self.gru_d_weight, self.gru_d_bias)
        weights = torch.softmax(logits, dim=-1)
        # Match the existing curve interface: ahead carries the power parameters.
        power = torch.cat((scales, decays), dim=-1)
        card, deck, note, preset, global_state = next_states
        return (
            power,
            weights,
            self.p_linear(self.head_p(x)),
            card,
            note,
            deck,
            preset,
            global_state,
        )

    def forgetting_curve(self, weights, elapsed_seconds, power=None):
        if power is None:
            raise ValueError("SM92 requires its power-curve parameters")
        scales, decays = power.chunk(2, dim=-1)
        scales = 1e-7 + scales.clamp(-25, 25).exp()
        decays = decays.clamp(-25, 25).exp()
        elapsed_seconds = elapsed_seconds.clamp(min=1)
        raw = (weights * (-decays * torch.log1p(elapsed_seconds / scales)).exp()).sum(
            -1
        )
        return 1e-5 + (1 - 2e-5) * raw

    def interp(self, power, elapsed_seconds):
        # The reference power head has no learned ahead residual.
        return torch.full_like(elapsed_seconds.squeeze(-1), 1e-5)
