"""The trajectory embedding model — a t2vec-style denoising seq2seq autoencoder.

Reference: Li, Zhao, Cong, Jensen, Wei, "Deep Representation Learning for
Trajectory Similarity Computation" (t2vec), ICDE 2018.

Why a *denoising* autoencoder: our ANPR data has missed detections, occlusion
and OCR errors. t2vec's key idea is to train the encoder to reconstruct the
*clean* trajectory from a *corrupted* (down-sampled / noised) copy, so the
learned embedding is robust to exactly the gaps that break FP-Growth. We
realize the corruption by randomly dropping a fraction of camera tokens.

The model is a vocabulary over CAMERA ids (≈977 + special tokens), not GPS
cells — checkpoint data is already discrete, so no grid quantization is needed.
Time is deliberately NOT modelled here (kept simple/robust, faithful to t2vec);
the temporal "are they actually travelling together" check happens in Stage 3
(cluster_confirm.py). A time-aware variant (START) is the documented stretch.

Pure PyTorch, no GPU assumptions — `device` is chosen by the caller.
"""
from __future__ import annotations

import torch
import torch.nn as nn

PAD, BOS, EOS, UNK = 0, 1, 2, 3
N_SPECIAL = 4


class Vocab:
    """Maps camera id -> token index (offset past the 4 special tokens)."""

    def __init__(self, cameras):
        cams = sorted(int(c) for c in cameras)
        self.cam2tok = {c: i + N_SPECIAL for i, c in enumerate(cams)}
        self.tok2cam = {i + N_SPECIAL: c for i, c in enumerate(cams)}
        self.size = len(cams) + N_SPECIAL

    def encode(self, cameras):
        return [self.cam2tok.get(int(c), UNK) for c in cameras]

    def state_dict(self):
        return {"cameras": list(self.tok2cam.values())}

    @classmethod
    def from_state(cls, state):
        return cls(state["cameras"])


class Seq2SeqAE(nn.Module):
    """GRU encoder -> embedding -> GRU decoder reconstructing the clean route."""

    def __init__(self, vocab_size: int, emb_dim: int = 128,
                 hidden: int = 256, layers: int = 2, dropout: float = 0.1):
        super().__init__()
        self.tok_emb = nn.Embedding(vocab_size, emb_dim, padding_idx=PAD)
        self.encoder = nn.GRU(emb_dim, hidden, layers,
                              batch_first=True, dropout=dropout if layers > 1 else 0.0)
        self.decoder = nn.GRU(emb_dim, hidden, layers,
                              batch_first=True, dropout=dropout if layers > 1 else 0.0)
        self.out = nn.Linear(hidden, vocab_size)
        self.emb_dim = emb_dim
        self.hidden = hidden
        self.layers = layers

    def encode(self, src, src_len):
        """Return the trajectory embedding: the last layer's final hidden state."""
        x = self.tok_emb(src)
        packed = nn.utils.rnn.pack_padded_sequence(
            x, src_len.cpu(), batch_first=True, enforce_sorted=False)
        _, h = self.encoder(packed)          # h: (layers, B, hidden)
        return h                              # full stack, used to seed decoder

    def embedding(self, src, src_len):
        """The vector we cluster on — top encoder layer's final hidden state."""
        h = self.encode(src, src_len)
        return h[-1]                          # (B, hidden)

    def forward(self, src, src_len, tgt_in):
        h = self.encode(src, src_len)
        y = self.tok_emb(tgt_in)
        dec, _ = self.decoder(y, h)
        return self.out(dec)                  # (B, T, vocab) logits


def corrupt(tokens, drop_prob, rng):
    """Down-sample a token list to mimic missed detections (t2vec noising)."""
    if drop_prob <= 0 or len(tokens) <= 2:
        return list(tokens)
    kept = [t for t in tokens if rng.random() > drop_prob]
    return kept if len(kept) >= 2 else list(tokens)
