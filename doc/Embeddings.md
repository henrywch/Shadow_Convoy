# Variable-Length Sequences → Fixed-Length Embeddings

> Walk-through of how the trajectory autoencoder in `src/embed/` maps each plate's variable-length camera sequence (10 to 3,202 cameras in our Stage 1 output) into a fixed 256-dim embedding vector. Companion to `doc/ML_Approaches.md §3` (the method-level reasoning) and `src/embed/README.md` (the pipeline / launcher).

---

## TL;DR

The trick is that **a GRU's hidden state has a fixed shape independent of the input length**. The sequence is consumed token by token, but each step *updates* a fixed-size hidden vector rather than producing a per-token output. We embed the *final* hidden state.

```
   pad to (B, T_max) ── tok_emb ──> (B, T_max, 128)
                                      │
                                      ▼ pack_padded_sequence + GRU (length-aware)
                                  (layers=2, B, 256)
                                      │
                                      ▼ h[-1]
                                    (B, 256)        ← embedding
```

---

## 1. Concrete example

Take two plates from this project's Stage 1 output:

- **Plate A**: `cameras = [8, 6, 366, …]` length **134**
- **Plate B**: `cameras = [49, 141, 141, …]` length **3,202**

These two end up in the same training batch (random shuffling). Both will become 256-dim vectors at the end. Here's the path.

---

## 2. Step-by-step

### Step 1 — pad to a rectangular tensor (in `collate`)

`train_encode.py:59-75` pads the batch to the longest sequence in it:

```
src         shape (B, T_max)        int64        T_max = max length in batch
src_len     shape (B,)              int64        the real length of each row
```

Plate A's row becomes `[8, 6, 366, …, PAD, PAD, …]` padded out to the batch's longest (probably the bus trajectory). Plate B's row stays the full 3,202 tokens. **They're the same width on the GPU tensor but different real lengths**, and `src_len` carries that truth.

### Step 2 — token embedding (`model.py:67`)

```python
x = self.tok_emb(src)                # (B, T_max) → (B, T_max, emb_dim=128)
```

Look up each camera ID in a learned 128-dim embedding table. `PAD` (token id 0) has its own row in this table; we keep it benign with `padding_idx=PAD` so gradient doesn't push it around.

### Step 3 — packed-sequence run through the GRU (`model.py:68-70`)

```python
packed = nn.utils.rnn.pack_padded_sequence(x, src_len, …)
_, h   = self.encoder(packed)        # h: (layers=2, B, hidden=256)
```

This is the key trick. `pack_padded_sequence` tells cuDNN exactly how many real tokens each row has, so the GRU only advances on real tokens and stops updating that row when it reaches `src_len`. The GRU still consumes plate A in 134 steps and plate B in 3,202 steps internally, but in both cases the returned `h` has the **same fixed shape** `(layers, B, hidden)`:

```
  Plate A:  h_0 → h_1 → h_2 → … → h_134                  ← final state h_134
  Plate B:  h_0 → h_1 → h_2 → …                → h_3202  ← final state h_3202
                                                         ↑
                                          both are shape (256,)
```

The GRU recurrence is `h_t = f(x_t, h_{t-1})`. **`h_t` always has the same dimension regardless of `t`**, because `f` outputs a vector of fixed size `hidden=256`. Length variability lives in the *number of times you apply f*; the result of the last application has the same shape no matter what `t` was.

### Step 4 — slice the top layer (`model.py:73-76`)

```python
def embedding(self, src, src_len):
    h = self.encode(src, src_len)
    return h[-1]                     # (B, hidden=256)
```

`h` is `(layers=2, B, 256)` — the final hidden state of *each* GRU layer. We take the top layer (`h[-1]`) as the trajectory's 256-dim embedding vector. This is what gets written into `embeddings.npy`.

---

## 3. Why that vector has meaning

A randomly-initialized GRU final state would be garbage — fixed-size, yes, but not informative. The training objective makes it useful:

1. **Encoder** produces `h_final` from the (possibly corrupted) input sequence.
2. **Decoder** GRU is seeded with `h_final` and must **reconstruct the clean original sequence**, token by token (`model.py:78-82`).
3. Loss is cross-entropy at every decoder timestep against the clean target (`train_encode.py:154`).

For the decoder to succeed, every bit of information needed to reproduce the route — which cameras, in which order, with what overall structure — has to be packed into the 256-dim `h_final`. That makes `h_final` an **information bottleneck** that learns to be a route summary.

The denoising twist (`corrupt()` in `model.py:85-90` drops a fraction of tokens before feeding the encoder) forces the embedding to be robust to missing observations — exactly the OCR/occlusion failure mode in ANPR data.

After training, two plates that drove the same route end up close in 256-dim space because their `h_final`s have to point the decoder to the same output sequence. That's why Stage 3's clustering finds fleets.

---

## 4. Why `pack_padded_sequence` matters specifically

Without it, the GRU would happily process padding tokens too:

- Each `PAD` position would run `h_t = f(emb(PAD), h_{t-1})` and *change* the hidden state with random PAD updates.
- Plate A's final state would actually be `h_3202` (with 3,068 PAD-driven updates after position 134), not `h_134`.
- All embeddings would collapse toward "what the PAD token's updates lead to," washing out the route signal.

`pack_padded_sequence` makes the GRU stop updating each row at its real `src_len`, so plate A's embedding is genuinely `h_134` and plate B's is `h_3202`. That's how you can put very-different-length plates in the same batch and still get meaningful embeddings out.

---

## 5. Summary table

| component | role | output shape | depends on length? |
|---|---|---|---|
| `collate` | pad to rectangle | `(B, T_max)` | T_max only |
| `tok_emb` | look up token vectors | `(B, T_max, 128)` | T_max only |
| `pack_padded_sequence` | length-aware GRU input | packed object | uses real lengths |
| `encoder` (GRU) | run recurrence | `(layers, B, 256)` | **no — fixed by `hidden`** |
| `h[-1]` | take top layer's final state | `(B, 256)` | **no** |

The fixed `hidden=256` dimension of the GRU's hidden state is what gives every plate the same-size embedding. The number of GRU steps differs per plate; the size of what the GRU emits at the end doesn't.

---

## 6. Practical consequences (relevant to this project)

- **Embedding dim is set by the `--hidden` flag of `train_encode.py`**, not by the input length. Default 256; we use 256.
- **Sequence-length cap (`--max-len` in `build_sequences.py`)** does *not* change the embedding dim — only the training cost. Lower cap = shorter GRU unroll = less memory and faster convergence, with no change to the output shape. See `doc/MaxGrowth.md` and the project log for why we went from `--max-len 0` (uncapped, up to 3,202) to a length cap during retraining.
- **Long sequences are an instability risk, not a dimensionality risk.** A 3,000-step BPTT can blow gradients into NaN; the embedding shape is unaffected. That's why `train_encode.py` has a NaN-guard around the optimizer step — see the inline comment around `train_encode.py:152`.
- **Downstream (`cluster_confirm.py`)** sees only `(N, 256)` regardless of how the plates were originally sized. HDBSCAN doesn't know or care that plate A had 134 cameras and plate B had 3,202.
