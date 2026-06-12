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

---

# TBPTT and bucket batching

> Two training-time techniques that address the long-sequence gradient-stability problem the embedding trainer ran into on this dataset. Both are knobs on `train_encode.py` (`--tbptt-chunk` and `--num-buckets`); both default to *off* for backward compatibility.

## 7. The problem they solve

The Stage 1 output (sequences/) has a long-tailed length distribution:

```
min=10  median=38  mean=62.5  max=3,202
```

Most plates are short; a small fraction of bus / taxi / commercial vehicles have thousands of cameras. Two failure modes follow:

1. **Random batching makes every bus contaminate ~31 normal plates.** A 3,202-token sequence in a batch forces all other rows to pad to length 3,202. The GRU then BPTTs through 3,202 timesteps on every row, regardless of those rows' real lengths. This both wastes compute and amplifies the gradient-explosion risk.
2. **Decoder BPTT at depth 3,000+ overflows.** Even with `clip_grad_value_`, `nan_to_num_` gradient rescue, and bf16 autocast, end-to-end gradient flow through 3,000+ teacher-forced steps eventually drifts the model into a numerically unstable region — observed empirically as NaN losses starting in epoch 6–8 on this project's runs.

Two complementary fixes, both implemented in `src/embed/`:

| technique | knob | what it bounds | code location |
|---|---|---|---|
| Bucket batching | `--num-buckets N` | per-batch max sequence length | `BucketBatchSampler` in `train_encode.py` |
| Truncated BPTT (decoder) | `--tbptt-chunk N` | per-batch decoder BPTT depth | `Seq2SeqAE.forward` in `model.py` |

## 8. Bucket batching

### Idea

Group indices into N buckets by sequence length, then form batches *within* each bucket. A batch's `T_max` is now the bucket's max length, not the dataset's max. Long sequences only batch with other long sequences.

```
random batching:
  batch = [38, 134, 22, 3202, 47, 18, …]
  T_max = 3202 → every row pads to 3202, every GRU runs 3,202 steps

bucket batching, num_buckets=16, bucket containing this batch:
  batch = [60, 62, 58, 65, 61, 59, …]          (the "median" bucket)
  T_max = 65 → minimal padding, GRU runs ~65 steps
```

### Implementation

`train_encode.py` defines `BucketBatchSampler` (single-process) and `DistributedBucketBatchSampler` (multi-rank). Both:

1. **Sort all indices by length** once at construction (`O(N log N)`, ~10 M comparisons for our 3.83 M plates — milliseconds).
2. **Split into `num_buckets` equal-size buckets** of consecutive indices. Bucket 0 has the shortest; bucket N-1 has the longest.
3. **Each epoch**: shuffle indices within each bucket; form batches of size `batch_size` within each bucket; shuffle the batch list. `set_epoch(e)` reseeds the RNG so different epochs see different orderings.
4. **DDP variant** truncates the global batch list to a multiple of `world_size` and yields every `world_size`-th batch starting from `rank`. This guarantees every rank produces the same number of batches per epoch — no straggler hang on the final all-reduce.

The DataLoader uses `batch_sampler=` (yields whole batches) instead of `sampler=` + `batch_size=` (yields individual indices). `collate_fn=collate` is unchanged.

### What it costs

- **Randomness**: with N buckets you sacrifice some inter-batch randomness — a batch in the "long" bucket can never include a short sequence. In practice `num_buckets=8–32` keeps enough within-epoch randomness while cutting `T_max` per batch by ~10×.
- **Memory imbalance across ranks**: the "long" bucket's batches push more memory than the "short" bucket's. If you OOM only when long-bucket batches hit, the right knob is per-bucket batch size — not implemented yet; the current sampler uses a uniform `batch_size`. For now, set `--batch-size` to whatever fits the *longest* bucket.

### Tuning

| symptom | knob | direction |
|---|---|---|
| OOM only on certain batches | `--batch-size` | down — long bucket dictates |
| Too much within-batch length variance still | `--num-buckets` | up (eg 16 → 32) |
| Convergence noisier than random batching | `--num-buckets` | down (8) — recovers more shuffling |
| Long bucket dominates wall time | `--batch-size` up + `--num-buckets` up | smaller-batch long bucket, more buckets |

## 9. Truncated BPTT (decoder side)

### Idea

Backprop through the *full* decoder gradient chain is what blew up at 3,000 timesteps. TBPTT runs the decoder in chunks of `chunk` tokens and **detaches the hidden state between chunks**, so gradients can never flow further back than `chunk` steps regardless of target length.

The encoder still runs as one pass (its forward is just `pack_padded_sequence` → final hidden; no per-step decoder loss to dictate chunking).

### Implementation (`model.py:78-103`)

```python
def forward(self, src, src_len, tgt_in):
    h = self.encode(src, src_len)
    y = self.tok_emb(tgt_in)
    if self.tbptt_chunk <= 0:
        dec, _ = self.decoder(y, h)
        return self.out(dec)               # vanilla full-target pass

    chunk = self.tbptt_chunk
    T = tgt_in.size(1)
    dec_h = h
    chunk_logits = []
    for start in range(0, T, chunk):
        end = min(start + chunk, T)
        dec_out, dec_h_new = self.decoder(y[:, start:end], dec_h)
        chunk_logits.append(self.out(dec_out))
        dec_h = dec_h_new.detach()         # ← caps decoder BPTT here
    return torch.cat(chunk_logits, dim=1)
```

`tbptt_chunk` is set on `core` (the un-wrapped module) from `train_encode.py` after DDP wrapping, so the DDP forward signature is unchanged and the gradient sync hooks fire normally on one backward per batch.

### Gradient flow caveat

The `detach()` between chunks has a real consequence:

- **Chunk 0**'s logits depend on `dec_h = h_enc` (still attached). Backprop from chunk 0's loss flows back through the decoder *and* into the encoder via `h_enc`. The encoder gets a training signal.
- **Chunks 1, 2, …**'s logits depend on `dec_h = previous_chunk_h.detach()` (detached). Backprop from those chunks' losses flows back through the decoder weights only — *not* into the encoder.

So with TBPTT enabled, **the encoder learns only from reconstructing the first `tbptt_chunk` tokens** of each clean sequence. For trajectory embedding this is benign: the route signature appears in any 256-window of the trajectory, so the encoder still learns to produce embeddings that capture route structure. But it's a different optimization landscape than the no-TBPTT case.

A consequence worth noting: **TBPTT in this form does not reduce memory**, only gradient depth. The full `(B, T, V)` logits tensor still exists; we just inserted detach points in its construction. If memory is the constraint, raise `--num-buckets` or lower `--batch-size` instead. If gradient stability is the constraint, lower `--tbptt-chunk`.

### Tuning

| symptom | knob | direction |
|---|---|---|
| NaN losses appearing late in training | `--tbptt-chunk` | down (256 → 128 → 64) |
| Encoder undertraining (cluster quality poor) | `--tbptt-chunk` | up — gives encoder more signal |
| Sweet spot from this dataset's testing | `--tbptt-chunk 256` | matches Stage 1's typical `--max-len` |

## 10. Combined recipe

For full-month sequences with `--max-len 0` (uncapped at Stage 1, up to 3,202 tokens), the combination that's worked is:

```bash
$PY -m torch.distributed.run --standalone --nproc_per_node=4 \
    $REPO/src/embed/train_encode.py \
    $REPO/data/output/embed_31/sequences \
    $REPO/data/output/embed_31/vectors \
    --epochs 10 --batch-size 32 --hidden 256 --emb-dim 128 --drop-prob 0.3 \
    --num-buckets 16 --tbptt-chunk 256
```

What each piece does:

- `--num-buckets 16` — most batches now have T_max ≤ 100 (you'll see the per-epoch `loss=…` lines descend faster too, because less compute is wasted on padding).
- `--tbptt-chunk 256` — even for the long-tail bucket where `T_max ≈ 3,000`, the decoder gradient chain is bounded at 256 steps.
- `bf16 autocast` (set automatically when `torch.cuda.is_bf16_supported()`) — same exponent range as fp32, can't overflow into inf.
- `clip_grad_value_(p, 1.0)` (replacing the old `clip_grad_norm_`) — per-element clamp, survives stray inf entries that norm-clip would mangle.
- `nan_to_num_(grad)` gradient rescue — partially-bad gradients get their inf/NaN elements replaced rather than the whole step skipped.
- `skip-on-nonfinite-loss` — last-line backstop. The per-epoch `skipped=N` / `rescued=N` counters tell you how often each path fired.

### What to verify after training

```bash
$REPO/.venv/bin/python -c "
import numpy as np
e = np.load('$REPO/data/output/embed_31/vectors/embeddings.npy')
nan = int(np.isnan(e).any(axis=1).sum())
fin = e[np.isfinite(e).all(axis=1)]
print(f'rows: {e.shape[0]:,}  NaN rows: {nan}')
if len(fin): print(f'mean={fin.mean():.3g}  std={fin.std():.3g}')
"
```

Expected: `NaN rows: 0`, std in `[0.05, 0.5]`, no infinities. If any of those fail, the run produced toxic embeddings — re-train with tighter knobs (lower `--tbptt-chunk` or lower `--lr`).
