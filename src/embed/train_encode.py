"""Stage 2 — train the denoising autoencoder and encode every plate (GPU).

Consumes Stage 1's compact per-plate sequences (Parquet, read with pyarrow —
no Spark needed; after prefilter the whole set fits in host RAM). Trains the
t2vec-style autoencoder in model.py, then writes one embedding vector per plate.

    embeddings.npy   float32  (n_plates, hidden)
    plates.npy       int64    (n_plates,)        row i ↔ embeddings[i]
    vocab.json                camera-id ↔ token map
    model.pt                  trained weights (for re-encoding new data)

Device: CUDA if available, else CPU (so the data path is testable on a CPU
box; real training wants a GPU). Multi-GPU via torchrun is auto-detected from
the RANK / WORLD_SIZE env vars set by src/slurm/gpu_embed.sbatch.

    python train_encode.py <sequences_parquet> <out_dir> [options]
"""
from __future__ import annotations

import argparse
import json
import os
import pickle
import random
from datetime import timedelta
from pathlib import Path

import numpy as np
import pyarrow.dataset as ds
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, Dataset, DistributedSampler

from model import BOS, EOS, PAD, Seq2SeqAE, Vocab, corrupt


class BucketBatchSampler:
    """Group indices into length-buckets and yield batches from within each bucket.

    Instead of random batching (where one 3,202-camera bus trajectory forces all
    31 other plates in the batch to pad to 3,202), this groups sequences of
    similar length so each batch's max length tracks the bucket's max length.
    Average BPTT depth drops from ~max-in-dataset to ~bucket-mean.

    Knobs:
      - num_buckets:  more buckets => tighter length grouping (less padding waste,
                      less gradient stability risk per batch) but less batch-level
                      shuffling randomness. 8–32 is the common sweet spot.
      - shuffle:      shuffle indices within each bucket and shuffle batch order
                      across buckets each epoch (call set_epoch from the loop).

    Yields lists of indices (suitable for DataLoader's `batch_sampler=`).
    """

    def __init__(self, lengths, batch_size, num_buckets=8,
                 shuffle=True, seed=42, drop_last=False):
        self.batch_size = batch_size
        self.num_buckets = max(1, int(num_buckets))
        self.shuffle = shuffle
        self.seed = seed
        self.drop_last = drop_last
        self.epoch = 0
        sorted_idx = sorted(range(len(lengths)), key=lambda i: lengths[i])
        bucket_size = (len(sorted_idx) + self.num_buckets - 1) // self.num_buckets
        self.buckets = [sorted_idx[i:i + bucket_size]
                        for i in range(0, len(sorted_idx), bucket_size)]

    def _build_batches(self):
        rng = random.Random(self.seed + self.epoch)
        all_batches = []
        for bucket in self.buckets:
            idx = list(bucket)
            if self.shuffle:
                rng.shuffle(idx)
            for s in range(0, len(idx), self.batch_size):
                batch = idx[s:s + self.batch_size]
                if self.drop_last and len(batch) < self.batch_size:
                    continue
                all_batches.append(batch)
        if self.shuffle:
            rng.shuffle(all_batches)
        return all_batches

    def __iter__(self):
        yield from self._build_batches()

    def __len__(self):
        n = 0
        for bucket in self.buckets:
            if self.drop_last:
                n += len(bucket) // self.batch_size
            else:
                n += (len(bucket) + self.batch_size - 1) // self.batch_size
        return n

    def set_epoch(self, epoch):
        self.epoch = int(epoch)


class DistributedBucketBatchSampler(BucketBatchSampler):
    """Per-rank slicing of BucketBatchSampler for DDP.

    Each rank produces an equal number of batches (we truncate the global
    batch list to a multiple of world_size so DDP's all-reduce never hangs
    waiting for a straggler rank with one fewer batch).
    """

    def __init__(self, lengths, batch_size, num_buckets,
                 num_replicas, rank, shuffle=True, seed=42):
        super().__init__(lengths, batch_size, num_buckets,
                         shuffle=shuffle, seed=seed, drop_last=False)
        self.num_replicas = max(1, int(num_replicas))
        self.rank = int(rank)
        total = sum(
            (len(b) + batch_size - 1) // batch_size for b in self.buckets)
        self._per_rank = total // self.num_replicas
        self._total = self._per_rank * self.num_replicas

    def __iter__(self):
        batches = self._build_batches()[: self._total]
        for i in range(self.rank, len(batches), self.num_replicas):
            yield batches[i]

    def __len__(self):
        return self._per_rank


def load_sequences(path: str):
    """Read the Parquet sequence directory into (plates, camera-lists)."""
    table = ds.dataset(path, format="parquet").to_table(columns=["plate", "cameras"])
    plates = table.column("plate").to_pylist()
    cameras = table.column("cameras").to_pylist()
    return plates, cameras


class SeqDataset(Dataset):
    def __init__(self, encoded, drop_prob, seed):
        self.encoded = encoded
        self.drop_prob = drop_prob
        # one RNG per worker-process; seed varies by index in __getitem__
        import random
        self._rng = random.Random(seed)

    def __len__(self):
        return len(self.encoded)

    def __getitem__(self, i):
        clean = self.encoded[i]
        noisy = corrupt(clean, self.drop_prob, self._rng)
        return noisy, clean


def collate(batch):
    """Pad a batch into src (noisy), tgt_in (BOS+clean), tgt_out (clean+EOS)."""
    srcs, cleans = zip(*batch)
    src_len = torch.tensor([len(s) for s in srcs], dtype=torch.long)
    smax = max(len(s) for s in srcs)
    tmax = max(len(c) for c in cleans) + 1
    B = len(batch)
    src = torch.full((B, smax), PAD, dtype=torch.long)
    tin = torch.full((B, tmax), PAD, dtype=torch.long)
    tout = torch.full((B, tmax), PAD, dtype=torch.long)
    for i, (s, c) in enumerate(zip(srcs, cleans)):
        src[i, :len(s)] = torch.tensor(s, dtype=torch.long)
        tin[i, 0] = BOS
        tin[i, 1:1 + len(c)] = torch.tensor(c, dtype=torch.long)
        tout[i, :len(c)] = torch.tensor(c, dtype=torch.long)
        tout[i, len(c)] = EOS
    return src, src_len, tin, tout


def ddp_setup():
    """Return (rank, world_size, local_rank); init process group if distributed.

    NCCL default timeout is 10 min. On large datasets, pre-DDP host-side data
    prep (parquet → Python lists, vocab build, encode of ~10⁸ tokens) routinely
    eats 5-10 min under 4-way memory contention. A rank that's even a few
    minutes behind reach DDP after the others triggers a TCPStore timeout on
    the leaders — they were waiting for the laggard's ncclUniqueId publish.
    Bump to 60 min as a safety net; the actual training all-reduces are sub-second.

    PyTorch 2.5+ NCCL requires the device be bound BEFORE init_process_group
    and passed in via `device_id=`, otherwise barrier()/all_reduce() guess the
    device and can hit `Cuda failure 1 'invalid argument'`. Bind first.
    """
    if "RANK" in os.environ and int(os.environ.get("WORLD_SIZE", "1")) > 1:
        rank = int(os.environ["RANK"])
        world = int(os.environ["WORLD_SIZE"])
        local = int(os.environ.get("LOCAL_RANK", "0"))
        init_kwargs = {
            "backend": "nccl" if torch.cuda.is_available() else "gloo",
            "timeout": timedelta(minutes=60),
        }
        if torch.cuda.is_available():
            torch.cuda.set_device(local)
            init_kwargs["device_id"] = torch.device(f"cuda:{local}")
        torch.distributed.init_process_group(**init_kwargs)
        return rank, world, local
    return 0, 1, 0


def main():
    p = argparse.ArgumentParser()
    p.add_argument("sequences")
    p.add_argument("output")
    p.add_argument("--emb-dim", type=int, default=128)
    p.add_argument("--hidden", type=int, default=256)
    p.add_argument("--layers", type=int, default=2)
    p.add_argument("--epochs", type=int, default=10)
    p.add_argument("--batch-size", type=int, default=256)
    p.add_argument("--lr", type=float, default=1e-3)
    p.add_argument("--drop-prob", type=float, default=0.3,
                   help="fraction of tokens dropped to make the noisy input "
                        "(the denoising signal that buys occlusion robustness)")
    p.add_argument("--num-workers", type=int, default=2)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--num-buckets", type=int, default=0,
                   help="Group sequences by length into N buckets and batch "
                        "within each bucket (0 = off, classic random batching). "
                        "8–32 is the common sweet spot. Drastically reduces "
                        "per-batch BPTT depth on long-tailed length distributions "
                        "(eg bus trajectories at 3,000+ tokens). See "
                        "doc/Embeddings.md § TBPTT and bucket batching.")
    p.add_argument("--tbptt-chunk", type=int, default=0,
                   help="Decoder TBPTT chunk size. 0 = off (vanilla full-target "
                        "decoder). When > 0, the decoder is run in chunks of "
                        "this many tokens with hidden-state detach between "
                        "chunks — caps decoder BPTT depth at the chunk size. "
                        "See doc/Embeddings.md § TBPTT and bucket batching for "
                        "the gradient-flow caveat (encoder learns from chunk 0 "
                        "only). Pair with --num-buckets for best stability.")
    a = p.parse_args()

    rank, world, local = ddp_setup()
    distributed = world > 1
    if torch.cuda.is_available():
        device = torch.device(f"cuda:{local}")
        torch.cuda.set_device(device)
    else:
        device = torch.device("cpu")
    is_main = rank == 0

    def log(msg):
        if is_main:
            print(f"[embed-train] {msg}", flush=True)

    torch.manual_seed(a.seed)

    # ── Encoded-sequence cache (rank 0 builds; everyone reads) ───────────
    # Reading 1.5 GB of parquet and encoding 237 M tokens to Python ints in
    # every rank is what wedged the previous run — under 4-way contention the
    # work routinely takes > 10 min, blowing past the NCCL/c10d store timeout
    # while the leaders waited for the laggard. Have rank 0 do it once and
    # cache to disk; the other ranks barrier-wait and read the cache.
    cache_path = Path(a.sequences).parent / "encoded_cache.pkl"
    plates = encoded = vocab = None

    if is_main and not cache_path.exists():
        plates_, cameras = load_sequences(a.sequences)
        log(f"loaded {len(plates_):,} sequences on device={device} world_size={world}")
        all_cams = {c for seq in cameras for c in seq}
        vocab = Vocab(all_cams)
        encoded = [vocab.encode(seq) for seq in cameras]
        plates = plates_
        log(f"vocab size {vocab.size:,} ({len(all_cams):,} cameras + "
            f"{vocab.size - len(all_cams)} special)")
        log(f"caching encoded sequences to {cache_path}")
        with open(cache_path, "wb") as f:
            pickle.dump((plates, encoded, vocab.state_dict()), f, protocol=4)
        log(f"cache ready ({cache_path.stat().st_size / 1024**2:.0f} MiB)")

    if distributed:
        # Non-rank-0 ranks wait here; rank 0 hits this after caching is done.
        # Pass device_ids so NCCL doesn't have to guess (the guess can hit
        # `Cuda failure 1 'invalid argument'` on some PyTorch+NCCL builds).
        if device.type == "cuda":
            torch.distributed.barrier(device_ids=[local])
        else:
            torch.distributed.barrier()

    if encoded is None:  # non-main ranks, or main rank found pre-existing cache
        with open(cache_path, "rb") as f:
            plates, encoded, vocab_state = pickle.load(f)
        vocab = Vocab.from_state(vocab_state)
        if is_main:
            log(f"loaded {len(plates):,} sequences from cache "
                f"({cache_path.stat().st_size / 1024**2:.0f} MiB)")

    dataset = SeqDataset(encoded, a.drop_prob, a.seed + rank)

    # ── Loader: bucket batching (length-grouped) or classic random ───────
    sampler = None
    batch_sampler = None
    if a.num_buckets > 0:
        lengths = [len(seq) for seq in encoded]
        if distributed:
            batch_sampler = DistributedBucketBatchSampler(
                lengths, a.batch_size, a.num_buckets,
                num_replicas=world, rank=rank, seed=a.seed)
        else:
            batch_sampler = BucketBatchSampler(
                lengths, a.batch_size, a.num_buckets, seed=a.seed)
        loader = DataLoader(dataset, batch_sampler=batch_sampler,
                            collate_fn=collate, num_workers=a.num_workers)
        log(f"bucket batching: {a.num_buckets} buckets, "
            f"{len(batch_sampler):,} batches/rank/epoch")
    else:
        sampler = DistributedSampler(dataset, num_replicas=world, rank=rank,
                                     shuffle=True) if distributed else None
        loader = DataLoader(dataset, batch_size=a.batch_size,
                            shuffle=sampler is None, sampler=sampler,
                            collate_fn=collate, num_workers=a.num_workers,
                            drop_last=False)

    model = Seq2SeqAE(vocab.size, a.emb_dim, a.hidden, a.layers).to(device)
    if distributed:
        model = nn.parallel.DistributedDataParallel(
            model, device_ids=[local] if device.type == "cuda" else None)
    core = model.module if distributed else model

    # Decoder-side TBPTT lives inside Seq2SeqAE.forward and reads this attribute.
    # Set it on the underlying (un-wrapped) module so DDP's forward signature
    # is unchanged and gradient sync semantics stay correct.
    core.tbptt_chunk = max(0, int(a.tbptt_chunk))
    if core.tbptt_chunk > 0:
        log(f"decoder TBPTT: chunk={core.tbptt_chunk} (gradient detach between chunks)")

    opt = torch.optim.Adam(model.parameters(), lr=a.lr)
    loss_fn = nn.CrossEntropyLoss(ignore_index=PAD)

    # bf16 autocast: same exponent range as fp32 (no overflow → no inf/NaN
    # from numeric overflow during the long-sequence forward pass), with
    # fp16-equivalent memory. RNN/GRU cells are supported by autocast.
    # No GradScaler needed for bf16.
    use_autocast = device.type == "cuda" and torch.cuda.is_bf16_supported()
    log(f"autocast bf16: {use_autocast}")

    model.train()
    for epoch in range(a.epochs):
        if sampler is not None:
            sampler.set_epoch(epoch)
        if batch_sampler is not None:
            batch_sampler.set_epoch(epoch)
        total, nb, n_skipped, n_rescued = 0.0, 0, 0, 0
        for src, src_len, tin, tout in loader:
            src, src_len = src.to(device), src_len.to(device)
            tin, tout = tin.to(device), tout.to(device)
            with torch.autocast(device_type=device.type, dtype=torch.bfloat16,
                                enabled=use_autocast):
                logits = model(src, src_len, tin)
                loss = loss_fn(logits.reshape(-1, logits.size(-1)), tout.reshape(-1))
            # Belt + suspenders: bf16 should eliminate overflow, but if loss
            # still goes non-finite (eg from an undetected pathological seq),
            # skip the step entirely so one bad batch doesn't drift the model.
            if not torch.isfinite(loss):
                n_skipped += 1
                opt.zero_grad(set_to_none=True)
                continue
            opt.zero_grad()
            loss.backward()
            # Rescue partially-bad gradients: replace any NaN/Inf elements with
            # finite values rather than poisoning the optimizer step.
            had_bad_grad = False
            for p in model.parameters():
                if p.grad is None:
                    continue
                if not torch.isfinite(p.grad).all():
                    p.grad.nan_to_num_(nan=0.0, posinf=1.0, neginf=-1.0)
                    had_bad_grad = True
            if had_bad_grad:
                n_rescued += 1
            # Value-clip (per-element) is robust to inf/NaN entries in a way
            # norm-clip is not: norm(vec_with_nan) == nan, so norm-clip
            # multiplies every gradient by nan and produces a nan step.
            nn.utils.clip_grad_value_(model.parameters(), 1.0)
            opt.step()
            total += loss.item(); nb += 1
        notes = []
        if n_skipped: notes.append(f"skipped={n_skipped}")
        if n_rescued: notes.append(f"rescued={n_rescued}")
        note = ("  " + "  ".join(notes)) if notes else ""
        log(f"epoch {epoch + 1}/{a.epochs}  loss={total / max(nb, 1):.4f}{note}")

    # ── Encode every plate (rank 0 only; no corruption) ──────────────────
    if is_main:
        core.eval()
        out = Path(a.output); out.mkdir(parents=True, exist_ok=True)
        embs = np.zeros((len(encoded), a.hidden), dtype=np.float32)
        with torch.no_grad():
            for start in range(0, len(encoded), a.batch_size):
                chunk = encoded[start:start + a.batch_size]
                src, src_len, _, _ = collate([(c, c) for c in chunk])
                vec = core.embedding(src.to(device), src_len.to(device))
                embs[start:start + len(chunk)] = vec.cpu().numpy()
        np.save(out / "embeddings.npy", embs)
        np.save(out / "plates.npy", np.asarray(plates, dtype=np.int64))
        (out / "vocab.json").write_text(json.dumps(vocab.state_dict()))
        torch.save(core.state_dict(), out / "model.pt")
        log(f"wrote {embs.shape[0]:,}×{embs.shape[1]} embeddings to {out}/")

    if distributed:
        torch.distributed.destroy_process_group()


if __name__ == "__main__":
    main()
