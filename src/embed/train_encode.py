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
from pathlib import Path

import numpy as np
import pyarrow.dataset as ds
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, Dataset, DistributedSampler

from model import BOS, EOS, PAD, Seq2SeqAE, Vocab, corrupt


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
    """Return (rank, world_size, local_rank); init process group if distributed."""
    if "RANK" in os.environ and int(os.environ.get("WORLD_SIZE", "1")) > 1:
        torch.distributed.init_process_group(backend="nccl"
                                             if torch.cuda.is_available() else "gloo")
        rank = int(os.environ["RANK"])
        world = int(os.environ["WORLD_SIZE"])
        local = int(os.environ.get("LOCAL_RANK", "0"))
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
    plates, cameras = load_sequences(a.sequences)
    log(f"loaded {len(plates):,} sequences on device={device} world_size={world}")

    all_cams = {c for seq in cameras for c in seq}
    vocab = Vocab(all_cams)
    encoded = [vocab.encode(seq) for seq in cameras]
    log(f"vocab size {vocab.size:,} ({len(all_cams):,} cameras + {vocab.size - len(all_cams)} special)")

    dataset = SeqDataset(encoded, a.drop_prob, a.seed + rank)
    sampler = DistributedSampler(dataset, num_replicas=world, rank=rank,
                                 shuffle=True) if distributed else None
    loader = DataLoader(dataset, batch_size=a.batch_size, shuffle=sampler is None,
                        sampler=sampler, collate_fn=collate,
                        num_workers=a.num_workers, drop_last=False)

    model = Seq2SeqAE(vocab.size, a.emb_dim, a.hidden, a.layers).to(device)
    if distributed:
        model = nn.parallel.DistributedDataParallel(
            model, device_ids=[local] if device.type == "cuda" else None)
    core = model.module if distributed else model

    opt = torch.optim.Adam(model.parameters(), lr=a.lr)
    loss_fn = nn.CrossEntropyLoss(ignore_index=PAD)

    model.train()
    for epoch in range(a.epochs):
        if sampler is not None:
            sampler.set_epoch(epoch)
        total, nb = 0.0, 0
        for src, src_len, tin, tout in loader:
            src, src_len = src.to(device), src_len.to(device)
            tin, tout = tin.to(device), tout.to(device)
            logits = model(src, src_len, tin)
            loss = loss_fn(logits.reshape(-1, logits.size(-1)), tout.reshape(-1))
            opt.zero_grad()
            loss.backward()
            nn.utils.clip_grad_norm_(model.parameters(), 5.0)
            opt.step()
            total += loss.item(); nb += 1
        log(f"epoch {epoch + 1}/{a.epochs}  loss={total / max(nb, 1):.4f}")

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
