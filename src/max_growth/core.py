"""Pure-Python MaxGrowth core.

Faithful to the structure of Bei et al. (VLDB 2024); the inputs and outputs
are simple dicts/lists so Spark can drive this with `collect()` on prepared
data and have nothing dataframe-shaped inside the algorithm.

Glossary (paper-aligned):
  plate           one tracked object identifier (an int in our data)
  camera          one checkpoint identifier (an int)
  trajectory[p]   sorted list of (time, camera) tuples — every visit by plate p
  pos_index[p]    dict mapping camera -> last index it appears in p's trajectory
  cluster         (camera, frozenset_of_plates) — m plates ε-close at one camera
  pattern         (route, members)
                    route   = tuple of camera ids (ordered)
                    members = frozenset of plates that ALL traversed every step

Algorithm (Section 4 of the paper, with our adaptations marked ⊕):

  ⊕ pre-filter plates whose visit count < k          (Apriori; safe because no
                                                      length-k route can use a
                                                      plate with < k visits)
    compute per-camera clusters with sliding window using ε
    keep clusters with |members| ≥ m
    foreach cluster CL₀ ∈ clusters:
        growth([CL₀], output)
    remove non-maximal patterns from output

  growth(S, out):
      let CL_last = S[-1], cam_last = CL_last.camera
      for each plate p in CL_last.members:        # candidate extensions
          collect next-camera options c′ from p's trajectory with
              0 < pos(p, c′) − pos(p, cam_last) ≤ d+1
          intersect members across all p ∈ members(CL_last) for each c′
          form candidate cluster CL′ = (c′, ∩ p.members), keep if |...| ≥ m
      for each surviving CL′:
          if len(S) + 1 ≥ k: emit pattern (route(S) ++ [c′], members(CL′))
          growth(S ++ [CL′], out)

  Maximality filter: pattern A dominates B  iff  members(A) ⊇ members(B)
                                            and route(A) is a d-supersequence
                                            of route(B). Keep only non-dominated.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


# ── types ──────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class Cluster:
    camera: int
    members: frozenset[int]    # plate ids


@dataclass(frozen=True)
class Pattern:
    route: tuple[int, ...]     # ordered cameras
    members: frozenset[int]    # plates in the platoon


# ── 1. Cluster construction ────────────────────────────────────────────
def find_clusters(
    arrivals_per_camera: dict[int, list[tuple[int, int]]],
    m: int,
    eps: int,
) -> list[Cluster]:
    """For each camera, sweep a time window of width ε; emit a Cluster
    whenever ≥ m plates are simultaneously inside it.

    arrivals_per_camera: camera_id -> [(time, plate), ...]  any order
    Returns clusters de-duplicated by (camera, members).
    """
    seen: set[tuple[int, frozenset[int]]] = set()
    out: list[Cluster] = []
    for cam, arrivals in arrivals_per_camera.items():
        arrivals.sort(key=lambda x: x[0])
        i = 0
        for j in range(len(arrivals)):
            while arrivals[j][0] - arrivals[i][0] > eps:
                i += 1
            window = arrivals[i:j + 1]
            if len(window) >= m:
                members = frozenset(p for _, p in window)
                if len(members) >= m:
                    key = (cam, members)
                    if key not in seen:
                        seen.add(key)
                        out.append(Cluster(cam, members))
    return out


# ── 2. Per-plate position index ────────────────────────────────────────
def position_indices(
    trajectories: dict[int, list[tuple[int, int]]],
) -> dict[int, dict[int, int]]:
    """plate -> {camera -> last position in trajectory}.

    'Last position' is what the paper actually needs in its `0 < p_c^i - p_c^n
    ≤ d+1` predicate: the *latest* time the plate visited that camera. Earlier
    visits at the same camera form their own candidate clusters but don't
    constrain the forward route step.
    """
    return {
        plate: {cam: idx for idx, (_, cam) in enumerate(traj)}
        for plate, traj in trajectories.items()
    }


# ── 3. Growth recursion ────────────────────────────────────────────────
def grow_from_root(
    root: Cluster,
    by_camera: dict[int, list[Cluster]],
    pos: dict[int, dict[int, int]],
    next_cams_per_plate: dict[int, list[int]],
    m: int, k: int, d: int,
) -> list[Pattern]:
    """Recursively grow patterns from a single root cluster.

    Pulled out as a top-level function so Spark can call it once per
    partitioned root, with `by_camera`, `pos`, and `next_cams_per_plate`
    arriving via broadcast.
    """
    out: list[Pattern] = []

    def recurse(stack: list[Cluster]) -> None:
        last = stack[-1]
        members = last.members

        # Each member proposes its own next cameras within d+1 positions;
        # the candidate cluster at c′ is the intersection.
        proposals: dict[int, set[int]] = {}
        for p in members:
            cur_pos = pos.get(p, {}).get(last.camera)
            if cur_pos is None:
                continue
            for c2 in next_cams_per_plate.get(p, ()):
                p2 = pos[p].get(c2)
                if p2 is None:
                    continue
                gap = p2 - cur_pos
                if 0 < gap <= d + 1:
                    proposals.setdefault(c2, set()).add(p)

        # Track whether any extension preserves the full current membership.
        # If yes, the longer pattern strictly dominates this stack — don't
        # emit. This local pruning collapses the emit-at-every-depth bloat
        # that would otherwise blow up the maximality filter downstream.
        extension_keeps_members = False
        for c2, supporters in proposals.items():
            if len(supporters) < m:
                continue
            new_members = frozenset(supporters)
            for cl2 in by_camera.get(c2, ()):
                shared = cl2.members & new_members
                if len(shared) < m:
                    continue
                if shared == members:
                    extension_keeps_members = True
                recurse(stack + [Cluster(c2, shared)])

        if not extension_keeps_members and len(stack) >= k:
            out.append(Pattern(
                route=tuple(c.camera for c in stack),
                members=members,
            ))

    recurse([root])
    return out


def growth(
    clusters: list[Cluster],
    pos: dict[int, dict[int, int]],
    next_cams_per_plate: dict[int, list[int]],
    m: int, k: int, d: int,
) -> list[Pattern]:
    """Single-driver convenience wrapper around `grow_from_root` — runs
    growth from every cluster sequentially. Used by `run_max_growth()`
    for local testing; production path is the Spark wrapper that
    parallelizes `grow_from_root` across executors."""
    by_camera: dict[int, list[Cluster]] = {}
    for cl in clusters:
        by_camera.setdefault(cl.camera, []).append(cl)
    out: list[Pattern] = []
    for cl in clusters:
        out.extend(grow_from_root(cl, by_camera, pos, next_cams_per_plate, m, k, d))
    return out


# ── 4. Maximality filter ───────────────────────────────────────────────
def is_dsubseq(short: tuple[int, ...], long: tuple[int, ...], d: int) -> bool:
    """True iff `short` appears in `long` as a subsequence with gaps ≤ d."""
    if len(short) > len(long):
        return False
    i = 0  # pointer into short
    prev_j = -1
    for j, c in enumerate(long):
        if i < len(short) and c == short[i]:
            if prev_j >= 0 and (j - prev_j - 1) > d:
                return False
            prev_j = j
            i += 1
    return i == len(short)


def maximal_only(patterns: list[Pattern], d: int) -> list[Pattern]:
    """Keep only patterns not dominated by any other.

    A dominates B  iff  A.members ⊇ B.members  AND  B.route is a d-subseq of A.route
                   AND  not (A == B).

    Inverted-index acceleration: a dominator of B must contain every member
    of B, so candidates are confined to the intersection of `plate → patterns`
    inverted lists. We seed the search with B's rarest member, then verify
    each candidate against the remaining members directly — avoiding a full
    Python set.intersection() over (potentially) huge lists.
    """
    if not patterns:
        return patterns
    from collections import defaultdict
    plate_to_idx: dict[int, list[int]] = defaultdict(list)
    for i, p in enumerate(patterns):
        for plate in p.members:
            plate_to_idx[plate].append(i)

    keep: list[Pattern] = []
    for i, b in enumerate(patterns):
        if not b.members:
            keep.append(b)
            continue
        # Seed candidate set on B's rarest member.
        seed_plate = min(b.members, key=lambda pl: len(plate_to_idx[pl]))
        candidates = plate_to_idx[seed_plate]

        dominated = False
        b_members = b.members
        b_route = b.route
        b_route_len = len(b_route)
        b_member_count = len(b_members)
        for j in candidates:
            if j == i:
                continue
            a = patterns[j]
            # Cheap rejections first.
            if len(a.members) < b_member_count or len(a.route) < b_route_len:
                continue
            if not (a.members >= b_members):
                continue
            if a.members == b_members and a.route == b_route:
                continue
            if is_dsubseq(b_route, a.route, d):
                dominated = True
                break
        if not dominated:
            keep.append(b)
    return keep


# ── 5. Entry point used by the Spark wrapper ───────────────────────────
def run_max_growth(
    trajectories: dict[int, list[tuple[int, int]]],
    m: int, k: int, d: int, eps: int,
) -> list[Pattern]:
    """trajectories: plate -> sorted [(time, camera), ...].

    Returns the maximal platoon patterns.
    """
    # Apriori prefilter: a plate with < k visits cannot participate in any
    # length-k route. This is safe and dramatically narrows the search.
    trajectories = {p: t for p, t in trajectories.items() if len(t) >= k}
    if not trajectories:
        return []

    arrivals_per_camera: dict[int, list[tuple[int, int]]] = {}
    next_cams_per_plate: dict[int, list[int]] = {}
    for plate, traj in trajectories.items():
        next_cams_per_plate[plate] = [c for _, c in traj]
        for t, c in traj:
            arrivals_per_camera.setdefault(c, []).append((t, plate))

    clusters = find_clusters(arrivals_per_camera, m, eps)
    pos = position_indices(trajectories)
    raw = growth(clusters, pos, next_cams_per_plate, m, k, d)
    # de-duplicate identical (route, members) pairs before maximality filter
    raw = list({(p.route, p.members): p for p in raw}.values())
    return maximal_only(raw, d)
