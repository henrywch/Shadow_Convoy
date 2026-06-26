"""Generate the demo's live datasets from the real pipeline outputs:

  demo/js/seed_data.js   window.SEED_DATA  — verified seed plates → companions,
                         each annotated with which detectors register the plate;
                         window.SEED_LIST — the dropdown list of plates.
  demo/js/mapdata.js     window.MAPDATA — camera (x,y) points + top corridor
                         segments with coords, for the dynamic ECharts maps.

Seeds are the "highly verified" plates: every plate that appears in a 3-of-3
consensus group (all three detectors agree). For each seed we list its companions
(co-members across its fused groups), tagging each with the detectors that
register it (conv = FP-Growth, patt = MaxGrowth, emb = Embedding).

Usage:  python scripts/build_demo_data.py
"""
import csv
import json
from pathlib import Path

REPO = Path("/inspire/hdd/project/video-understanding/public/personal/chwang/live/cpjs/BD_Hadoop")
OUT = REPO / "demo/js"


def members_set(csv_path, col="members"):
    s = set()
    with open(csv_path) as f:
        for r in csv.DictReader(f):
            for p in str(r[col]).split(","):
                if p.strip():
                    s.add(p.strip())
    return s


def build_seeds():
    conv = members_set(REPO / "data/output/communities/fleets.csv")
    patt = members_set(REPO / "data/downstream/pattern_graph/fleets.csv")
    emb = members_set(REPO / "data/output/embed_31/fleets.csv")

    def detectors_of(p):
        d = []
        if p in conv: d.append("conv")
        if p in patt: d.append("patt")
        if p in emb: d.append("emb")
        return d

    reg = list(csv.DictReader(open(REPO / "data/downstream/consensus_registry/registry.csv")))
    # index groups by member
    groups_of = {}
    for r in reg:
        mem = r["members"].split(",")
        ag = int(r["detectors_agreeing"])
        for m in mem:
            groups_of.setdefault(m, []).append((ag, mem))

    # seeds = plates that appear in a 3-of-3 group
    seeds = set()
    for r in reg:
        if r["detectors_agreeing"] == "3":
            seeds.update(r["members"].split(","))

    data = {}
    for s in seeds:
        comp = {}
        for ag, mem in groups_of.get(s, []):
            for m in mem:
                if m == s:
                    continue
                if m not in comp or ag > comp[m]["agree"]:
                    comp[m] = {"plate": m, "detectors": detectors_of(m), "agree": ag}
        rows = sorted(comp.values(),
                      key=lambda c: (-c["agree"], -len(c["detectors"]), int(c["plate"])))[:18]
        data[s] = {"detectors": detectors_of(s), "companions": rows}

    seed_list = sorted(data, key=lambda p: (-len(data[p]["detectors"]),
                                            -len(data[p]["companions"]), int(p)))
    js = ("window.SEED_DATA = " + json.dumps(data, ensure_ascii=False) + ";\n"
          + "window.SEED_LIST = " + json.dumps(seed_list, ensure_ascii=False) + ";\n")
    (OUT / "seed_data.js").write_text(js, encoding="utf-8")
    print(f"[demo-data] seed_data.js: {len(data)} seed plates, "
          f"{sum(len(v['companions']) for v in data.values())} companion rows")


def build_mapdata():
    coords = {}
    with open(REPO / "data/downstream/camera_graph/camera_map.csv") as f:
        for r in csv.DictReader(f):
            coords[int(r["camera"])] = (float(r["x"]), float(r["y"]))
    cameras = [[x, y, c] for c, (x, y) in coords.items()]

    segs = []
    with open(REPO / "data/downstream/corridor_od/corridor_segments.csv") as f:
        for r in sorted(csv.DictReader(f), key=lambda r: -int(r["n_plates"]))[:140]:
            a, b = int(r["src"]), int(r["dst"])
            if a in coords and b in coords:
                (x0, y0), (x1, y1) = coords[a], coords[b]
                segs.append([round(x0, 3), round(y0, 3), round(x1, 3), round(y1, 3),
                             int(r["n_plates"])])

    js = ("window.MAPDATA = " + json.dumps({"cameras": cameras, "segments": segs},
                                            ensure_ascii=False) + ";\n")
    (OUT / "mapdata.js").write_text(js, encoding="utf-8")
    print(f"[demo-data] mapdata.js: {len(cameras)} cameras, {len(segs)} corridor segments")


if __name__ == "__main__":
    build_seeds()
    build_mapdata()
