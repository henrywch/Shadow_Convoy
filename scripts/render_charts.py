"""Render all result charts as ECharts → PNG (for 报告.docx) and emit the same
option JSON as demo/js/charts.js (for the live demo). Single source of truth so
the report figures and the demo always agree with the actual run results.

Charts use fitting types per data shape: bar / horizontal-bar / funnel / donut /
radar / nightingale-rose. PNGs render via headless Chrome over a local ECharts
(demo/js/echarts.min.js), at 2× device scale for crisp print.

Usage:  python scripts/render_charts.py [--png] [--js]   (default: both)
"""
import json
import subprocess
import sys
import tempfile
from pathlib import Path

REPO = Path("/inspire/hdd/project/video-understanding/public/personal/chwang/live/cpjs/BD_Hadoop")
ECHARTS = REPO / "demo/js/echarts.min.js"
OUT_PNG = REPO / "doc/figures/charts"
PAL = ["#5B8FF9", "#61DDAA", "#F6BD16", "#7262FD", "#78D3F8", "#F08BB4", "#FF9845", "#5AD8A6"]
AXIS = {"axisLine": {"lineStyle": {"color": "#888"}}, "axisLabel": {"color": "#333", "fontSize": 13},
        "nameTextStyle": {"color": "#333"}}
TITLE = lambda t, s="": {"text": t, "subtext": s, "left": "center", "top": 8,
                          "textStyle": {"fontSize": 18, "color": "#222"},
                          "subtextStyle": {"fontSize": 12, "color": "#888"}}


def hbar(title, sub, cats, vals, color, fmt=""):
    return {"title": TITLE(title, sub), "backgroundColor": "#fff",
            "grid": {"left": 150, "right": 70, "top": 70, "bottom": 30},
            "xAxis": {"type": "value", **AXIS}, "yAxis": {"type": "category", "data": cats, "inverse": True, **AXIS},
            "series": [{"type": "bar", "data": vals, "itemStyle": {"color": color, "borderRadius": [0, 4, 4, 0]},
                        "label": {"show": True, "position": "right", "color": "#333", "formatter": fmt or "{c}"}}]}


CHARTS = {
 # ── FP-Growth ───────────────────────────────────────────────────────────
 "fpg_size": {"title": TITLE("FP-Growth：各规模车队数量", "k=2…6，共 5,447 个车队"), "backgroundColor": "#fff",
   "grid": {"left": 70, "right": 40, "top": 70, "bottom": 45},
   "xAxis": {"type": "category", "data": ["2 车", "3 车", "4 车", "5 车", "6 车"], "name": "车队规模 k", **AXIS},
   "yAxis": {"type": "value", "name": "车队数", **AXIS},
   "series": [{"type": "bar", "data": [1369, 730, 1054, 1254, 1040], "barWidth": "55%",
               "itemStyle": {"color": PAL[0], "borderRadius": [4, 4, 0, 0]},
               "label": {"show": True, "position": "top", "color": "#333"}}]},
 "fpg_super": hbar("FP-Growth：超级连接点车牌", "出现在最多车队中的车牌（前 10）",
   ["3271142", "16972992", "16970573", "511670", "3542309", "18150551", "18139166", "18151225", "18163383", "5703388"],
   [1548, 1548, 1548, 1548, 1533, 1495, 1190, 1164, 1164, 1100], PAL[5]),
 # ── MaxGrowth ───────────────────────────────────────────────────────────
 "mg_routelen": {"title": TITLE("MaxGrowth：路径长度分布", "223,184 个模式，按相机数"), "backgroundColor": "#fff",
   "series": [{"type": "funnel", "left": "8%", "right": "8%", "top": 70, "bottom": 20, "minSize": "16%",
               "sort": "descending", "gap": 2, "label": {"position": "inside", "color": "#fff", "formatter": "{b}: {c}"},
               "data": [{"value": 193814, "name": "3 相机"}, {"value": 25677, "name": "4 相机"},
                        {"value": 3264, "name": "5 相机"}, {"value": 382, "name": "6 相机"},
                        {"value": 39, "name": "7 相机"}, {"value": 7, "name": "8 相机"}, {"value": 1, "name": "9 相机"}],
               "color": PAL}]},
 "mg_corridors": hbar("MaxGrowth：最繁忙同行走廊", "按经过的不同车牌数（前 8 条有向路径）",
   ["434→404→238", "68→181→45", "391→375→372", "268→478→481", "297→278→437", "278→437→308", "11→227→557", "307→442→515"],
   [9296, 6773, 5838, 5436, 3534, 3305, 3187, 3120], PAL[2]),
 # ── Embedding ───────────────────────────────────────────────────────────
 "emb_confirm": {"title": TITLE("Embedding：聚类共现确认占比", "12,708 个簇"), "backgroundColor": "#fff",
   "tooltip": {"trigger": "item"}, "legend": {"bottom": 8, "textStyle": {"color": "#333"}},
   "series": [{"type": "pie", "radius": ["42%", "68%"], "center": ["50%", "52%"], "avoidLabelOverlap": True,
               "label": {"formatter": "{b}\n{c} ({d}%)", "color": "#333"},
               "data": [{"value": 4041, "name": "已确认（真同行）", "itemStyle": {"color": PAL[1]}},
                        {"value": 8667, "name": "未确认（仅路径相似）", "itemStyle": {"color": "#d7dce3"}}]}]},
 "emb_sizes": hbar("Embedding：最大的已确认车队", "按车牌数（前 8 个已确认簇）",
   ["#11981", "#11478", "#12449", "#11758", "#12556", "#11048", "#11985", "#11031"],
   [759, 685, 622, 598, 551, 444, 395, 395], PAL[6]),
 # ── cross-cutting ───────────────────────────────────────────────────────
 "detector_radar": {"title": TITLE("三检测器定性对比", "0–5 分，越大越强"), "backgroundColor": "#fff",
   "legend": {"data": ["FP-Growth", "MaxGrowth", "Embedding"], "top": 44, "right": 16,
              "orient": "vertical", "textStyle": {"color": "#333"}},
   "radar": {"center": ["50%", "56%"], "radius": "58%",
             "indicator": [{"name": "车牌覆盖", "max": 5}, {"name": "大群体", "max": 5}, {"name": "有向性", "max": 5},
                           {"name": "精度倾向", "max": 5}, {"name": "可扩展性", "max": 5}],
             "axisName": {"color": "#333"}},
   "series": [{"type": "radar", "areaStyle": {"opacity": 0.12}, "data": [
       {"value": [1, 2, 0, 5, 4], "name": "FP-Growth", "itemStyle": {"color": PAL[0]}},
       {"value": [4, 3, 5, 3, 3], "name": "MaxGrowth", "itemStyle": {"color": PAL[2]}},
       {"value": [5, 4, 2, 2, 5], "name": "Embedding", "itemStyle": {"color": PAL[1]}}]}]},
 "consensus_agree": {"title": TITLE("共识融合登记表", "1,167 个融合组的一致性"), "backgroundColor": "#fff",
   "legend": {"bottom": 8, "textStyle": {"color": "#333"}}, "tooltip": {"trigger": "item"},
   "series": [{"type": "pie", "radius": ["40%", "66%"], "center": ["50%", "52%"],
               "label": {"formatter": "{b}\n{c}", "color": "#333"},
               "data": [{"value": 19, "name": "三检测器一致 (3-of-3)", "itemStyle": {"color": PAL[3]}},
                        {"value": 1148, "name": "两检测器一致 (2-of-3)", "itemStyle": {"color": PAL[4]}}]}]},
 "clone_rose": {"title": TITLE("套牌车候选（不可能转移次数）", "全月 31 天，前 8 名"), "backgroundColor": "#fff",
   "tooltip": {"trigger": "item"}, "legend": {"type": "scroll", "bottom": 6, "textStyle": {"color": "#333"}},
   "series": [{"type": "pie", "roseType": "area", "radius": ["18%", "72%"], "center": ["50%", "52%"],
               "label": {"color": "#333", "formatter": "{b}\n{c}"},
               "data": [{"value": 108, "name": "505029"}, {"value": 47, "name": "181657"}, {"value": 46, "name": "510330"},
                        {"value": 44, "name": "381500"}, {"value": 40, "name": "34823"}, {"value": 36, "name": "2323255"},
                        {"value": 36, "name": "618124"}, {"value": 26, "name": "1153396"}],
               "itemStyle": {"borderRadius": 4}}]},
 "od_pairs": hbar("走廊 OD 流：最繁忙的群体起讫对", "按经过的不同车牌数（前 8 个 OD 对）",
   ["434→238", "68→45", "391→372", "268→481", "434→29", "330→481", "297→437", "348→45"],
   [9300, 6775, 5859, 5442, 4493, 4005, 3536, 3523], PAL[7]),
}

SIZES = {"detector_radar": (760, 560), "emb_confirm": (760, 520), "consensus_agree": (760, 520),
         "clone_rose": (760, 560), "mg_routelen": (760, 520)}


def render_png(name, option):
    w, h = SIZES.get(name, (820, 480))
    html = f"""<!doctype html><html><head><meta charset="utf-8">
<script src="file://{ECHARTS}"></script>
<style>html,body{{margin:0;background:#fff}}#c{{width:{w}px;height:{h}px}}</style></head>
<body><div id="c"></div><script>
var ch=echarts.init(document.getElementById('c'),null,{{renderer:'canvas'}});
ch.setOption({json.dumps(option, ensure_ascii=False)});
</script></body></html>"""
    with tempfile.NamedTemporaryFile("w", suffix=".html", delete=False, encoding="utf-8") as f:
        f.write(html); path = f.name
    OUT_PNG.mkdir(parents=True, exist_ok=True)
    out = OUT_PNG / f"{name}.png"
    with tempfile.TemporaryDirectory() as ud:
        subprocess.run(["google-chrome", "--headless", "--disable-gpu", "--no-sandbox",
                        "--hide-scrollbars", f"--user-data-dir={ud}", "--force-device-scale-factor=2",
                        f"--screenshot={out}", f"--window-size={w},{h}",
                        "--virtual-time-budget=5000", f"file://{path}"],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=90)
    Path(path).unlink(missing_ok=True)
    print(f"[charts] {name}.png  ({out.stat().st_size//1024} KB)")


if __name__ == "__main__":
    do_png = "--js" not in sys.argv or "--png" in sys.argv
    do_js = "--png" not in sys.argv or "--js" in sys.argv
    if do_png:
        for name, opt in CHARTS.items():
            render_png(name, opt)
    if do_js:
        js = "window.CHARTS = " + json.dumps(CHARTS, ensure_ascii=False, indent=1) + ";\n"
        (REPO / "demo/js/charts.js").write_text(js, encoding="utf-8")
        print(f"[charts] wrote demo/js/charts.js ({len(CHARTS)} charts)")
