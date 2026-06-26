window.CHARTS = {
 "fpg_size": {
  "title": {
   "text": "FP-Growth：各规模车队数量",
   "subtext": "k=2…6，共 5,447 个车队",
   "left": "center",
   "top": 8,
   "textStyle": {
    "fontSize": 18,
    "color": "#222"
   },
   "subtextStyle": {
    "fontSize": 12,
    "color": "#888"
   }
  },
  "backgroundColor": "#fff",
  "grid": {
   "left": 70,
   "right": 40,
   "top": 70,
   "bottom": 45
  },
  "xAxis": {
   "type": "category",
   "data": [
    "2 车",
    "3 车",
    "4 车",
    "5 车",
    "6 车"
   ],
   "name": "车队规模 k",
   "axisLine": {
    "lineStyle": {
     "color": "#888"
    }
   },
   "axisLabel": {
    "color": "#333",
    "fontSize": 13
   },
   "nameTextStyle": {
    "color": "#333"
   }
  },
  "yAxis": {
   "type": "value",
   "name": "车队数",
   "axisLine": {
    "lineStyle": {
     "color": "#888"
    }
   },
   "axisLabel": {
    "color": "#333",
    "fontSize": 13
   },
   "nameTextStyle": {
    "color": "#333"
   }
  },
  "series": [
   {
    "type": "bar",
    "data": [
     1369,
     730,
     1054,
     1254,
     1040
    ],
    "barWidth": "55%",
    "itemStyle": {
     "color": "#5B8FF9",
     "borderRadius": [
      4,
      4,
      0,
      0
     ]
    },
    "label": {
     "show": true,
     "position": "top",
     "color": "#333"
    }
   }
  ]
 },
 "fpg_super": {
  "title": {
   "text": "FP-Growth：超级连接点车牌",
   "subtext": "出现在最多车队中的车牌（前 10）",
   "left": "center",
   "top": 8,
   "textStyle": {
    "fontSize": 18,
    "color": "#222"
   },
   "subtextStyle": {
    "fontSize": 12,
    "color": "#888"
   }
  },
  "backgroundColor": "#fff",
  "grid": {
   "left": 150,
   "right": 70,
   "top": 70,
   "bottom": 30
  },
  "xAxis": {
   "type": "value",
   "axisLine": {
    "lineStyle": {
     "color": "#888"
    }
   },
   "axisLabel": {
    "color": "#333",
    "fontSize": 13
   },
   "nameTextStyle": {
    "color": "#333"
   }
  },
  "yAxis": {
   "type": "category",
   "data": [
    "3271142",
    "16972992",
    "16970573",
    "511670",
    "3542309",
    "18150551",
    "18139166",
    "18151225",
    "18163383",
    "5703388"
   ],
   "inverse": true,
   "axisLine": {
    "lineStyle": {
     "color": "#888"
    }
   },
   "axisLabel": {
    "color": "#333",
    "fontSize": 13
   },
   "nameTextStyle": {
    "color": "#333"
   }
  },
  "series": [
   {
    "type": "bar",
    "data": [
     1548,
     1548,
     1548,
     1548,
     1533,
     1495,
     1190,
     1164,
     1164,
     1100
    ],
    "itemStyle": {
     "color": "#F08BB4",
     "borderRadius": [
      0,
      4,
      4,
      0
     ]
    },
    "label": {
     "show": true,
     "position": "right",
     "color": "#333",
     "formatter": "{c}"
    }
   }
  ]
 },
 "mg_routelen": {
  "title": {
   "text": "MaxGrowth：路径长度分布",
   "subtext": "223,184 个模式，按相机数",
   "left": "center",
   "top": 8,
   "textStyle": {
    "fontSize": 18,
    "color": "#222"
   },
   "subtextStyle": {
    "fontSize": 12,
    "color": "#888"
   }
  },
  "backgroundColor": "#fff",
  "series": [
   {
    "type": "funnel",
    "left": "8%",
    "right": "8%",
    "top": 70,
    "bottom": 20,
    "minSize": "16%",
    "sort": "descending",
    "gap": 2,
    "label": {
     "position": "inside",
     "color": "#fff",
     "formatter": "{b}: {c}"
    },
    "data": [
     {
      "value": 193814,
      "name": "3 相机"
     },
     {
      "value": 25677,
      "name": "4 相机"
     },
     {
      "value": 3264,
      "name": "5 相机"
     },
     {
      "value": 382,
      "name": "6 相机"
     },
     {
      "value": 39,
      "name": "7 相机"
     },
     {
      "value": 7,
      "name": "8 相机"
     },
     {
      "value": 1,
      "name": "9 相机"
     }
    ],
    "color": [
     "#5B8FF9",
     "#61DDAA",
     "#F6BD16",
     "#7262FD",
     "#78D3F8",
     "#F08BB4",
     "#FF9845",
     "#5AD8A6"
    ]
   }
  ]
 },
 "mg_corridors": {
  "title": {
   "text": "MaxGrowth：最繁忙同行走廊",
   "subtext": "按经过的不同车牌数（前 8 条有向路径）",
   "left": "center",
   "top": 8,
   "textStyle": {
    "fontSize": 18,
    "color": "#222"
   },
   "subtextStyle": {
    "fontSize": 12,
    "color": "#888"
   }
  },
  "backgroundColor": "#fff",
  "grid": {
   "left": 150,
   "right": 70,
   "top": 70,
   "bottom": 30
  },
  "xAxis": {
   "type": "value",
   "axisLine": {
    "lineStyle": {
     "color": "#888"
    }
   },
   "axisLabel": {
    "color": "#333",
    "fontSize": 13
   },
   "nameTextStyle": {
    "color": "#333"
   }
  },
  "yAxis": {
   "type": "category",
   "data": [
    "434→404→238",
    "68→181→45",
    "391→375→372",
    "268→478→481",
    "297→278→437",
    "278→437→308",
    "11→227→557",
    "307→442→515"
   ],
   "inverse": true,
   "axisLine": {
    "lineStyle": {
     "color": "#888"
    }
   },
   "axisLabel": {
    "color": "#333",
    "fontSize": 13
   },
   "nameTextStyle": {
    "color": "#333"
   }
  },
  "series": [
   {
    "type": "bar",
    "data": [
     9296,
     6773,
     5838,
     5436,
     3534,
     3305,
     3187,
     3120
    ],
    "itemStyle": {
     "color": "#F6BD16",
     "borderRadius": [
      0,
      4,
      4,
      0
     ]
    },
    "label": {
     "show": true,
     "position": "right",
     "color": "#333",
     "formatter": "{c}"
    }
   }
  ]
 },
 "emb_confirm": {
  "title": {
   "text": "Embedding：聚类共现确认占比",
   "subtext": "12,708 个簇",
   "left": "center",
   "top": 8,
   "textStyle": {
    "fontSize": 18,
    "color": "#222"
   },
   "subtextStyle": {
    "fontSize": 12,
    "color": "#888"
   }
  },
  "backgroundColor": "#fff",
  "tooltip": {
   "trigger": "item"
  },
  "legend": {
   "bottom": 8,
   "textStyle": {
    "color": "#333"
   }
  },
  "series": [
   {
    "type": "pie",
    "radius": [
     "42%",
     "68%"
    ],
    "center": [
     "50%",
     "52%"
    ],
    "avoidLabelOverlap": true,
    "label": {
     "formatter": "{b}\n{c} ({d}%)",
     "color": "#333"
    },
    "data": [
     {
      "value": 4041,
      "name": "已确认（真同行）",
      "itemStyle": {
       "color": "#61DDAA"
      }
     },
     {
      "value": 8667,
      "name": "未确认（仅路径相似）",
      "itemStyle": {
       "color": "#d7dce3"
      }
     }
    ]
   }
  ]
 },
 "emb_sizes": {
  "title": {
   "text": "Embedding：最大的已确认车队",
   "subtext": "按车牌数（前 8 个已确认簇）",
   "left": "center",
   "top": 8,
   "textStyle": {
    "fontSize": 18,
    "color": "#222"
   },
   "subtextStyle": {
    "fontSize": 12,
    "color": "#888"
   }
  },
  "backgroundColor": "#fff",
  "grid": {
   "left": 150,
   "right": 70,
   "top": 70,
   "bottom": 30
  },
  "xAxis": {
   "type": "value",
   "axisLine": {
    "lineStyle": {
     "color": "#888"
    }
   },
   "axisLabel": {
    "color": "#333",
    "fontSize": 13
   },
   "nameTextStyle": {
    "color": "#333"
   }
  },
  "yAxis": {
   "type": "category",
   "data": [
    "#11981",
    "#11478",
    "#12449",
    "#11758",
    "#12556",
    "#11048",
    "#11985",
    "#11031"
   ],
   "inverse": true,
   "axisLine": {
    "lineStyle": {
     "color": "#888"
    }
   },
   "axisLabel": {
    "color": "#333",
    "fontSize": 13
   },
   "nameTextStyle": {
    "color": "#333"
   }
  },
  "series": [
   {
    "type": "bar",
    "data": [
     759,
     685,
     622,
     598,
     551,
     444,
     395,
     395
    ],
    "itemStyle": {
     "color": "#FF9845",
     "borderRadius": [
      0,
      4,
      4,
      0
     ]
    },
    "label": {
     "show": true,
     "position": "right",
     "color": "#333",
     "formatter": "{c}"
    }
   }
  ]
 },
 "detector_radar": {
  "title": {
   "text": "三检测器定性对比",
   "subtext": "0–5 分，越大越强",
   "left": "center",
   "top": 8,
   "textStyle": {
    "fontSize": 18,
    "color": "#222"
   },
   "subtextStyle": {
    "fontSize": 12,
    "color": "#888"
   }
  },
  "backgroundColor": "#fff",
  "legend": {
   "data": [
    "FP-Growth",
    "MaxGrowth",
    "Embedding"
   ],
   "top": 44,
   "right": 16,
   "orient": "vertical",
   "textStyle": {
    "color": "#333"
   }
  },
  "radar": {
   "center": [
    "50%",
    "56%"
   ],
   "radius": "58%",
   "indicator": [
    {
     "name": "车牌覆盖",
     "max": 5
    },
    {
     "name": "大群体",
     "max": 5
    },
    {
     "name": "有向性",
     "max": 5
    },
    {
     "name": "精度倾向",
     "max": 5
    },
    {
     "name": "可扩展性",
     "max": 5
    }
   ],
   "axisName": {
    "color": "#333"
   }
  },
  "series": [
   {
    "type": "radar",
    "areaStyle": {
     "opacity": 0.12
    },
    "data": [
     {
      "value": [
       1,
       2,
       0,
       5,
       4
      ],
      "name": "FP-Growth",
      "itemStyle": {
       "color": "#5B8FF9"
      }
     },
     {
      "value": [
       4,
       3,
       5,
       3,
       3
      ],
      "name": "MaxGrowth",
      "itemStyle": {
       "color": "#F6BD16"
      }
     },
     {
      "value": [
       5,
       4,
       2,
       2,
       5
      ],
      "name": "Embedding",
      "itemStyle": {
       "color": "#61DDAA"
      }
     }
    ]
   }
  ]
 },
 "consensus_agree": {
  "title": {
   "text": "共识融合登记表",
   "subtext": "1,167 个融合组的一致性",
   "left": "center",
   "top": 8,
   "textStyle": {
    "fontSize": 18,
    "color": "#222"
   },
   "subtextStyle": {
    "fontSize": 12,
    "color": "#888"
   }
  },
  "backgroundColor": "#fff",
  "legend": {
   "bottom": 8,
   "textStyle": {
    "color": "#333"
   }
  },
  "tooltip": {
   "trigger": "item"
  },
  "series": [
   {
    "type": "pie",
    "radius": [
     "40%",
     "66%"
    ],
    "center": [
     "50%",
     "52%"
    ],
    "label": {
     "formatter": "{b}\n{c}",
     "color": "#333"
    },
    "data": [
     {
      "value": 19,
      "name": "三检测器一致 (3-of-3)",
      "itemStyle": {
       "color": "#7262FD"
      }
     },
     {
      "value": 1148,
      "name": "两检测器一致 (2-of-3)",
      "itemStyle": {
       "color": "#78D3F8"
      }
     }
    ]
   }
  ]
 },
 "clone_rose": {
  "title": {
   "text": "套牌车候选（不可能转移次数）",
   "subtext": "全月 31 天，前 8 名",
   "left": "center",
   "top": 8,
   "textStyle": {
    "fontSize": 18,
    "color": "#222"
   },
   "subtextStyle": {
    "fontSize": 12,
    "color": "#888"
   }
  },
  "backgroundColor": "#fff",
  "tooltip": {
   "trigger": "item"
  },
  "legend": {
   "type": "scroll",
   "bottom": 6,
   "textStyle": {
    "color": "#333"
   }
  },
  "series": [
   {
    "type": "pie",
    "roseType": "area",
    "radius": [
     "18%",
     "72%"
    ],
    "center": [
     "50%",
     "52%"
    ],
    "label": {
     "color": "#333",
     "formatter": "{b}\n{c}"
    },
    "data": [
     {
      "value": 108,
      "name": "505029"
     },
     {
      "value": 47,
      "name": "181657"
     },
     {
      "value": 46,
      "name": "510330"
     },
     {
      "value": 44,
      "name": "381500"
     },
     {
      "value": 40,
      "name": "34823"
     },
     {
      "value": 36,
      "name": "2323255"
     },
     {
      "value": 36,
      "name": "618124"
     },
     {
      "value": 26,
      "name": "1153396"
     }
    ],
    "itemStyle": {
     "borderRadius": 4
    }
   }
  ]
 },
 "od_pairs": {
  "title": {
   "text": "走廊 OD 流：最繁忙的群体起讫对",
   "subtext": "按经过的不同车牌数（前 8 个 OD 对）",
   "left": "center",
   "top": 8,
   "textStyle": {
    "fontSize": 18,
    "color": "#222"
   },
   "subtextStyle": {
    "fontSize": 12,
    "color": "#888"
   }
  },
  "backgroundColor": "#fff",
  "grid": {
   "left": 150,
   "right": 70,
   "top": 70,
   "bottom": 30
  },
  "xAxis": {
   "type": "value",
   "axisLine": {
    "lineStyle": {
     "color": "#888"
    }
   },
   "axisLabel": {
    "color": "#333",
    "fontSize": 13
   },
   "nameTextStyle": {
    "color": "#333"
   }
  },
  "yAxis": {
   "type": "category",
   "data": [
    "434→238",
    "68→45",
    "391→372",
    "268→481",
    "434→29",
    "330→481",
    "297→437",
    "348→45"
   ],
   "inverse": true,
   "axisLine": {
    "lineStyle": {
     "color": "#888"
    }
   },
   "axisLabel": {
    "color": "#333",
    "fontSize": 13
   },
   "nameTextStyle": {
    "color": "#333"
   }
  },
  "series": [
   {
    "type": "bar",
    "data": [
     9300,
     6775,
     5859,
     5442,
     4493,
     4005,
     3536,
     3523
    ],
    "itemStyle": {
     "color": "#5AD8A6",
     "borderRadius": [
      0,
      4,
      4,
      0
     ]
    },
    "label": {
     "show": true,
     "position": "right",
     "color": "#333",
     "formatter": "{c}"
    }
   }
  ]
 }
};
