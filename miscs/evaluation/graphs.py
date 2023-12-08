#!/usr/bin/env python3

import pandas as pd
import numpy as np
import re
import sys
from pathlib import Path
from typing import Any, List
from natsort import natsort_keygen
import warnings
import plot

from plot import (
    apply_aliases,
    catplot,
    column_alias,
    explode,
    sns,
    PAPER_MODE,
    plt,
    mpl,
    format,
    magnitude_formatter,
    change_width,
    apply_hatch,
    apply_hatch2,
    apply_hatch_ax,
)
from plot import ROW_ALIASES, COLUMN_ALIASES, FORMATTER

TEST_ROOT = Path(__file__).parent.resolve()
PROJECT_ROOT = TEST_ROOT.parent.parent
MEASURE_RESULTS = TEST_ROOT.joinpath("measurements")

FORMATTER.update(
    {
        "iops": magnitude_formatter(3),
        "io_throughput": magnitude_formatter(6),
        "useconds": magnitude_formatter(-6),
        "kB": magnitude_formatter(3),
        "MB": magnitude_formatter(6),
        "krps": magnitude_formatter(3, offsetstring=True),
        "mrps": magnitude_formatter(6, offsetstring=True),
    }
)

if PAPER_MODE:
    out_format = ".pdf"
else:
    out_format = ".png"

palette = sns.color_palette("pastel")
# palette = sns.color_palette("colorblind")
# palette = [palette[-1], palette[1], palette[2]]
col_base = palette[0]

# hatches = ["", "..", "//"]
hatches = ["", "..", "**"]
hatches_mpk = ["", "**"]
hatch_base = hatches[0]
barheight = 0.5
app_height = 1.8

def sort(df: pd.DataFrame, systems: List[str], col = "system") -> pd.DataFrame:
    sparse = pd.concat([ df[df[col] == n] for n in systems ])
    return pd.concat([ sparse, df ]).drop_duplicates(keep='first')

def annotate_bar_values_s(g: Any):
    for c in g.ax.containers:
        labels = [f'   {(v.get_width()):.2f}s' for v in c]
        g.ax.bar_label(c, labels=labels, label_type='edge')

def annotate_bar_values_s2_ax(ax: Any, fontsize=7, rotation=0):
    for c in ax.containers:
        labels = [f'{(v.get_height()):.2f}s' for v in c]
        ax.bar_label(c, labels=labels, label_type='edge',
                     padding=2, fontsize=fontsize, rotation=rotation)

def annotate_bar_values_s2(g: Any):
    for c in g.ax.containers:
        labels = [f'{(v.get_height()):.2f}s' for v in c]
        g.ax.bar_label(c, labels=labels, label_type='edge',
                       padding=3, fontsize=5)

def annotate_bar_values_us(g: Any, offsets=None):
    for i, c in enumerate(g.ax.containers):
        if offsets is None:
            space = [' '*3]*len(c)
        else:
            space = [' '*s for s in offsets]
        labels = [f'{s}{(v.get_width()*1000*1000):.1f}us' for s,v in zip(space, c)]
        g.ax.bar_label(c, labels=labels, label_type='edge', fontsize=7)

def images(df: pd.DataFrame, name: str, names: List[str] = []) -> Any:
    if len(names) == 0: names = [name]

    width = 3.3
    aspect = 1.2
    ##g = df.plot(x="app", y="execution_times", kind="bar")
    g = catplot(
        data=df,
        y="app",
        x="execution_times",
        kind="bar",
        ci="sd",  # show standard deviation! otherwise with_stddev_to_long_form does not work.
        height=width/aspect,
        aspect=aspect,
        palette=[col_base, col_base, palette[1], palette[2]],
    )
    g.ax.set_ylabel("Apps")
    g.ax.set_xlabel("Time (s)")
    g.ax.set_yticklabels(["AES", "GZIP", "SHA3", "NeWu"])
    # hatches = ["//", "..", "//|", "..|"]
    hatches = ["", "|", "..", "..|"]
    apply_hatch(g, patch_legend=False, hatch_list=hatches)
    ##annotate_bar_values_kB(g)
    g._legend.remove()
    ##g._legend.set_title("")
    ##sns.move_legend(g, "upper right", bbox_to_anchor=(0.77, 1.01), labelspacing=.2)
    # g.ax.set_xlim(0, 2500000)
    ##g.ax.set_xlim(0, 2800000)

    FONT_SIZE = 9
    g.ax.annotate(
        "Lower is better",
        xycoords="axes points",
        xy=(0, 0),
        xytext=(-20, -27),
        fontsize=FONT_SIZE,
        color="navy",
        weight="bold",
    )
    g.ax.annotate(
        "",
        xycoords="axes points",
        xy=(-20-15, -25),
        xytext=(-20, -25),
        fontsize=FONT_SIZE,
        arrowprops=dict(arrowstyle="-|>", color="navy"),
    )

    g.despine()
    ##format(g.ax.xaxis, "useconds")
    return g

def compute_ratio(x: pd.DataFrame) -> pd.Series:
    title = x.benchmark_title.iloc[0]
    scale = x.scale.iloc[0]
    native = x.value.iloc[0]
    if len(x.value) == 2:
        l = x.value.iloc[1]
    else:
        print(f"WARNING: found only values for {title} for {x.identifier.iloc[0]}")
        # FIXME
        import math

        l = math.nan
    if x.proportion.iloc[0] == "LIB":
        diff = l / native
        proportion = "lower is better"
    else:
        diff = native / l
        proportion = "higher is better"

    result = dict(
        title=x.title.iloc[0],
        benchmark_title=title,
        benchmark_group=x.benchmark_name,
        diff=diff,
        native=native,
        we=l,
        scale=scale,
        proportion=proportion,
    )
    return pd.Series(result, name="metrics")


CONVERSION_MAPPING = {
    "MB": 10e6,
    "KB": 10e3,
}

ALL_UNITS = "|".join(CONVERSION_MAPPING.keys())
UNIT_FINDER = re.compile(r"(\d+)\s*({})".format(ALL_UNITS), re.IGNORECASE)


def unit_replacer(matchobj: re.Match) -> str:
    """Given a regex match object, return a replacement string where units are modified"""
    number = matchobj.group(1)
    unit = matchobj.group(2)
    new_number = int(number) * CONVERSION_MAPPING[unit]
    return f"{new_number} B"


def sort_row(val: pd.Series) -> Any:
    return natsort_keygen()(val.apply(lambda v: UNIT_FINDER.sub(unit_replacer, v)))


def bar_colors_rainbow(graph: Any, df: pd.Series, num_colors: int) -> None:
    colors = sns.color_palette(n_colors=num_colors)
    groups = 0
    last_group = df[0].iloc[0]
    for i, patch in enumerate(graph.axes[0][0].patches):
        if last_group != df[i].iloc[0]:
            last_group = df[i].iloc[0]
            groups += 1
        patch.set_facecolor(colors[groups])


def bar_colors(graph: Any, colors) -> None:
    for i, patch in enumerate(graph.axes[0][0].patches):
        patch.set_facecolor(colors[i%len(colors)])


def sort_baseline_first(df: pd.DataFrame, baseline_system: str) -> pd.DataFrame:
    beginning = df[df["system"] == baseline_system]
    end = df[df["system"] != baseline_system]
    return pd.concat([beginning, end])

def main() -> None:

    if len(sys.argv) < 2:
        print(f"USAGE: {sys.argv[0]} graph.tsv...")
    for arg in sys.argv[1:]:
        tsv_path = Path(arg)
        df = pd.read_csv(tsv_path, sep="\t")
        assert isinstance(df, pd.DataFrame)
        name = tsv_path.stem

        if name.startswith("all_apps"):
            graph = images(df, "execution_time")
        else:
            print(f"unhandled graph name: {tsv_path}", file=sys.stderr)
            sys.exit(1)

        fname = "figure"
        print(f"write {fname}")
        graph.savefig(MEASURE_RESULTS / fname, bbox_inches='tight')

if __name__ == "__main__":
    # main_old()
    main()
