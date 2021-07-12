"""
Script that plots benchmark data-visualizations.
"""
from plotnine.themes.theme_gray import theme_gray
from plotnine.themes.theme import theme
from plotnine.themes.elements import (element_line, element_rect,
                                      element_text, element_blank)
import sys
import pandas as pd
import numpy as np
import plotnine as p9

from plotnine import *
from plotnine.data import *

import warnings

TM = ["Sequential", "Interleave"]
RS = ["One", "Socket", "L1"]
BS = [1, 8]


class theme_my538(theme_gray):
    def __init__(self, base_size=11, base_family='DejaVu Sans'):
        theme_gray.__init__(self, base_size, base_family)
        bgcolor = '#FFFFFF'
        self.add_theme(
            theme(
                axis_text=element_text(size=base_size+3),
                axis_ticks=element_blank(),
                title=element_text(color='#3C3C3C'),
                legend_background=element_rect(fill='None'),
                legend_key=element_rect(fill='#FFFFFF', colour=None),
                panel_background=element_rect(fill=bgcolor),
                panel_border=element_blank(),
                panel_grid_major=element_line(
                    color='#D5D5D5', linetype='solid', size=1),
                panel_grid_minor=element_blank(),
                plot_background=element_rect(
                    fill=bgcolor, color=bgcolor, size=1),
                strip_background=element_rect(size=0)),
            inplace=True)


def plot_benchmark_throughputs(df):
    "Plots a throughput graph for every data-structure in the results file"
    # Fail if we have more than one experiment duration

    data_set = []
    for name in df.name.unique():
        benchmark = df.loc[df['name'] == name]
        benchmark = benchmark.groupby(['name', 'rs', 'tm', 'batch_size', 'threads', 'duration'], as_index=False).agg(
            {'exp_time_in_sec': 'max', 'iterations': 'sum'})
        benchmark['throughput'] = benchmark['iterations'] / \
            benchmark['exp_time_in_sec']
        benchmark['configuration'] = benchmark.apply(
            lambda row: "Bench={} RS={} TM={} BS={}".format(name, row.rs, row.tm, row.batch_size), axis=1)
        data_set.append(benchmark)

    benchmarks = pd.concat(data_set)
    p = ggplot(data=benchmarks, mapping=aes(x='threads', y='throughput', color='configuration')) + \
        theme_my538(base_size=8) + \
        labs(y="Throughput [Melems/s]") + \
        theme(legend_position='top', legend_title=element_blank()) + \
        scale_x_continuous(breaks=benchmarks['threads'].unique(), labels=["{}".format(thr) for thr in benchmarks['threads'].unique()], name='# Threads') + \
        scale_y_continuous(labels=lambda lst: ["{:,}".format(x / 1_000_000) for x in lst]) + \
        geom_point() + \
        geom_line()

    p.save("throughput-log-append.png", dpi=300, width=12, height=5)
    p.save("throughput-log-append.pdf", dpi=300, width=12, height=5)


if __name__ == '__main__':
    warnings.filterwarnings('ignore')
    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)
    pd.set_option('display.expand_frame_repr', True)

    if len(sys.argv) != 2:
        print("Usage: Give path to CSV results file as first argument")
        sys.exit(1)

    df = pd.read_csv(sys.argv[1], skip_blank_lines=True)
    plot_benchmark_throughputs(df)
