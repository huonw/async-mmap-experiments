import base64
import json
import zlib
import gzip

import argparse
import numpy as np
import pandas as pd
import plotly
import plotly.express as px
import plotly.io as pio


def dump_json(value):
    return json.dumps(value, separators=(",", ":"), cls=plotly.utils.PlotlyJSONEncoder)


def compress_json(value, compress="zlib"):
    jsonified = dump_json(value)
    bytified = jsonified.encode()
    if compress == "gzip":
        bytified = gzip.compress(bytified)
    elif compress == "zlib":
        bytified = zlib.compress(bytified)
    b64 = base64.b64encode(bytified).decode()
    return b64


def format_tag(
    figure, height=300, caption="caption here", optimise=True, compress="zlib"
):
    dictified = figure.to_plotly_json()
    assert isinstance(dictified, dict)
    assert dictified.keys() == {"data", "layout"}

    if optimise:
        # set globally
        del dictified["layout"]["template"]
    b64 = compress_json(dictified, compress=compress)
    return f"""{{% include plotly.html height="{height}px" caption="{caption}" data="{b64}" %}}"""


def configuration_labels():
    async_labels = pd.DataFrame(
        [
            {
                "use_async": True,
                "use_parallel": True,
                "async_name": "Async, concurrent",
            },
            {"use_async": False, "use_parallel": True, "async_name": "Sync, 8 threads"},
            {"use_async": False, "use_parallel": False, "async_name": "Sync, 1 thread"},
        ]
    )
    io_type = pd.DataFrame(
        [
            {"use_mmap": True, "io_name": "Memory-mapped IO"},
            {"use_mmap": False, "io_name": "Conventional IO"},
        ]
    )

    return async_labels.join(io_type, how="cross").set_index(
        ["use_async", "use_parallel", "use_mmap"]
    )


def main():
    parser = argparse.ArgumentParser(description="Summarise the content of CSV")
    parser.add_argument(
        "filename", type=argparse.FileType("r"), help="The CSV file to summarise"
    )
    args = parser.parse_args()

    # example contents:
    # use_async,use_mmap,use_parallel,cold_cache,repeat,duration
    # true,true,true,true,0,1.595
    # true,true,true,false,0,0.069
    df = pd.read_csv(args.filename)

    aggregated = df.groupby(["cold_cache", "use_async", "use_parallel", "use_mmap"])[
        "duration"
    ].agg(["min", "median", "max", "mean", "std"])

    print(aggregated)

    # Augment the data with human-readable labels for plotting
    df_labelled = df.join(
        configuration_labels(),
        on=["use_async", "use_parallel", "use_mmap"],
        how="inner",
    )
    plot(df_labelled.query("cold_cache == False"), caption="Warm cache")
    plot(df_labelled.query("cold_cache == True"), caption="Cold cache")


def plot(df_plotting, caption):
    colors = px.colors.qualitative.Plotly
    color_map = {
        "Memory-mapped IO": colors[0],
        "Conventional IO": colors[1],
    }

    fig = px.strip(
        df_plotting,
        x="duration",
        y="async_name",
        color="io_name",
        color_discrete_map=color_map,
        category_orders={
            "async_name": ["Async, concurrent", "Sync, 1 thread", "Sync, 8 threads"],
            "io_name": ["Conventional IO", "Memory-mapped IO"],
        },
        labels={
            "duration": "Duration (s)",
            "async_name": "Configuration",
            "io_name": "I/O type",
        },
        stripmode="overlay",
    )
    print(df_plotting["duration"].max())
    fig.update_traces(
        marker=dict(size=8, opacity=0.5),
        hovertemplate="%{x:.4}s",
        jitter=1,
    )

    # same as blog
    font_family = "system-ui, -apple-system,'Segoe UI', Roboto, Helvetica, Arial, sans-serif, 'Apple Color Emoji', 'Segoe UI Emoji'"
    fig.update_layout(
        # The y-axis label is not needed, just go off the tick labels
        yaxis=dict(title=""),
        xaxis=dict(
            # always start from 0
            rangemode="tozero",
            title=dict(font=dict(family=font_family, size=14)),
        ),
        # limited space, so put the legend along the top
        legend=dict(
            title="", orientation="h", yanchor="bottom", y=1.0, xanchor="left", x=0
        ),
        margin=dict(t=20, b=20, l=0, r=0),
        font=dict(family=font_family, size=14),
        hoverlabel=dict(font_family=font_family),
        template=pio.templates["seaborn"],
    )
    # TODO: export the zlib-compressed results as a blog tag
    fig.show()
    print(format_tag(fig, caption=caption))


if __name__ == "__main__":
    main()
