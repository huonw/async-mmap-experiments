import argparse
import pandas as pd
import plotly.express as px
import plotly.io as pio


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
    plot(df_labelled.query("cold_cache == False"))
    plot(df_labelled.query("cold_cache == True"))


def plot(df_plotting):
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
            "async_name": ["Async, concurrent", "Sync, 1 thread", "Sync, 8 threads"]
        },
        labels={
            "duration": "Duration (s)",
            "async_name": "Configuration",
            "io_name": "I/O type",
        },
        stripmode="overlay",
    )
    # Call out the minimums with text
    minimums = (
        df_plotting.groupby(["async_name", "io_name"])["duration"].min().reset_index()
    )
    for row in minimums.itertuples():
        fig.add_annotation(
            x=row.duration,
            y=row.async_name,
            text=(
                f"{row.duration:#.3}s"
                if row.duration > 0.1
                else f"{row.duration * 1000:#.3}ms"
            ),
            xanchor="right",
            yanchor="bottom",
            showarrow=True,
            arrowcolor=color_map[row.io_name],
            arrowwidth=1,
            ax=0,
            ay=(50 if row.io_name == "Memory-mapped IO" else -30),
            arrowside="none",
            standoff=4,
            font=dict(size=18),
            bordercolor=color_map[row.io_name],
            borderpad=2,
            borderwidth=2,
        )
    print(df_plotting["duration"].max())
    fig.update_traces(marker=dict(size=8, opacity=0.8), hovertemplate="%{x:.3}s")
    fig.update_layout(
        # The y-axis label is not needed, just go off the tick labels
        yaxis=dict(title=""),
        # always start from 0
        xaxis=dict(range=[0, df_plotting["duration"].max() * 1.01]),
        # limited space, so put the legend along the top
        legend=dict(
            title="", orientation="h", yanchor="bottom", y=1.0, xanchor="left", x=0
        ),
        margin=dict(t=20, b=20, l=0, r=0),
        font=dict(
            # same as blog
            family="system-ui, -apple-system,'Segoe UI', Roboto, Helvetica, Arial, sans-serif, 'Apple Color Emoji', 'Segoe UI Emoji'",
            size=18,
        ),
        hovermode="x",
        template=pio.templates["seaborn"],
    )
    # TODO: export the zlib-compressed results as a blog tag
    fig.show()


if __name__ == "__main__":
    main()
