import argparse
import pandas as pd


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


if __name__ == "__main__":
    main()
