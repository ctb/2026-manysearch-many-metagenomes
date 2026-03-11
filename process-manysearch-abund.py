#! /usr/bin/env python
import polars as pl
import argparse
import sys
import glob

extract_species_from_name = (pl.col("query_name")
                             .str.split(' ')
                             .list.slice(1, 2)
                             .list.join(' '))


def main():
    p = argparse.ArgumentParser()
    p.add_argument('csv_dir', help='directory full of manysearch CSVs w/abund')
    p.add_argument('-o', '--output-parquet',
                   required=True,
                   help='output parquet file')
    p.add_argument('-t', '--threshold-bp', default=3_000,
                   help='minimum threshold for overlap (default: 3_000)')
    args = p.parse_args()

    csv_dir = args.csv_dir.rstrip('/')

    filenames = glob.glob(csv_dir + '/*.csv')
    print(f"found {len(filenames)} CSV files in {csv_dir}")

    all_df = (pl.scan_csv(filenames, raise_if_empty=False)
              .with_columns(
                  intersect_bp=pl.col("intersect_hashes") * pl.col("scaled"),
                  weighted_bp=pl.col('n_weighted_found') * pl.col("scaled"),
                  species=extract_species_from_name,
                  metag=pl.col("match_name"),
              )
              .filter(pl.col("intersect_bp") >= args.threshold_bp)
              .select(["species", "metag", "intersect_bp", "weighted_bp"])
              ).collect()


    print(f"loaded manysearch of {all_df['metag'].n_unique()} metagenomes.")

    all_df.write_parquet(args.output_parquet,
                         compression='zstd', compression_level=22)
    print(f"saved to '{args.output_parquet}'")


if __name__ == '__main__':
    sys.exit(main())
