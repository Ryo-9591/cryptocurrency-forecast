from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Iterable

import pandas as pd


logger = logging.getLogger(__name__)


def convert_csv_to_parquet(csv_path: Path, output_path: Path | None = None) -> Path:
    """Convert a single CSV file to Parquet.

    Parameters
    ----------
    csv_path:
        The path to the CSV file.
    output_path:
        Optional destination path for the Parquet file. When omitted, the Parquet
        file will be created next to the CSV file with the same stem.
    """
    if output_path is None:
        output_path = csv_path.with_suffix(".parquet")

    logger.info("Reading CSV: %s", csv_path)
    df = pd.read_csv(csv_path)

    logger.info("Writing Parquet: %s", output_path)
    df.to_parquet(output_path, index=False, engine="pyarrow", compression="snappy")

    return output_path


def convert_directory(
    csv_files: Iterable[Path],
) -> list[Path]:
    """Convert multiple CSV files to Parquet."""
    converted_files: list[Path] = []
    for csv_file in csv_files:
        parquet_path = convert_csv_to_parquet(csv_file)
        converted_files.append(parquet_path)
    return converted_files


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert CSV files located under a directory into Parquet format.",
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("data"),
        help="Directory containing CSV files (default: %(default)s)",
    )
    parser.add_argument(
        "--pattern",
        default="*.csv",
        help="Glob pattern used to find CSV files (default: %(default)s)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    input_dir: Path = args.input_dir
    if not input_dir.exists():
        raise FileNotFoundError(f"入力ディレクトリが存在しません: {input_dir}")

    csv_files = sorted(input_dir.glob(args.pattern))
    if not csv_files:
        logger.warning("対象のCSVファイルが見つかりませんでした。")
        return

    for parquet_file in convert_directory(csv_files):
        logger.info("Converted: %s", parquet_file)


if __name__ == "__main__":
    main()

