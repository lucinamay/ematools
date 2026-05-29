import io
import logging as lg
from datetime import datetime

import polars as pl

from ematools.data import adverse_effects, procedures, sections_48, smpcs
from ematools.extract_ae import identify_48


def most_recent_smpcs(
    cutoff_year: int = 2020,
    fraction=0.01,
):
    """returns a dataframe of the most recent SmPCs for the cutoff date of 2020"""
    # sections_48(cutoff_year=cutoff_year)
    for method in ["pdfplumber", "camelot", "table-transformer"]:
        lg.info(f"Extracting adverse effects using method: {method}")
        df = adverse_effects(cutoff_year=cutoff_year, fraction=fraction, method=method)
        print(df)
        df.write_csv(f"most_recent_smpcs_{cutoff_year}_{fraction}_{method}.csv")


def main():
    lg.basicConfig(
        # level=lg.DEBUG,
        level=lg.INFO,
    )
    most_recent_smpcs(2020)


if __name__ == "__main__":
    main()
