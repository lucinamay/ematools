import logging as lg
from datetime import datetime
from typing import Optional

import polars as pl
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
)

from ematools.extract_ae import identify_48
from ematools.helper import cache_df, cached_pdf
from ematools.scrape import (
    medicine_maintable,
    parse_main_register,
    parse_procedures_rows,
)


def with_progress(func, total, description="Processing..."):
    """
    Wraps a function with a progress bar that advances on each call.

    Args:
        func: The function to wrap
        total: Total number of items to process
        description: Description text for the progress bar

    Returns:
        A tuple of (progress_context, wrapped_function)
    """
    progress = Progress(
        TextColumn("[bold]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
    )
    task = progress.add_task(description, total=total)

    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        progress.advance(task)
        return result

    return progress, wrapper


@cache_df()
def medicines_register() -> pl.DataFrame:
    """Returns the Union Human Medicines Register (currently approved) as a dataframe.

    Args:
    ---
        none

    Returns:
    ---
        A pl.DataFrame with single-medicinal product information, including information
        extracted from the individual medicinal product pages (Marketing Authorisation Holder, ATC, EMA links)

    """
    df = parse_main_register()

    progress_bar, medicine_with_progress = with_progress(
        medicine_maintable,
        total=len(df),
        description="Processing medicines register...",
    )

    with progress_bar:
        df = df.with_columns(
            pl.struct(pl.all())
            .map_elements(
                medicine_with_progress,
                return_dtype=pl.Struct(
                    {
                        "mah": pl.String,
                        "atc": pl.String,
                        "ema_links": pl.String,
                    }
                ),
            )
            .alias("result")
        ).unnest("result")

    return df


@cache_df()
def procedures() -> pl.DataFrame:
    """Returns a flat file of all procedures, cacheing results for quick access.

    Args:
    ---
        none

    Returns:
    ---
        A pl.DataFrame with all procedures and their dates for all medicinal
        products in `medicines_register`, including information on the medicinal
        products from this register as a big flatfile-type table. For procedures
        with available documents (decisions, annexes a.k.a. SmPCs), links to the
        English PDFs are included.

    """
    df_main = medicines_register()

    schema = pl.List(
        pl.Struct({k: pl.String for k, _ in parse_procedures_rows(1)[0].items()})
    )

    progress_bar, procedures_with_progress = with_progress(
        parse_procedures_rows,
        total=len(df_main),
        description="Processing procedures...",
    )

    with progress_bar:
        lg.debug("starting")
        df = (
            df_main.with_columns(
                pl.col("id")
                .map_elements(procedures_with_progress, return_dtype=schema)
                .alias("procedures")
            )
            .explode("procedures")
            .unnest("procedures")
        ).with_columns(pl.col("close_date").str.to_date())
        lg.debug("done")

    progress_bar, procedures_with_progress = with_progress(
        parse_procedures_rows,
        total=len(df_main),
        description="Processing procedures...",
    )

    return df


def smpcs(max_year: Optional[int] = None) -> pl.DataFrame:
    """
    A dataframe containing all smpcs and their 'pdf paths' which streamed BytesIOs
    that you can pass to pdf readers
    """
    colname = "annexes_en"
    df = procedures()

    df = df.filter(pl.col(colname).is_not_null())
    if max_year:
        lg.debug(f"filtering up to and including {max_year}")
        df = df.filter(pl.col("close_date") <= datetime(max_year, 12, 31))

    df = (
        df.sort(["eu_number", "close_date"], descending=[False, True])
        .group_by("eu_number")
        .first()
    )
    lg.info(df["close_date"].dt.year().value_counts(sort=True))
    assert df.n_unique("eu_number") == df.height

    progress_bar, cached_pdf_with_progress = with_progress(
        cached_pdf,
        total=len(df),
        description="Getting PDFs...",
    )

    with progress_bar:
        df = df.with_columns(
            pl.col(colname)
            .map_elements(cached_pdf_with_progress, return_dtype=pl.Object)
            .alias("pdf")
        )
    return df


def sections_48(
    cutoff_year, fraction: Optional[float] = None, *args, **kwargs
) -> pl.DataFrame:
    """returns all most recent sections 4.8 as a flat tabular file"""
    df = smpcs(max_year=cutoff_year)
    if fraction:
        df = df.sample(fraction=fraction, with_replacement=False, seed=1)
    progress_bar, f_with_progress = with_progress(
        identify_48,
        total=len(df),
        description="Finding Sections 4.8...",
    )
    with progress_bar:
        df = (
            df.with_columns(
                pl.col("pdf")
                .map_elements(
                    f_with_progress,
                    return_dtype=pl.List(
                        pl.Struct(
                            {
                                "start": pl.Int64,
                                "content": pl.String,
                                "end": pl.Int64,
                            }
                        )
                    ),
                )
                .alias("aes")
            )
            .explode("aes")
            .unnest("aes")
        )
    return df


# @cache_df()
def adverse_effects(
    cutoff_year,
    method: str = "pdfplumber",
    fraction: Optional[float] = None,
    *args,
    **kwargs,
) -> pl.DataFrame:
    """Returns all adverse effects from section 4.8 as a flat tabular file.

    Each row represents one adverse effect for one SmPC/product.

    Args:
        cutoff_year: Maximum year for procedures (inclusive)
        method: Extraction method - 'pdfplumber', 'camelot', or 'table-transformer'
        **kwargs: Additional arguments passed to extract_from_section48

    Returns:
        A pl.DataFrame with columns: eu_number, name, inn, mah, atc, close_date,
        start, end, soc, frequency, term, page
    """
    from ematools.extract_ae import extract_from_section48

    df_sections = sections_48(cutoff_year, fraction=fraction)
    all_results = []
    progress = Progress(
        TextColumn("[bold]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
    )
    task = progress.add_task("Extracting adverse effects...", total=len(df_sections))

    with progress:
        for row in df_sections.iter_rows(named=True):
            try:
                # Create section dict from row data
                section_dict = {
                    "start": row["start"],
                    "end": row["end"],
                    "content": row.get("content", ""),
                }

                df_ae = extract_from_section48(
                    section48_dict=section_dict,
                    pdf=row["pdf"],
                    method=method,
                    **kwargs,
                )

                if df_ae.height > 0:
                    metadata = {
                        k: v
                        for k, v in row.items()
                        if k not in ["pdf", "content", "start", "end"]
                    }

                    for ae_row in df_ae.iter_rows(named=True):
                        result_row = {**metadata, **ae_row, "extraction_method": method}
                        all_results.append(result_row)
                else:
                    lg.warning(
                        f"No adverse effects found for {row.get('eu_number', 'unknown')}"
                    )

            except Exception as e:
                lg.warning(
                    f"Failed to extract AEs for {row.get('eu_number', 'unknown')}: {e}"
                )

            progress.advance(task)

    # Convert all results to DataFrame
    if not all_results:
        # Return empty DataFrame with expected schema
        return pl.DataFrame(
            schema={
                "eu_number": pl.String,
                "name": pl.String,
                "inn": pl.String,
                "soc": pl.String,
                "frequency": pl.String,
                "term": pl.String,
                "page": pl.Int64,
                "extraction_method": pl.String,
            }
        )

    return pl.DataFrame(all_results)
