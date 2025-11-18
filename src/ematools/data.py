import logging as lg

import polars as pl
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
)

from ematools.helper import cache_df
from ematools.parse import (
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
    df = df.with_columns(
        pl.struct(pl.all())
        .map_elements(
            medicine_maintable,
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
        )
        lg.debug("done")

    progress_bar, procedures_with_progress = with_progress(
        parse_procedures_rows,
        total=len(df_main),
        description="Processing procedures...",
    )

    return df
