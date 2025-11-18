"""Simple caching decorator for DataFrame results using pystow.

This module provides a decorator that automatically caches function results
as parquet files using pystow for data management.
"""

import functools
import hashlib
import io
import logging as lg
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Any, Callable

import polars as pl
import pystow
import requests

MAINMODULE = pystow.module("ematools")
CACHEDIR = MAINMODULE.join("cache")
REQUESTDIR = MAINMODULE.join("cache", "requests")
_log_lock = Lock()


def cache_df(folder: Path = CACHEDIR, cache_key: str | None = None) -> Callable:
    """Decorator that caches DataFrame results as parquet files using pystow.

    This decorator wraps functions that return polars DataFrames and automatically
    saves their results to disk using pystow. On subsequent calls, if the cache
    exists, the function reads from the cached parquet file instead of executing
    the original function.

    Args:
        folder: The subfolder within the pystow module where cache files
                  are stored (defaults to pystow' cachedir (`~/.data/ematools/cache`)).
        cache_key: Optional custom name for the cache file (without extension).
                  If not provided, uses the function name.

    Returns:
        A decorator function that wraps the target function with caching logic.

    Example:
        >>> @cache_df()
        ... def load_data() -> pl.DataFrame:
        ...     # Expensive operation
        ...     return pl.DataFrame({"col": [1, 2, 3]})
        ...
        >>> df = load_data()  # Executes function and caches result
        >>> df = load_data()  # Reads from cache

        >>> @cache_df(cache_key="custom_name")
        ... def process_data() -> pl.DataFrame:
        ...     return pl.DataFrame({"data": [4, 5, 6]})

    Notes:
        - The decorated function must return a polars DataFrame.
        - Cache files are stored as parquet format.
        - The cache location is managed by pystow and can be found at:
          ~/.data/{module_name}/{subfolder}/{cache_key}.parquet
    """

    def decorator(func: Callable[..., pl.DataFrame]) -> Callable[..., pl.DataFrame]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> pl.DataFrame:
            filename = cache_key if cache_key else func.__name__
            filepath: Path = Path(folder) / f"{filename}.parquet"

            if filepath.exists():
                lg.debug(f"cached file {filepath} exists, reading from cached file")
                return pl.read_parquet(filepath)

            df = func(*args, **kwargs)
            df.write_parquet(filepath)
            return df

        return wrapper

    return decorator


def cached_get(
    url: str,
    force: bool = False,
    cache_dir: Path = REQUESTDIR,
    max_retries: int = 3,
) -> requests.Response:
    """Makes an HTTP GET request with caching.

    Args:
    ---
        url: The URL to fetch.
        force: If True, bypasses cache and makes a fresh request.
        cache_dir: Directory where cache files are stored.
        max_retries: Maximum number of retry attempts for failed requests.

    Returns:
    ---
        A requests.Response object.

    Raises:
    ---
        requests.RequestException: If the request fails after all retry attempts.
    """
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)

    url_hash = hashlib.sha256(url.encode()).hexdigest()[:16]
    cache_file = cache_dir / f"{url.split('/')[-1].split('.')[0][:25]}_{url_hash}.html"
    log_file = cache_dir / "request_log.parquet"

    # Return cached response if available
    if not force and cache_file.exists():
        response = requests.Response()
        response.status_code = 200
        response._content = cache_file.read_bytes()
        response.url = url
        return response

    # Make request with retries
    for attempt in range(max_retries):
        try:
            response = requests.get(url)
            if response.status_code == 200:
                cache_file.write_bytes(response.content)

                # Update log with Polars
                new_entry = pl.DataFrame(
                    {
                        "filename": [cache_file.name],
                        "url": [url],
                        "timestamp": [datetime.now()],
                        "status_code": [response.status_code],
                    }
                )

                with _log_lock:
                    if log_file.exists():
                        try:
                            log_df = pl.read_parquet(log_file)
                            log_df = pl.concat(
                                [log_df.filter(pl.col("url") != url), new_entry]
                            )
                        except Exception as e:
                            lg.warning(f"Failed to read log file, creating new: {e}")
                            log_df = new_entry
                    else:
                        log_df = new_entry
                    log_df.write_parquet(log_file)

                return response

        except requests.RequestException as e:
            if attempt == max_retries - 1:
                lg.warning(f"Attempt {attempt} on {url} with warning {e}")
                raise
            continue

    raise requests.RequestException(
        f"Failed to fetch {url} after {max_retries} attempts"
    )


def cached_pdf(link: str, stream=True) -> bytes | io.BytesIO:
    """downloads pdf from link, saving it in cache, and returns the pdf file.

    Args:
    ---
        link (str): the url link to the pdf file
        stream (bool): whether to return the pdf as a BytesIO type that can be
        read by pdf readers. Defaults to True.

    Returns:
    ---
        the pdf as io.BytesIO or bytes (if `stream==False`)
    """
    response = cached_get(link)
    pdf_bytes = response.content
    if stream:
        return io.BytesIO(pdf_bytes)
    return pdf_bytes
