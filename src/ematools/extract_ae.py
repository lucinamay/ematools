"""Adverse effect table extraction from EMA SmPC section 4.8.

Atomic functions for:
1. Raw table extraction (PDF -> list of DataFrames, one per table)
2. Table interpretation (raw DataFrames -> structured adverse effects)
"""

import io
import re
from typing import Literal

import polars as pl
import pymupdf

from ematools.patterns import FREQUENCY_TERMS, SOC_PATTERNS

# =============================================================================
# Detection helpers (atomic, pure)
# =============================================================================


def detect_frequency(text: str) -> str | None:
    """Detect frequency category from text."""
    text_lower = text.lower().strip()
    for freq_cat, variants in FREQUENCY_TERMS.items():
        if any(v in text_lower for v in variants):
            return freq_cat
    return None


def detect_soc(text: str) -> bool:
    """Check if text matches a System Organ Class pattern."""
    text_lower = text.lower().strip()
    return any(re.search(p, text_lower) for p in SOC_PATTERNS)


# =============================================================================
# Raw table extraction (atomic, method-specific)
# =============================================================================


def extract_tables_pdfplumber(
    pdf_bytes: io.BytesIO,
    pages: list[int],
    settings: dict | None = None,
) -> list[pl.DataFrame]:
    """Extract raw tables using pdfplumber. Returns list of DataFrames, one per table."""
    import pdfplumber

    pdf_bytes.seek(0)

    results = []
    with pdfplumber.open(pdf_bytes) as pdf:
        for page_num in pages:
            if page_num >= len(pdf.pages):
                continue
            page = pdf.pages[page_num]
            tables = page.extract_tables(table_settings=settings)

            for table in tables:
                if not table:
                    continue
                max_cols = max(len(row) for row in table)
                col_names = [f"col_{i}" for i in range(max_cols)]

                rows = []
                for row in table:
                    cells = [(cell or "").strip() for cell in row]
                    cells.extend([""] * (max_cols - len(cells)))
                    rows.append(dict(zip(col_names, cells)))

                df = pl.DataFrame(rows).with_columns(pl.lit(page_num).alias("_page"))
                results.append(df)

    return results


def extract_tables_camelot(
    pdf_path: str,
    pages: list[int],
    flavor: Literal["lattice", "stream"] = "stream",
    settings: dict | None = None,
) -> list[pl.DataFrame]:
    """Extract raw tables using camelot. Returns list of DataFrames, one per table."""
    import camelot

    settings = settings or (
        {"edge_tol": 500, "row_tol": 15, "column_tol": 50} if flavor == "stream" else {}
    )

    pages_str = ",".join(str(p + 1) for p in pages)

    try:
        tables = camelot.read_pdf(pdf_path, pages=pages_str, flavor=flavor, **settings)
    except Exception:
        return []

    results = []
    for table in tables:
        page_num = table.page - 1
        pdf_df = table.df

        col_names = [f"col_{i}" for i in range(len(pdf_df.columns))]
        pdf_df.columns = col_names

        df = (
            pl.from_pandas(pdf_df)
            .with_columns(
                [pl.col(c).cast(pl.String).str.strip_chars() for c in col_names]
            )
            .with_columns(pl.lit(page_num).alias("_page"))
        )
        results.append(df)

    return results


def extract_tables_transformer(
    pdf: io.BytesIO | pymupdf.Document,
    pages: list[int],
    model_name: str = "microsoft/table-transformer-detection",
    device: str = "cpu",
) -> list[pl.DataFrame]:
    """Extract raw tables using Microsoft Table Transformer. Returns list of DataFrames."""
    try:
        import torch
        from PIL import Image
        from transformers import AutoImageProcessor, TableTransformerForObjectDetection
    except ImportError:
        raise ImportError(
            "Table Transformer requires: pip install transformers torch pillow"
        )

    processor = AutoImageProcessor.from_pretrained(model_name)
    model = TableTransformerForObjectDetection.from_pretrained(model_name).to(device)

    if isinstance(pdf, io.BytesIO):
        pdf.seek(0)
        doc = pymupdf.open(stream=pdf, filetype="pdf")
    else:
        doc = pdf

    results = []

    for page_num in pages:
        if page_num >= len(doc):
            continue
        page = doc[page_num]

        pix = page.get_pixmap(matrix=pymupdf.Matrix(2, 2))
        img = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)

        inputs = processor(images=img, return_tensors="pt").to(device)
        with torch.no_grad():
            outputs = model(**inputs)

        target_sizes = torch.tensor([img.size[::-1]])
        detections = processor.post_process_object_detection(
            outputs, threshold=0.7, target_sizes=target_sizes
        )[0]

        for box in detections["boxes"]:
            x1, y1, x2, y2 = box.tolist()
            rect = pymupdf.Rect(x1 / 2, y1 / 2, x2 / 2, y2 / 2)

            blocks = page.get_text("dict", clip=rect)["blocks"]

            lines = []
            for block in blocks:
                if "lines" not in block:
                    continue
                for line in block["lines"]:
                    y = line["bbox"][1]
                    text = " ".join(span["text"] for span in line["spans"]).strip()
                    if text:
                        lines.append({"y": y, "x": line["bbox"][0], "text": text})

            if not lines:
                continue

            lines.sort(key=lambda ln: (ln["y"], ln["x"]))
            rows = []
            current_row = []
            current_y = None
            y_tolerance = 5

            for line in lines:
                if current_y is None or abs(line["y"] - current_y) <= y_tolerance:
                    current_row.append(line["text"])
                    current_y = line["y"] if current_y is None else current_y
                else:
                    if current_row:
                        rows.append(current_row)
                    current_row = [line["text"]]
                    current_y = line["y"]

            if current_row:
                rows.append(current_row)

            if not rows:
                continue

            max_cols = max(len(row) for row in rows)
            col_names = [f"col_{i}" for i in range(max_cols)]

            data = []
            for row in rows:
                row.extend([""] * (max_cols - len(row)))
                data.append(dict(zip(col_names, row)))

            df = pl.DataFrame(data).with_columns(pl.lit(page_num).alias("_page"))
            results.append(df)

    return results


# =============================================================================
# Unified raw extraction
# =============================================================================


def extract(
    pdf: io.BytesIO | pymupdf.Document | str,
    pages: list[int],
    method: Literal["pdfplumber", "camelot", "table-transformer"] = "pdfplumber",
    settings: dict | None = None,
) -> list[pl.DataFrame]:
    """Extract raw tables from PDF pages. No interpretation, just the tables as DataFrames.

    Use this to assess raw table-reading quality and tune settings.

    Args:
        pdf: PDF as BytesIO, pymupdf.Document, or file path
        pages: List of page numbers (0-indexed)
        method: 'pdfplumber', 'camelot', or 'table-transformer'
        settings: Method-specific extraction settings

    Returns:
        List of DataFrames, one per detected table. Each has columns col_0, col_1, ...
        plus _page indicating source page.
    """
    if method == "pdfplumber":
        if isinstance(pdf, str):
            with open(pdf, "rb") as f:
                pdf_bytes = io.BytesIO(f.read())
        elif isinstance(pdf, pymupdf.Document):
            pdf_bytes = io.BytesIO(pdf.tobytes())
        else:
            pdf_bytes = pdf
        return extract_tables_pdfplumber(pdf_bytes, pages, settings)

    elif method == "camelot":
        if not isinstance(pdf, str):
            raise ValueError("Camelot requires a file path")
        return extract_tables_camelot(pdf, pages, settings=settings)

    elif method == "table-transformer":
        if isinstance(pdf, str):
            with open(pdf, "rb") as f:
                pdf_bytes = io.BytesIO(f.read())
        elif isinstance(pdf, pymupdf.Document):
            pdf_bytes = io.BytesIO(pdf.tobytes())
        else:
            pdf_bytes = pdf
        return extract_tables_transformer(pdf_bytes, pages)

    raise ValueError(f"Unknown method: {method}")


# =============================================================================
# Table interpretation (atomic, layout-specific)
# =============================================================================


def detect_table_layout(
    df: pl.DataFrame,
) -> Literal["horizontal", "vertical", "unknown"]:
    """Detect table layout based on header row content.

    horizontal: frequency categories in header row (columns are frequencies)
    vertical: frequency in first column per row
    """
    if df.height < 2:
        return "unknown"

    header = df.row(0)
    freq_in_header = sum(1 for cell in header if detect_frequency(str(cell)))

    if freq_in_header >= 2:
        return "horizontal"

    col_0 = df.get_column(df.columns[0]).to_list() if df.columns else []
    freq_in_first_col = sum(1 for cell in col_0[1:] if detect_frequency(str(cell)))

    if freq_in_first_col >= 2:
        return "vertical"

    return "unknown"


def interpret_horizontal_layout(df: pl.DataFrame) -> list[dict]:
    """Interpret table with frequency categories as column headers."""
    if df.height < 2:
        return []

    data_cols = [c for c in df.columns if not c.startswith("_")]
    if not data_cols:
        return []

    header = df.row(0)
    page = df.get_column("_page")[0] if "_page" in df.columns else None

    freq_cols: dict[int, str] = {}
    for i, cell in enumerate(header):
        if i < len(data_cols):
            freq = detect_frequency(str(cell))
            if freq:
                freq_cols[i] = freq

    if not freq_cols:
        return []

    results = []
    current_soc = None

    for row_idx in range(1, df.height):
        row = df.row(row_idx)
        cells = [str(c).strip() for c in row[: len(data_cols)]]

        if not any(cells):
            continue

        first_cell = cells[0] if cells else ""
        non_empty = [c for c in cells if c]

        if len(non_empty) == 1 and detect_soc(first_cell):
            current_soc = first_cell
            continue

        if detect_soc(first_cell):
            current_soc = first_cell

        if not current_soc:
            continue

        for col_idx, freq in freq_cols.items():
            if col_idx < len(cells) and cells[col_idx]:
                term = cells[col_idx]
                if term and not detect_soc(term) and not detect_frequency(term):
                    results.append(
                        {
                            "soc": current_soc,
                            "frequency": freq,
                            "term": term,
                            "page": page,
                        }
                    )

    return results


def interpret_vertical_layout(df: pl.DataFrame) -> list[dict]:
    """Interpret table with frequency in first/second column per row."""
    data_cols = [c for c in df.columns if not c.startswith("_")]
    if not data_cols:
        return []

    page = df.get_column("_page")[0] if "_page" in df.columns else None
    results = []
    current_soc = None

    for row_idx in range(df.height):
        row = df.row(row_idx)
        cells = [str(c).strip() for c in row[: len(data_cols)]]
        cells = [c for c in cells if c]

        if not cells:
            continue

        if len(cells) == 1:
            if detect_soc(cells[0]):
                current_soc = cells[0]
            continue

        if detect_soc(cells[0]):
            current_soc = cells[0]
            cells = cells[1:]
            if not cells:
                continue

        if not current_soc:
            continue

        freq = None
        terms = []
        for cell in cells:
            f = detect_frequency(cell)
            if f:
                freq = f
            elif cell and not detect_soc(cell):
                terms.append(cell)

        if freq and terms:
            for term in terms:
                results.append(
                    {
                        "soc": current_soc,
                        "frequency": freq,
                        "term": term,
                        "page": page,
                    }
                )

    return results


def interpret_table(df: pl.DataFrame) -> list[dict]:
    """Auto-detect layout and interpret table."""
    layout = detect_table_layout(df)

    if layout == "horizontal":
        return interpret_horizontal_layout(df)
    elif layout == "vertical":
        return interpret_vertical_layout(df)

    horiz = interpret_horizontal_layout(df)
    vert = interpret_vertical_layout(df)
    return horiz if len(horiz) >= len(vert) else vert


# =============================================================================
# Section 4.8 identification
# =============================================================================


def identify_48(pdf_stream: io.BytesIO) -> list[dict]:
    """Find section 4.8 boundaries in PDF. Returns list of {start, end, content}."""
    pdf_stream.seek(0)
    pdf = pymupdf.open(stream=pdf_stream)

    pattern_48 = re.compile(r"^\s*4\.8\s*[Uu]ndesirable\s*effects\s*$", re.MULTILINE)
    pattern_49 = re.compile(r"^\s*4\.9\s*[Oo]verdose\s*$", re.MULTILINE)

    results = []
    in_section = False
    content_parts = []

    for i, page in enumerate(pdf):
        text = page.get_text()

        if not in_section:
            if "4.8" in text and re.search(pattern_48, text):
                content_parts = [re.split(pattern_48, text, maxsplit=1)[-1]]
                results.append({"start": i, "end": None})
                in_section = True
        else:
            if "4.9" in text and re.search(pattern_49, text):
                content_parts.append(re.split(pattern_49, text, maxsplit=1)[0])
                results[-1]["content"] = " ".join(content_parts)
                results[-1]["end"] = i
                in_section = False
                content_parts = []
            else:
                content_parts.append(text)

    if in_section and content_parts:
        results[-1]["content"] = " ".join(content_parts)
        results[-1]["end"] = len(pdf) - 1

    return results


# =============================================================================
# Composite extraction functions
# =============================================================================


def extract_adverse_effects(
    pdf: io.BytesIO | pymupdf.Document | str,
    pages: list[int],
    method: Literal["pdfplumber", "camelot", "table-transformer"] = "pdfplumber",
    settings: dict | None = None,
) -> pl.DataFrame:
    """Full pipeline: extract tables and interpret as adverse effects.

    Args:
        pdf: PDF source
        pages: Pages to extract from (0-indexed)
        method: Extraction method
        settings: Method-specific settings

    Returns:
        DataFrame with columns: soc, frequency, term, page
    """
    tables = extract(pdf, pages, method, settings)

    all_effects = []
    for df in tables:
        effects = interpret_table(df)
        all_effects.extend(effects)

    if not all_effects:
        return pl.DataFrame(
            schema={
                "soc": pl.String,
                "frequency": pl.String,
                "term": pl.String,
                "page": pl.Int64,
            }
        )

    return (
        pl.DataFrame(all_effects)
        .with_columns(
            pl.col("soc").str.strip_chars(),
            pl.col("frequency").str.strip_chars(),
            pl.col("term").str.strip_chars(),
        )
        .filter(pl.col("term").str.len_chars() > 0)
        .unique()
    )


def extract_from_section48(
    section48_dict: dict,
    pdf: io.BytesIO | pymupdf.Document | str,
    method: Literal["pdfplumber", "camelot", "table-transformer"] = "pdfplumber",
    settings: dict | None = None,
) -> pl.DataFrame:
    """Extract adverse effects from identified section 4.8 boundaries."""
    start = section48_dict["start"]
    end = section48_dict.get("end", start)
    pages = list(range(start, end + 1))
    return extract_adverse_effects(pdf, pages, method, settings)


# =============================================================================
# MedDRA mapping
# =============================================================================


def map_to_meddra(df: pl.DataFrame, meddra_dict: dict | None = None) -> pl.DataFrame:
    """Map adverse effect terms to MedDRA Preferred Terms."""
    if "meddra_term" not in df.columns:
        df = df.with_columns(pl.lit(None).cast(pl.String).alias("meddra_term"))

    if meddra_dict is None:
        return df

    def lookup(term: str) -> str | None:
        return meddra_dict.get(term.lower().strip()) or meddra_dict.get(term)

    return df.with_columns(
        pl.col("term").map_elements(lookup, return_dtype=pl.String).alias("meddra_term")
    )


# =============================================================================
# Presets
# =============================================================================

PDFPLUMBER_PRESETS = {
    "default": {
        "vertical_strategy": "text",
        "horizontal_strategy": "text",
        "intersection_tolerance": 15,
        "min_words_vertical": 3,
        "min_words_horizontal": 1,
        "snap_tolerance": 3,
        "join_tolerance": 3,
        "text_tolerance": 3,
    },
    "tight": {
        "vertical_strategy": "text",
        "horizontal_strategy": "text",
        "intersection_tolerance": 10,
        "min_words_vertical": 3,
        "min_words_horizontal": 1,
        "snap_tolerance": 1,
        "join_tolerance": 1,
        "text_tolerance": 1,
    },
    "loose": {
        "vertical_strategy": "text",
        "horizontal_strategy": "text",
        "intersection_tolerance": 25,
        "min_words_vertical": 2,
        "min_words_horizontal": 1,
        "snap_tolerance": 8,
        "join_tolerance": 8,
        "text_tolerance": 8,
    },
}

CAMELOT_PRESETS = {
    "default": {"edge_tol": 500, "row_tol": 15, "column_tol": 50},
    "tight": {"edge_tol": 300, "row_tol": 10, "column_tol": 30},
    "loose": {"edge_tol": 700, "row_tol": 25, "column_tol": 80},
}
