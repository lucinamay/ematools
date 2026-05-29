"""Interactive notebook for debugging and tailoring table extraction from SmPC section 4.8.

Single-screen interface with PDF viewer on left, extraction controls and results on right.
Navigate through SmPCs, adjust extraction parameters, and save best configurations.
"""

import marimo

__generated_with = "0.18.1"
app = marimo.App(width="full")

with app.setup:
    # Initialization code that runs before all other cells
    pass


@app.cell
def _():
    """Cell 1: Imports and data loading."""

    import json
    import tempfile
    import base64
    import io

    from pathlib import Path

    import marimo as mo
    import pymupdf
    from PIL import Image
    import polars as pl
    from IPython.display import HTML, display
    from ipywidgets import HBox, VBox, Output, Dropdown

    from ematools.data import sections_48
    from ematools.extract_ae import extract

    # --- Load your data directly here ---
    cutoff_year = 2020
    fraction = 0.05
    df_sections = sections_48(cutoff_year=cutoff_year, fraction=fraction)
    return df_sections, extract, io, mo, pl, pymupdf


@app.cell
def _(df_sections, extract, io, mo, pl, pymupdf):
    def pdf_section48(df_sections, idx=0):
        row = df_sections[idx].to_dicts()[0]
        start = row["start"]
        end = row["end"]

        # open original PDF from bytes
        src = pymupdf.open(stream=row["pdf"], filetype="pdf")

        # create new in-memory PDF with only selected pages
        dst = pymupdf.open()
        for p in range(start, min(end + 1, src.page_count)):
            dst.insert_pdf(src, from_page=p, to_page=p)

        # write trimmed pdf to bytes buffer
        buf = io.BytesIO()
        dst.save(buf)
        buf.seek(0)

        src.close()
        dst.close()

        # return actual PDF viewer component
        return mo.pdf(buf)


    def compare(df_sections, idx=0, method="pdfplumber", settings=None):
        pdf_view = pdf_section48(df_sections, idx=idx)
        info = df_sections[idx].to_dicts()[0]

        dfs = extract(
            info["pdf"],
            pages = list(range(info["start"],(info["end"] + 1))),
            method=method,
            settings=settings,
        )
        df = pl.concat(dfs, how="diagonal")
        table_view = mo.ui.table(df,wrapped_columns=df.columns,page_size=50)

        left = mo.Html(
            f'<div style="width: 40vw; overflow: auto;">{pdf_view}</div>'
        )
        right = mo.Html(f'<div style="width: 40vw; overflow: auto;">{table_view}</div>')
        return mo.hstack(
            [left, right],
            # gap="1rem",
            widths=[50,50],
        )


    compare(
        df_sections,
        idx=0,
        method="pdfplumber",
        settings={
            "vertical_strategy": "lines",
            "horizontal_strategy": "lines",
            "intersection_tolerance": 25,
            # "min_words_vertical": 2,
            # "min_words_horizontal": 1,
            # "snap_tolerance": 6,
            "join_tolerance": 6,
            # "text_tolerance": 6,
            # "text_x_tolerance": 3,
            # "text_y_tolerance": 6,
        },
    )
    return (pdf_section48,)


@app.cell
def _(df_sections):
    idx = 5

    row = df_sections[idx].to_dicts()[0]
    pdf = row["pdf"]
    pdf.seek(0)
    import pdfplumber

    results = []
    settings = {
        # "vertical_strategy": "text",
        # "horizontal_strategy": "text",
        # "intersection_tolerance": 11,
        # "min_words_vertical": 2,
        # "min_words_horizontal": 5,
        # "snap_tolerance": 3,
        # "join_tolerance": 10,
        # "text_tolerance": 5,
        # "snap_tolerance": 14,
        # "edge_min_length": 200,
        # "text_keep_blank_chars": True,
    }
    with pdfplumber.open(pdf) as p:
        imgs = []
        for page in p.pages:
            if not page.page_number - 1 in range(row["start"], row["end"]+1):
                continue
            tables = page.extract_tables(settings)
            print(tables)
            im = page.to_image()
            im.reset().debug_tablefinder(settings)
            imgs.append(im)

    print(str(settings))

    kwargs_string = "{" + ','.join([': '.join([f'"{k}"',f'"{k}"']) for k,v in settings.items()]) + "}"
    print(idx, row["name"], row["close_date"], row["eu_number"],str(settings), sep=",")
    imgs
    return idx, pdf, row, settings


@app.cell
def _():
    # 0,Vaborem,2020-03-30,EU/1/18/1334,{"edge_min_length": 200},single,matrix-like
    # 1,NovoRapid,2019-04-01,EU/1/99/119,{"edge_min_length": 200},single,matrix-like
    # 2,Azilect,2019-06-11,EU/1/04/304,{},multiple,
    # 3,Ozempic,2020-10-01,EU/1/17/1251,{},single,
    # 4,Nevanac,2019-08-28,EU/1/07/433,{},single,frequencies within cell
    # 5,Nevanac,2019-08-28,EU/1/07/433,{},single,frequencies within cell
    # 6,Pemetrexed Pfizer,2020-10-02,EU/1/15/1057,{},single,
    # 7,Pemetrexed Pfizer,2020-10-02,EU/1/15/1057,{},single,
    # 8,Temodal,2020-11-18,EU/1/98/096,{},single,table name;soc headers with frequencies as column
    # 9,Temodal,2020-11-18,EU/1/98/096,{},single,table name;soc headers with frequencies as column
    # 10,Temodal,2020-11-18,EU/1/98/096,{},single,table name;soc headers with frequencies as column
    # 11,Temodal,2020-11-18,EU/1/98/096,{},single,table name;soc headers with frequencies as column
    # 12,Temodal,2020-11-18,EU/1/98/096,{},single,table name;soc headers with frequencies as column
    # 13,Temodal,2020-11-18,EU/1/98/096,{},single,table name;soc headers with frequencies as column
    # 14,Temodal,2020-11-18,EU/1/98/096,{},single,table name;soc headers with frequencies as column
    # 15,Cyramza,2020-01-27,EU/1/14/957,{},multiple,monotherapy and chemotherapycombination above table
    # 16,Avastin,2020-04-06,EU/1/04/300,{},single,severe separate;postmarketing
    # 17,Champix,2019-09-18,EU/1/06/360,{'horizontal_strategy': 'text', 'intersection_tolerance': 11},single,soc headers with frequencies as column;inaccurate main header
    # 18,Champix,2019-09-18,EU/1/06/360,{'horizontal_strategy': 'text', 'intersection_tolerance': 11},single,soc headers with frequencies as column;inaccurate main header
    # 19,Champix,2019-09-18,EU/1/06/360,{'horizontal_strategy': 'text', 'intersection_tolerance': 11},single,soc headers with frequencies as column;inaccurate main header

    # 0.05
    # 0,Erleada,2020-01-29,EU/1/18/1342,{},single,adverse reaction and frequency together
    # 1,Herzuma,2020-09-16,EU/1/17/1257,{},single,mono and chemotherapy together;clean_soc_term_freq
    # 2,Zonegran,2019-10-28,EU/1/04/307,{},single,matrix-like
    # 3,Zonegran,2019-10-28,EU/1/04/307,{},single,matrix-like
    # 4,Zonegran,2019-10-28,EU/1/04/307,{},single,matrix-like
    # 5,Anagrelide Viatris,2018-02-19,EU/1/17/1256,{},single,frequency header above frequency headers
    # 6,IVEMEND,2019-12-18,EU/1/07/437,{},single,two types of medicines
    # 7,Genvoya,2020-09-18,EU/1/15/1061,{},single,soc headers with frequencies as column
    # 8,ReFacto AF,2020-10-01,EU/1/99/103,{},matrix-like
    # 9




    # Sometimes, "tabulated list" is there, sometimes, "table" is there, splitting further on that is good.
    return


@app.cell
def _(extract, mo, pdf, pl, row, settings):
    tables_extract = extract(pdf=pdf,pages=list(range(row["start"],row["end"]+1)), method="pdfplumber",settings=settings)
    df = pl.concat(tables_extract,how="diagonal")# if tables else pl.DataFrame()
    mo.ui.table(df,page_size=50)
    return (df,)


@app.cell
def _(df, pl):
    def add_read_column_names(df:pl.DataFrame)->pl.DataFrame:
        """in: df without column names with column names."""
        # print([x[0] for x in  df[0].to_dict().values()])
        first_row = [str(x[0]).replace("\n"," ") for x in  df[0].to_dict().values()]
        return df.rename(dict(zip(df.columns,first_row))).remove(df[0].to_dict())

    def parse_matrixlike(dfs):
        pass
    add_read_column_names(df)

    return


@app.cell
def _(df_sections, idx, pdf_section48):
    pdf_section48(df_sections,idx=idx)
    return


@app.cell
def _():
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
