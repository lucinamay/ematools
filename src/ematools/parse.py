import json
import logging as lg
import re

import polars as pl

from ematools.helper import cache_df, cached_get


def _clean_json(json: str) -> str:
    """cleans json characters"""
    return re.sub(r"[\x00-\x1f\x7f-\x9f]", " ", json)


@cache_df()
def parse_main_register() -> pl.DataFrame:
    """Parse EU medicines register from JavaScript dataSet variable."""

    all_data = []
    page = 1
    base = "https://ec.europa.eu/health/documents/community-register/html"

    while True:
        url = (
            f"{base}/reg_hum_act.htm" if page == 1 else f"{base}/reg_hum_act{page}.htm"
        )
        r = cached_get(url)

        if r.status_code != 200:
            lg.debug(f"Stopped traversing at page {page} as it did not exist")
            break

        match = re.search(r"var dataSet = (\[.*?\]);", r.text, re.DOTALL)
        if not match:
            lg.debug(f"No match on page {page}")
            break

        data = json.loads(_clean_json(match.group(1)))
        all_data.extend(data)
        page += 1

    # Flatten nested eu_num structure
    rows = []
    for item in all_data:
        rows.append(
            {
                "eu_number": item["eu_num"]["display"],
                "pre": item["eu_num"]["pre"],
                "id": item["eu_num"]["id"],
                "name": item["name"],
                "inn": item["inn"],
                "indication": item["indication"]
                .replace("<br/>", " ")
                .replace("<br>", " ")
                .replace("<u>", " ")
                .replace("</u>", " ")
                .replace("• ", " ")
                .strip(),
                "company": item["company"],
            }
        )

    return pl.DataFrame(rows).cast({"id": pl.Int64})


def medicine_page(idx: int | str, pre: str = "h") -> str:
    if isinstance(idx, int) and idx < 1000:
        idx = f"{idx:03d}"
    base = (
        f"https://ec.europa.eu/health/documents/community-register/html/{pre}{idx}.htm"
    )
    return cached_get(base).text


def parse_medicine_page_top(idx: int) -> dict:
    """Given a certain index (of the product), returns the information in the
    first table of that product site as a dictionary
    """

    html = medicine_page(idx)

    match = re.search(r"var dataSet_product_information = (\[.*?\]);", html, re.DOTALL)
    if not match:
        return {}

    all_data = json.loads(_clean_json(match.group(1)))
    cleaned_data = {}
    for item in all_data:
        match t := item["type"]:
            case "eu_num":
                cleaned_data["eu_number"] = item["value"]
            case "name" | "inn" | "indication" | "mah":
                cleaned_data[t] = item["value"]
            case "atc":
                atcs = []
                for atc_entry in item["meta"]:
                    for level_dict in atc_entry:
                        if level_dict["level"] == "5":
                            atcs.append(level_dict["code"])
                cleaned_data[t] = ";".join(atcs)
            case "ema_links":
                cleaned_data[t] = ";".join([d["url"] for d in item["meta"]])
            case "orphan_links":
                continue
            case _:
                lg.warning(f"Encountered unknown type: {t}, skipping")
                continue
    return cleaned_data


def medicine_maintable(row: dict) -> dict:
    data = parse_medicine_page_top(row["id"])
    if "indication" in data:
        data.pop("indication")
    for type_ in ["name", "eu_number", "inn"]:
        if type_ not in data:
            continue
        v = data.pop(type_)
        assert row[type_] == v, f"{type_}: {row[type_]},{v} "
    return data


def parse_procedures(idx: int | str) -> pl.DataFrame:
    """Parse EC procedures table from product page. Returns DataFrame with columns:
    close_date, procedure_type, ema_number, decision_number, summary_en, decisions_en, annexes_en

    URLs follow pattern: https://ec.europa.eu/health/documents/community-register/{year}/{YYYYMMDD}{proc_id}/{type}_{proc_id}_en.pdf
    where date is the decision date."""

    html = medicine_page(idx)
    match = re.search(r"var dataSet_proc = (\[.*?\]);", html, re.DOTALL)
    if not match:
        return pl.DataFrame()

    data = json.loads(match.group(1))
    base_url = "https://ec.europa.eu/health/documents/community-register"

    rows = []
    for rec in data:
        proc_id = rec["id"]
        row = {
            "close_date": rec.get("closed"),
            "procedure_type": rec.get("type"),
            "ema_number": rec.get("ema_number"),
            "decision_number": rec.get("decision", {}).get("number"),
        }

        # Build EN URLs if files exist and decision date is available
        dec_date = rec.get("decision", {}).get("date")
        if dec_date:
            year = dec_date.split("-")[0]
            dec_date_formatted = dec_date.replace("-", "")
            url_path = f"{base_url}/{year}/{dec_date_formatted}{proc_id}"

            files_dec = rec.get("files_dec") or []
            files_anx = rec.get("files_anx") or []

            if any(f["code"] == "en" for f in files_dec):
                row["decisions_en"] = f"{url_path}/dec_{proc_id}_en.pdf"
            if any(f["code"] == "en" for f in files_anx):
                row["annexes_en"] = f"{url_path}/anx_{proc_id}_en.pdf"
                row["summary_en"] = row["annexes_en"]  # SPC is in annex

        rows.append(row)

    return pl.DataFrame(rows)


def parse_procedures_rows(id_val):
    df = parse_procedures(id_val)
    return df.to_dicts() if df.height > 0 else []
