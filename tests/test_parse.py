"""
Tests for adverse effects extraction
"""

import io

import polars as pl
import pytest

from ematools.extract_ae import (
    detect_frequency,
    detect_soc,
    extract_with_camelot,
    extract_with_pdfplumber,
    map_to_meddra,
)


def test_detect_frequency():
    """Test frequency detection"""
    assert detect_frequency("Very common") == "very common"
    assert detect_frequency("Common (≥1/100 to <1/10)") == "common"
    assert detect_frequency("Uncommon") == "uncommon"
    assert detect_frequency("Rare") == "rare"
    assert detect_frequency("Not known") == "not known"
    assert detect_frequency("Some random text") is None


def test_detect_soc():
    """Test System Organ Class detection"""
    assert detect_soc("Nervous system disorders")
    assert detect_soc("Cardiac disorders")
    assert detect_soc("Gastrointestinal disorders")
    assert detect_soc("General disorders and administration site conditions")
    assert not detect_soc("Headache")
    assert not detect_soc("Common")


def test_map_to_meddra():
    """Test MedDRA mapping"""
    df = pl.DataFrame(
        {
            "soc": ["Nervous system disorders", "Cardiac disorders"],
            "frequency": ["common", "uncommon"],
            "term": ["dizziness", "tachycardia"],
            "page": [1, 1],
        }
    )

    meddra_dict = {"dizziness": "Dizziness", "tachycardia": "Tachycardia"}

    result = map_to_meddra(df, meddra_dict)

    assert "meddra_term" in result.columns
    assert result["meddra_term"][0] == "Dizziness"
    assert result["meddra_term"][1] == "Tachycardia"


def test_map_to_meddra_no_dict():
    """Test MedDRA mapping with no dictionary"""
    df = pl.DataFrame(
        {
            "soc": ["Nervous system disorders"],
            "frequency": ["common"],
            "term": ["dizziness"],
            "page": [1],
        }
    )

    result = map_to_meddra(df, None)
    assert "meddra_term" in result.columns
    assert result["meddra_term"][0] is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
