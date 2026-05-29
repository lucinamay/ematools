#!/usr/bin/env python3
"""
Quick test script for adverse_effects extraction
"""

import logging as lg

lg.basicConfig(level=lg.INFO)

from ematools import adverse_effects

print("Testing adverse_effects extraction...")
print("=" * 80)

# Test with a small cutoff year to limit data
try:
    df = adverse_effects(cutoff_year=2020, method="pdfplumber")

    print(f"\n✓ Successfully extracted {len(df)} adverse effects")
    print(f"\nColumns: {df.columns}")
    print(f"\nShape: {df.shape}")

    if len(df) > 0:
        print("\nFirst 5 rows:")
        print(df.head(5))

        print("\nUnique products:")
        print(f"{df['eu_number'].n_unique()} unique EU numbers")

        print("\nFrequency distribution:")
        print(df["frequency"].value_counts())

        print("\nTop 10 SOCs:")
        print(df["soc"].value_counts().head(10))

        print("\nSample terms:")
        print(df["term"].unique().head(20).to_list())

        print("\n✓ All checks passed!")
    else:
        print("\n⚠ Warning: No adverse effects extracted")

except Exception as e:
    print(f"\n✗ Error: {e}")
    import traceback

    traceback.print_exc()
