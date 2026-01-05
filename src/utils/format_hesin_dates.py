"""
Script to reformat data_hesin_dates.csv to match the format of data_hesin.csv
while keeping the date information.

The output will have the diag_icd10 column formatted as "CODE Description"
(matching data_hesin.csv format) along with the event_date column.
"""

import pandas as pd
from pathlib import Path


def normalize_icd10_code(code: str) -> str:
    """
    Normalize ICD10 code by removing dots for comparison.
    E.g., 'L03.1' -> 'L031', 'L729' -> 'L729'
    """
    if pd.isna(code):
        return ""
    return str(code).replace(".", "").strip()


def extract_code_from_description(diag_icd10_with_desc: str) -> str:
    """
    Extract just the code from a description like 'L03.1 Cellulitis of other parts of limb'.
    Returns the code without dots for matching purposes.
    """
    if pd.isna(diag_icd10_with_desc):
        return ""
    # The code is the first part before the space
    parts = str(diag_icd10_with_desc).split(" ", 1)
    code = parts[0] if parts else ""
    return normalize_icd10_code(code)


def format_hesin_dates(
    data_hesin_path: Path,
    data_hesin_dates_path: Path,
    output_path: Path
) -> None:
    """
    Reformat data_hesin_dates.csv to match the format of data_hesin.csv
    while preserving the date information.
    
    Args:
        data_hesin_path: Path to data_hesin.csv (has code with descriptions)
        data_hesin_dates_path: Path to data_hesin_dates.csv (has codes only + dates)
        output_path: Path where the reformatted file will be saved
    """
    print("Loading data_hesin.csv to build code-to-description mapping...")
    # Load data_hesin.csv to create a mapping from code to full description
    df_hesin = pd.read_csv(data_hesin_path)
    
    # Create a mapping from normalized code to the full diag_icd10 string (with description)
    code_to_description = {}
    for diag_icd10 in df_hesin["diag_icd10"].dropna().unique():
        normalized_code = extract_code_from_description(diag_icd10)
        if normalized_code:
            code_to_description[normalized_code] = diag_icd10
    
    print(f"Built mapping for {len(code_to_description)} unique ICD10 codes")
    
    print("Loading data_hesin_dates.csv...")
    df_dates = pd.read_csv(data_hesin_dates_path)
    
    print("Applying code-to-description mapping...")
    # Normalize the codes in data_hesin_dates for matching
    df_dates["normalized_code"] = df_dates["diag_icd10"].apply(normalize_icd10_code)
    
    # Map to the full description
    df_dates["diag_icd10_formatted"] = df_dates["normalized_code"].map(code_to_description)
    
    # Count how many codes didn't find a match
    unmatched_count = df_dates["diag_icd10_formatted"].isna().sum()
    if unmatched_count > 0:
        print(f"Warning: {unmatched_count} rows did not find a matching description")
        # For unmatched codes, keep the original code
        df_dates["diag_icd10_formatted"] = df_dates["diag_icd10_formatted"].fillna(
            df_dates["diag_icd10"]
        )
    
    # Create the output dataframe with the same column order as data_hesin.csv + event_date
    df_output = pd.DataFrame({
        "dnx_hesin_diag_id": df_dates["dnx_hesin_diag_id"],
        "eid": df_dates["eid"],
        "diag_icd10": df_dates["diag_icd10_formatted"],
        "event_date": df_dates["event_date"]
    })
    
    print(f"Saving output to {output_path}...")
    df_output.to_csv(output_path, index=False)
    print(f"Done! Saved {len(df_output)} rows to {output_path}")


def main():
    # Define paths
    base_path = Path(__file__).parent.parent / "data" / "data_hesin"
    data_hesin_path = base_path / "data_hesin.csv"
    data_hesin_dates_path = base_path / "data_hesin_dates.csv"
    output_path = base_path / "data_hesin_dates_fixed.csv"
    
    format_hesin_dates(data_hesin_path, data_hesin_dates_path, output_path)


if __name__ == "__main__":
    main()

