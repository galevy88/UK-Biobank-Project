import argparse
import re
from pathlib import Path

import pandas as pd
import yaml
from tqdm import tqdm

# Enable tqdm for pandas
tqdm.pandas()

# Path definitions
DATA_HESIN_DIR = Path("/Users/galle/PycharmProjects/UK-Biobank-Project/src/data/data_hesin")
CODES_FILES_DIR = Path("/Users/galle/PycharmProjects/UK-Biobank-Project/src/data/pipelines/z_score_pipeline/codes_files")
DEFAULT_DATA_PATH = DATA_HESIN_DIR / "data_hesin_dates_fixed.csv"
DEFAULT_OUTPUT_DIR = DATA_HESIN_DIR / "sub_data_hesin"
DEFAULT_YAML_PATH = CODES_FILES_DIR / "grant_poc.yaml"


def load_codes_from_yaml(yaml_path: Path) -> list[str]:
    """
    Load ICD10 codes from a YAML file.
    
    Args:
        yaml_path: Path to the YAML file containing codes
        
    Returns:
        List of ICD10 codes
    """
    with open(yaml_path, "r") as f:
        data = yaml.safe_load(f)
    
    codes = data.get("codes", [])
    # Convert to strings in case they're parsed as numbers
    return [str(code) for code in codes]


def normalize_code(code: str) -> str:
    """
    Normalize ICD10 code by removing dots for comparison.
    E.g., 'M85.92' -> 'M8592', 'M85' -> 'M85'
    """
    if pd.isna(code):
        return ""
    return str(code).replace(".", "").strip().upper()


def extract_code_from_diag(diag_icd10: str) -> str:
    """
    Extract just the code from a diag_icd10 string like 'L03.1 Cellulitis of other parts of limb'.
    Returns the normalized code (without dots).
    """
    if pd.isna(diag_icd10):
        return ""
    # The code is the first part before the space
    parts = str(diag_icd10).split(" ", 1)
    code = parts[0] if parts else ""
    return normalize_code(code)


def code_matches(diag_code: str, filter_codes: list[str]) -> bool:
    """
    Check if a diagnosis code matches any of the filter codes.
    
    Matching rules:
    - If filter code is a prefix (e.g., 'M85'), match any code starting with it
    - If filter code is exact (e.g., 'M85.92'), match exactly
    
    Args:
        diag_code: The normalized diagnosis code from the data
        filter_codes: List of normalized filter codes from YAML
        
    Returns:
        True if the diagnosis code matches any filter code
    """
    for filter_code in filter_codes:
        if diag_code.startswith(filter_code):
            return True
    return False


def filter_hesin_by_codes(
    data_path: Path,
    yaml_path: Path,
    output_dir: Path
) -> Path:
    """
    Filter data_hesin_dates_fixed.csv based on codes from a YAML file.
    
    Args:
        data_path: Path to data_hesin_dates_fixed.csv
        yaml_path: Path to the YAML file with codes
        output_dir: Directory where output will be saved
        
    Returns:
        Path to the output file
    """
    print(f"Loading codes from {yaml_path}...")
    codes = load_codes_from_yaml(yaml_path)
    print(f"Loaded {len(codes)} codes")
    
    # Normalize all filter codes
    normalized_codes = [normalize_code(code) for code in codes]
    
    print(f"Loading data from {data_path}...")
    df = pd.read_csv(data_path)
    print(f"Loaded {len(df)} rows")
    
    print("Extracting and normalizing diagnosis codes...")
    df["normalized_code"] = df["diag_icd10"].progress_apply(extract_code_from_diag)
    
    print("Filtering rows based on codes...")
    mask = df["normalized_code"].progress_apply(lambda x: code_matches(x, normalized_codes))
    df_filtered = df[mask].drop(columns=["normalized_code"])
    
    print(f"Filtered to {len(df_filtered)} rows ({len(df_filtered) / len(df) * 100:.2f}%)")
    
    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Output file has the same name as the YAML file but with .csv extension
    output_filename = yaml_path.stem + ".csv"
    output_path = output_dir / output_filename
    
    print(f"Saving output to {output_path}...")
    df_filtered.to_csv(output_path, index=False)
    print(f"Done! Saved {len(df_filtered)} rows to {output_path}")
    
    return output_path


def main():
    parser = argparse.ArgumentParser(
        description="Filter data_hesin_dates_fixed.csv based on ICD10 codes from a YAML file"
    )
    parser.add_argument(
        "yaml_file",
        type=str,
        nargs="?",
        default=None,
        help="Path to the YAML file containing codes (default: grant_poc.yaml)"
    )
    parser.add_argument(
        "--data-path",
        type=str,
        default=None,
        help="Path to data_hesin_dates_fixed.csv (default: auto-detected)"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Output directory (default: sub_data_hesin in data_hesin directory)"
    )
    
    args = parser.parse_args()
    
    # Handle YAML file path
    if args.yaml_file is None:
        yaml_path = DEFAULT_YAML_PATH
    else:
        yaml_path = Path(args.yaml_file)
        if not yaml_path.is_absolute():
            # Check if it's just a filename (look in codes_files directory)
            if not yaml_path.exists():
                yaml_in_codes_dir = CODES_FILES_DIR / yaml_path.name
                if yaml_in_codes_dir.exists():
                    yaml_path = yaml_in_codes_dir
                else:
                    # Also try adding .yaml extension
                    yaml_with_ext = CODES_FILES_DIR / (yaml_path.stem + ".yaml")
                    if yaml_with_ext.exists():
                        yaml_path = yaml_with_ext
    
    if not yaml_path.exists():
        raise FileNotFoundError(f"YAML file not found: {yaml_path}")
    
    # Handle data path
    if args.data_path:
        data_path = Path(args.data_path)
    else:
        data_path = DEFAULT_DATA_PATH
    
    if not data_path.exists():
        raise FileNotFoundError(f"Data file not found: {data_path}")
    
    # Handle output directory
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        output_dir = DEFAULT_OUTPUT_DIR
    
    filter_hesin_by_codes(data_path, yaml_path, output_dir)


if __name__ == "__main__":
    main()

