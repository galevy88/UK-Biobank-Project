import os
import pandas as pd
from functools import reduce

# Path configuration
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(SCRIPT_DIR)  # src directory

DATA_TYPE = "heart_data"  # Change this to work with different data types (e.g., "heart_data")
INPUT_DIR = os.path.join(BASE_DIR, "data", "joints", DATA_TYPE, "input")
OUTPUT_DIR = os.path.join(BASE_DIR, "data", "joints", DATA_TYPE, "output")
OUTPUT_FILENAME = "joined_data.csv"
JOIN_KEY = "Participant ID"
JOIN_TYPE = "inner"  # Options: 'outer', 'inner', 'left', 'right'


def find_participant_id_column(df, preferred_col="Participant ID"):
    """Find the participant ID column in a DataFrame."""
    # Exact match first
    if preferred_col in df.columns:
        return preferred_col
    
    # Look for columns containing "Participant ID"
    matching_cols = [col for col in df.columns if "Participant ID" in col]
    if matching_cols:
        return matching_cols[0]
    
    # Look for "eid" column
    if "eid" in df.columns:
        return "eid"
    
    raise ValueError(f"Could not find participant ID column. Available columns: {list(df.columns)}")


def join_csvs(input_dir, output_dir, output_filename="joined_data.csv", join_key="Participant ID", how="outer"):
    """
    Join all CSV files in a directory based on a common key column.
    
    Args:
        input_dir: Directory containing CSV files to join
        output_dir: Directory to save the joined output
        output_filename: Name of the output file
        join_key: Column name to join on (will search for similar names if not found)
        how: Type of join ('outer', 'inner', 'left', 'right')
    
    Returns:
        DataFrame: The joined data
    """
    # Get all CSV files in the input directory
    csv_files = [f for f in os.listdir(input_dir) if f.endswith('.csv')]
    
    if not csv_files:
        raise ValueError(f"No CSV files found in {input_dir}")
    
    print(f"Found {len(csv_files)} CSV files to join:")
    for f in csv_files:
        print(f"  - {f}")
    
    dataframes = []
    
    for csv_file in csv_files:
        file_path = os.path.join(input_dir, csv_file)
        df = pd.read_csv(file_path)
        
        # Find the participant ID column
        id_col = find_participant_id_column(df, join_key)
        print(f"\n{csv_file}: Using '{id_col}' as join key ({len(df)} rows)")
        
        # Rename to standard name if different
        if id_col != join_key:
            df = df.rename(columns={id_col: join_key})
        
        # Drop duplicate participant ID columns (e.g., if there are multiple)
        cols_to_drop = [col for col in df.columns if "Participant ID" in col and col != join_key]
        if cols_to_drop:
            print(f"  Dropping duplicate ID columns: {cols_to_drop}")
            df = df.drop(columns=cols_to_drop)
        
        dataframes.append(df)
    
    # Join all dataframes
    print(f"\nJoining {len(dataframes)} dataframes using '{how}' join...")
    joined_df = reduce(lambda left, right: pd.merge(left, right, on=join_key, how=how), dataframes)
    
    print(f"Joined data: {len(joined_df)} rows, {len(joined_df.columns)} columns")
    
    # Save to output directory
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, output_filename)
    joined_df.to_csv(output_path, index=False)
    print(f"\nSaved joined data to: {output_path}")
    
    return joined_df


if __name__ == "__main__":
    joined_df = join_csvs(
        input_dir=INPUT_DIR,
        output_dir=OUTPUT_DIR,
        output_filename=OUTPUT_FILENAME,
        join_key=JOIN_KEY,
        how=JOIN_TYPE,
    )

