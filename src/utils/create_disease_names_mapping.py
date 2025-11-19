import pandas as pd
import yaml
import os
from collections import OrderedDict

# Set working directory to src (script directory)
SRC_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(SRC_DIR)
print(f"Working directory set to: {os.getcwd()}")

# === CONFIGURATION ===
OLD_CSV_PATH = 'data/pipelines/z_score_pipeline/grant_poc_kobi_new_list/ci_analysis/old_upper_ci_analysis_kobi_gal_session.csv'
YOUNG_CSV_PATH = 'data/pipelines/z_score_pipeline/grant_poc_kobi_new_list/ci_analysis/young_upper_ci_analysis_kobi_gal_session.csv'
YOUNG_CSV_PATH_FALLBACK = 'data/pipelines/z_score_pipeline/grant_poc_kobi_new_list/ci_analysis/young_upper_ci_analysis_selected_based_on_kobi_v3.csv'
TREE_YAML_PATH = 'data/disease_tree_plot/output/tree_yaml.yaml'
OUTPUT_YAML_PATH = 'data/disease_graph/input/disease_names_mapping_kobi_gal_session.yaml'


def load_matrix_codes(csv_path):
    """Load disease codes from CSV file."""
    if not os.path.exists(csv_path):
        return set()
    df = pd.read_csv(csv_path, index_col=0)
    codes = set(df.index.tolist()) | set(df.columns.tolist())
    return codes


def find_code_in_yaml(yaml_data, code, path=None):
    """Recursively search for a code in the YAML structure and return its title."""
    if path is None:
        path = []
    
    if isinstance(yaml_data, dict):
        for key, value in yaml_data.items():
            # Check if this key matches the code we're looking for
            if key == code:
                # If the value is a dict with a 'title' key, return it
                if isinstance(value, dict) and 'title' in value:
                    return value['title']
                # If the value is a string, return it
                elif isinstance(value, str):
                    return value
                # Otherwise, return the key itself as a fallback
                else:
                    return key
            
            # Recursively search in subcategories
            if isinstance(value, dict):
                result = find_code_in_yaml(value, code, path + [key])
                if result:
                    return result
    
    elif isinstance(yaml_data, list):
        for item in yaml_data:
            result = find_code_in_yaml(item, code, path)
            if result:
                return result
    
    return None


def extract_all_codes_from_yaml(yaml_data, code_to_name=None, path=None):
    """Extract all code-to-name mappings from YAML structure."""
    if code_to_name is None:
        code_to_name = {}
    if path is None:
        path = []
    
    if isinstance(yaml_data, dict):
        for key, value in yaml_data.items():
            # Skip non-code keys
            if key in ['CategoryCount', 'title', 'subcategories']:
                if isinstance(value, dict):
                    extract_all_codes_from_yaml(value, code_to_name, path + [key])
                continue
            
            # Check if key looks like a disease code (e.g., C05, I20, etc.)
            # Pattern: starts with letter, followed by digits, optionally with dot and more
            if isinstance(key, str) and len(key) >= 2:
                if key[0].isalpha() and key[1:].replace('.', '').isdigit():
                    # This looks like a disease code
                    if isinstance(value, dict) and 'title' in value:
                        code_to_name[key] = value['title']
                    elif isinstance(value, str):
                        code_to_name[key] = value
                    # Also check subcategories for more specific codes
                    if isinstance(value, dict) and 'subcategories' in value:
                        extract_all_codes_from_yaml(value.get('subcategories', {}), code_to_name, path + [key])
                else:
                    # Continue searching
                    if isinstance(value, dict):
                        extract_all_codes_from_yaml(value, code_to_name, path + [key])
    
    return code_to_name


def get_all_codes_from_csvs():
    """Get all unique codes from CSV files."""
    all_codes = set()
    
    # Load from old CSV
    codes = load_matrix_codes(OLD_CSV_PATH)
    all_codes.update(codes)
    print(f"  ✓ Found {len(codes)} codes in old CSV")
    
    # Load from young CSV (try both paths)
    codes = load_matrix_codes(YOUNG_CSV_PATH)
    if not codes:
        codes = load_matrix_codes(YOUNG_CSV_PATH_FALLBACK)
    all_codes.update(codes)
    print(f"  ✓ Found {len(codes)} codes in young CSV")
    
    print(f"  ✓ Total unique codes: {len(all_codes)}")
    return sorted(all_codes)


# ------------------------------------------------------------------ #
# Main execution
# ------------------------------------------------------------------ #
print("="*70)
print("Creating Disease Names Mapping YAML")
print("="*70)

# Get all codes from CSV files
print("\n1. Extracting codes from CSV files...")
all_codes = get_all_codes_from_csvs()
print(f"   Codes: {', '.join(all_codes)}")

# Load tree YAML
print(f"\n2. Loading tree YAML from: {TREE_YAML_PATH}")
with open(TREE_YAML_PATH, 'r') as file:
    tree_data = yaml.safe_load(file)

# Extract all code-to-name mappings from YAML
print("\n3. Extracting code-to-name mappings from tree YAML...")
all_mappings = extract_all_codes_from_yaml(tree_data)
print(f"   ✓ Found {len(all_mappings)} code mappings in tree YAML")

# Create mapping for codes we need
print("\n4. Creating mapping for codes in CSV files...")
disease_mapping = OrderedDict()

for code in all_codes:
    # First try to find exact match
    name = all_mappings.get(code)
    
    # If not found, try to find in YAML structure
    if not name:
        name = find_code_in_yaml(tree_data, code)
    
    # If still not found, use code as fallback
    if not name:
        name = code
        print(f"   ⚠ Warning: Could not find name for code {code}, using code as name")
    
    disease_mapping[code] = name

# Save to YAML file
print(f"\n5. Saving mapping to: {OUTPUT_YAML_PATH}")
os.makedirs(os.path.dirname(OUTPUT_YAML_PATH), exist_ok=True)

# Convert OrderedDict to regular dict for cleaner YAML output
disease_mapping_dict = dict(disease_mapping)

with open(OUTPUT_YAML_PATH, 'w') as file:
    yaml.dump(disease_mapping_dict, file, default_flow_style=False, sort_keys=False, allow_unicode=True)

print(f"   ✓ Successfully saved {len(disease_mapping)} mappings")

# Print summary
print("\n" + "="*70)
print("Summary:")
print("="*70)
print(f"  - Total codes: {len(all_codes)}")
print(f"  - Mappings created: {len(disease_mapping)}")
print(f"  - Output file: {OUTPUT_YAML_PATH}")
print("\nMapping preview:")
for code, name in list(disease_mapping.items())[:10]:
    print(f"  {code}: {name}")
if len(disease_mapping) > 10:
    print(f"  ... and {len(disease_mapping) - 10} more")
print("="*70 + "\n")

