import pandas as pd
import yaml
import sys
import os

# Set working directory to src (script directory)
SRC_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(SRC_DIR)
print(f"Working directory set to: {os.getcwd()}")

# --- Configuration ---
YAML_FILE = 'data/disease_tree_plot/output/tree_yaml.yaml'
CSV_FILE = 'data/pipelines/z_score_pipeline/grant_poc_kobi_new_list/ci_analysis/old_upper_ci_analysis.csv'
OUTPUT_YAML = 'data/disease_graph/input/interesting_diseases_v2.yaml'
MIN_CORRELATION_SCORE = 1.2  # Minimum correlation score to consider
TARGET_DISEASES_COUNT = 100  # Target 10-20 diseases
# ---------------------

# Disease category mapping (ICD-10 code prefixes)
DISEASE_CATEGORIES = {
    'I': 'Cardiovascular/Heart diseases',
    'G': 'Neurodegenerative/Neurological diseases',
    'J': 'Respiratory diseases',
    'M': 'Musculoskeletal diseases',
    'L': 'Skin diseases',
    'C': 'Neoplasms/Cancer',
    'E': 'Endocrine/Metabolic diseases',
    'K': 'Digestive diseases',
    'N': 'Genitourinary diseases'
}

def parse_yaml_codes(node, code_map):
    """
    Recursively traverses the nested YAML structure to build a flat
    dictionary mapping disease codes to their 'title' (name).
    """
    if isinstance(node, dict):
        for key, value in node.items():
            if isinstance(value, dict) and 'title' in value:
                # Store the mapping: key (e.g., 'A00') -> title (e.g., 'Cholera')
                code_map[key] = value['title']
                
                # Recurse into subcategories if they exist
                if 'subcategories' in value and isinstance(value['subcategories'], dict):
                    parse_yaml_codes(value['subcategories'], code_map)
            elif isinstance(value, dict):
                # Recurse for nested structures that might not have a title at the current level
                parse_yaml_codes(value, code_map)

def load_code_name_map(yaml_file):
    """
    Loads the YAML file and initiates the parsing.
    """
    print(f"Loading disease names from {yaml_file}...")
    code_to_name = {}
    try:
        with open(yaml_file, 'r', encoding='utf-8') as f:
            yaml_data = yaml.safe_load(f)
            parse_yaml_codes(yaml_data, code_to_name)
        print(f"Successfully loaded {len(code_to_name)} disease codes.")
        return code_to_name
    except FileNotFoundError:
        print(f"Error: The file {yaml_file} was not found.")
        print("Please make sure it's in the same directory as this script.")
        sys.exit(1)
    except Exception as e:
        print(f"Error loading or parsing YAML file: {e}")
        sys.exit(1)

def get_disease_category(code):
    """Get the category of a disease based on its ICD-10 code prefix."""
    if not code:
        return 'Unknown'
    prefix = code[0]
    return DISEASE_CATEGORIES.get(prefix, 'Other')


def find_interesting_diseases(csv_file, code_to_name):
    """
    Find 10-20 interesting diseases with high correlations from diverse categories.
    """
    print(f"Loading correlation data from {csv_file}...")
    try:
        df = pd.read_csv(csv_file, index_col=0)
    except FileNotFoundError:
        print(f"Error: The file {csv_file} was not found.")
        sys.exit(1)
    except Exception as e:
        print(f"Error loading CSV file: {e}")
        sys.exit(1)

    print("Analyzing correlations...")
    
    # Ensure data is numeric
    df = df.apply(pd.to_numeric, errors='coerce')
    
    # Get upper triangle to avoid duplicates
    import numpy as np
    mask = np.triu(np.ones_like(df, dtype=bool), k=1)
    upper_triangle = df.where(mask)
    
    # Find all pairs with correlation >= MIN_CORRELATION_SCORE
    high_corr_pairs = upper_triangle.stack()
    high_corr_pairs = high_corr_pairs[high_corr_pairs >= MIN_CORRELATION_SCORE]
    high_corr_pairs = high_corr_pairs.sort_values(ascending=False)
    
    print(f"  ✓ Found {len(high_corr_pairs)} pairs with correlation >= {MIN_CORRELATION_SCORE}")
    
    # Filter to only keep pairs where BOTH diseases are in interesting categories (not "Other")
    interesting_pairs = {}
    for (code1, code2), score in high_corr_pairs.items():
        cat1 = get_disease_category(code1)
        cat2 = get_disease_category(code2)
        # Keep if both diseases are in DISEASE_CATEGORIES (not "Other")
        if cat1 != 'Other' and cat2 != 'Other':
            interesting_pairs[(code1, code2)] = score
    
    print(f"  ✓ Found {len(interesting_pairs)} pairs between interesting disease categories")
    
    if not interesting_pairs:
        print("No interesting correlations found. Try lowering MIN_CORRELATION_SCORE.")
        return []
    
    # Count how many correlations each disease has (with other interesting diseases)
    disease_scores = {}
    disease_connections = {}
    
    for (code1, code2), score in interesting_pairs.items():
        # Track total correlation score for each disease
        disease_scores[code1] = disease_scores.get(code1, 0) + score
        disease_scores[code2] = disease_scores.get(code2, 0) + score
        
        # Track number of connections
        disease_connections[code1] = disease_connections.get(code1, 0) + 1
        disease_connections[code2] = disease_connections.get(code2, 0) + 1
    
    # Sort diseases by their total correlation score
    sorted_diseases = sorted(disease_scores.items(), key=lambda x: x[1], reverse=True)
    
    print(f"\n  ✓ {len(sorted_diseases)} unique diseases involved in high correlations")
    
    # Build a well-connected network by selecting diseases that connect to each other
    # Use a greedy algorithm: start with most connected disease, then add diseases
    # that have connections to already-selected diseases
    
    selected_codes = set()
    selected_diseases = []
    category_counts = {}
    
    # Build adjacency list for quick lookup
    adjacency = {}
    for (code1, code2), score in interesting_pairs.items():
        if code1 not in adjacency:
            adjacency[code1] = {}
        if code2 not in adjacency:
            adjacency[code2] = {}
        adjacency[code1][code2] = score
        adjacency[code2][code1] = score
    
    # Sort diseases by number of connections
    sorted_by_connections = sorted(
        disease_connections.items(),
        key=lambda x: x[1],
        reverse=True
    )
    
    # Start with the most connected disease
    if sorted_by_connections:
        seed_code = sorted_by_connections[0][0]
        category = get_disease_category(seed_code)
        selected_codes.add(seed_code)
        selected_diseases.append({
            'code': seed_code,
            'name': code_to_name.get(seed_code, seed_code),
            'category': category,
            'total_score': disease_scores[seed_code],
            'connections': disease_connections[seed_code]
        })
        category_counts[category] = 1
        print(f"  Seed disease: {seed_code} ({disease_connections[seed_code]} connections)")
    
    # Iteratively add diseases that connect to the selected set
    while len(selected_diseases) < TARGET_DISEASES_COUNT:
        best_candidate = None
        best_score = -1
        
        # Find the best candidate: disease with most connections to selected diseases
        for code in disease_connections.keys():
            if code in selected_codes:
                continue
            
            category = get_disease_category(code)
            
            # Check category balance (max 15 per category to allow up to ~135 total)
            # This allows for 100 diseases target while maintaining some balance
            if category_counts.get(category, 0) >= 15:
                continue
            
            # Count connections to already-selected diseases
            connections_to_selected = 0
            total_score_to_selected = 0
            
            if code in adjacency:
                for selected_code in selected_codes:
                    if selected_code in adjacency[code]:
                        connections_to_selected += 1
                        total_score_to_selected += adjacency[code][selected_code]
            
            # Skip if no connections to selected diseases
            if connections_to_selected == 0:
                continue
            
            # Score = number of connections + total correlation score
            candidate_score = connections_to_selected * 10 + total_score_to_selected
            
            if candidate_score > best_score:
                best_score = candidate_score
                best_candidate = {
                    'code': code,
                    'name': code_to_name.get(code, code),
                    'category': category,
                    'total_score': disease_scores[code],
                    'connections': disease_connections[code],
                    'connections_to_selected': connections_to_selected
                }
        
        # If we found a candidate, add it
        if best_candidate:
            selected_codes.add(best_candidate['code'])
            selected_diseases.append(best_candidate)
            category_counts[best_candidate['category']] = category_counts.get(best_candidate['category'], 0) + 1
        else:
            # No more candidates with connections, break
            print(f"  No more well-connected candidates found. Stopping at {len(selected_diseases)} diseases.")
            break
    
    # Print selected diseases
    print(f"\n{'='*70}")
    print(f"SELECTED {len(selected_diseases)} INTERESTING DISEASES")
    print(f"{'='*70}\n")
    
    for i, disease in enumerate(selected_diseases, 1):
        print(f"{i}. {disease['name']} ({disease['code']})")
        print(f"   Category: {disease['category']}")
        print(f"   Total correlation score: {disease['total_score']:.2f}")
        print(f"   Total connections: {disease['connections']}")
        if 'connections_to_selected' in disease:
            print(f"   Connections to selected diseases: {disease['connections_to_selected']}")
        print()
    
    # Show correlations between selected diseases
    print(f"\n{'='*70}")
    print(f"CORRELATIONS BETWEEN SELECTED DISEASES")
    print(f"{'='*70}\n")
    
    selected_codes_list = [d['code'] for d in selected_diseases]
    
    # Find correlations between selected diseases
    between_correlations = []
    for (code1, code2), score in interesting_pairs.items():
        if code1 in selected_codes_list and code2 in selected_codes_list:
            cat1 = get_disease_category(code1)
            cat2 = get_disease_category(code2)
            between_correlations.append({
                'code1': code1,
                'name1': code_to_name.get(code1, code1),
                'code2': code2,
                'name2': code_to_name.get(code2, code2),
                'cat1': cat1,
                'cat2': cat2,
                'score': score
            })
    
    between_correlations.sort(key=lambda x: x['score'], reverse=True)
    
    print(f"Found {len(between_correlations)} correlations between selected diseases:\n")
    
    # Display top correlations
    for i, corr in enumerate(between_correlations[:30], 1):  # Show top 30
        same_cat = " (same category)" if corr['cat1'] == corr['cat2'] else " (cross-category)"
        print(f"{i}. Score: {corr['score']:.2f}{same_cat}")
        print(f"   {corr['name1']} ({corr['code1']}) [{corr['cat1']}]")
        print(f"   ↔")
        print(f"   {corr['name2']} ({corr['code2']}) [{corr['cat2']}]")
        print()
    
    # Save selected disease codes to YAML
    output_data = {'codes': selected_codes_list}
    os.makedirs(os.path.dirname(OUTPUT_YAML), exist_ok=True)
    with open(OUTPUT_YAML, 'w') as f:
        yaml.dump(output_data, f, default_flow_style=False)
    print(f"\n✓ Saved {len(selected_codes_list)} disease codes to: {OUTPUT_YAML}")
    print(f"  Output format: codes list with {len(between_correlations)} connections between them")
    
    return selected_diseases

def main():
    code_to_name_map = load_code_name_map(YAML_FILE)
    if code_to_name_map:
        find_interesting_diseases(CSV_FILE, code_to_name_map)

if __name__ == "__main__":
    main()