import yaml
import csv
import re
import os

def read_yaml_file(yaml_file_path):
    with open(yaml_file_path, 'r') as file:
        return yaml.safe_load(file)

def read_tsv_file(tsv_file_path):
    tsv_data = {}
    with open(tsv_file_path, 'r') as file:
        reader = csv.DictReader(file, delimiter='\t')
        for row in reader:
            code = row['coding']
            meaning = row['meaning']
            # Normalize code to standard format (e.g., A000 -> A00.0)
            normalized_code = code
            if len(code) > 3 and code[0].isalpha() and code[1:].isdigit():
                normalized_code = f"{code[0]}{code[1:3]}.{code[3:]}"
            # Group by the parent category (e.g., A00 for A00.0, A00.1)
            parent = normalized_code.split('.')[0] if '.' in normalized_code else normalized_code
            if parent not in tsv_data:
                tsv_data[parent] = []
            tsv_data[parent].append((normalized_code, meaning))
    return tsv_data

def merge_yaml_tsv(yaml_data, tsv_data):
    result = {'CategoryCount': {}}

    # Iterate through each chapter in the YAML file
    for chapter, chapter_data in yaml_data['CategoryCount'].items():
        result['CategoryCount'][chapter] = {
            'title': chapter_data['title'],
            'subcategories': {}
        }

        # Iterate through each subcategory in the chapter
        for subcategory_code, subcategory_title in chapter_data['subcategories'].items():
            # All subcategories are strings in the updated YAML
            result['CategoryCount'][chapter]['subcategories'][subcategory_code] = {
                'title': subcategory_title,
                'subcategories': {}
            }

            # Split the range (e.g., A00-A09, G00-G09) to find matching TSV entries
            start, end = subcategory_code.split('-')
            for code in tsv_data:
                # Check if the code falls within the range (e.g., A00 or G00 is in A00-A09 or G00-G09)
                if code.startswith(start[0]) and start <= code <= end:
                    # Find the main code entry in tsv_data (the one without a dot)
                    main_entry = next((entry for entry in tsv_data[code] if '.' not in entry[0]), None)
                    # Use the meaning without the code prefix, or fall back to YAML title
                    title = main_entry[1].split(' ', 1)[1] if main_entry and ' ' in main_entry[1] else main_entry[1] if main_entry else subcategory_title
                    result['CategoryCount'][chapter]['subcategories'][subcategory_code]['subcategories'][code] = {
                        'title': title,
                        'subcategories': {}
                    }
                    # Add sub-diseases (e.g., A00.0, G00.0)
                    for sub_code, sub_meaning in tsv_data.get(code, []):
                        if '.' in sub_code:  # Only include sub-diseases (with a dot)
                            # Use the meaning without the code prefix
                            sub_title = sub_meaning.split(' ', 1)[1] if ' ' in sub_meaning else sub_meaning
                            result['CategoryCount'][chapter]['subcategories'][subcategory_code]['subcategories'][code]['subcategories'][sub_code] = sub_title

    return result

def main(yaml_file_path, tsv_file_path, output_file_path):
    # Read the input files
    yaml_data = read_yaml_file(yaml_file_path)
    tsv_data = read_tsv_file(tsv_file_path)

    # Merge the data
    merged_data = merge_yaml_tsv(yaml_data, tsv_data)

    # Create the output directory if it doesn't exist
    output_dir = os.path.dirname(output_file_path)
    if output_dir:  # Ensure the path has a directory component
        os.makedirs(output_dir, exist_ok=True)

    # Write the output to a YAML file
    with open(output_file_path, 'w') as file:
        yaml.safe_dump(merged_data, file, sort_keys=False, allow_unicode=True)

if __name__ == "__main__":
    yaml_file_path = 'data/disease_tree_plot/input/supra_family.yaml'  # Input YAML file path
    tsv_file_path = 'data/disease_tree_plot/input/codes.tsv'  # Input TSV file path
    output_file_path = 'data/disease_tree_plot/output/tree_yaml.yaml'  # Output file path
    main(yaml_file_path, tsv_file_path, output_file_path)