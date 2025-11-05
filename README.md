# UK Biobank Project

A comprehensive data processing pipeline for analyzing disease connections and co-occurrences in UK Biobank data, specifically focusing on Hospital Episode Statistics (HESIN) data with ICD-10 disease codes.

## Overview

This project provides a multi-step pipeline for:
- Filtering UK Biobank HESIN data by disease codes and participant demographics
- Creating bootstrap samples with shuffled disease codes for statistical analysis
- Generating disease co-occurrence connection matrices
- Performing statistical analysis and visualization of disease relationships

## Features

- **Flexible Filtering**: Filter data by disease codes (ICD-10) and participant demographics (age, sex, etc.)
- **Bootstrap Sampling**: Create multiple shuffled versions of data for statistical validation
- **Disease Co-occurrence Analysis**: Generate connection matrices showing disease co-occurrence patterns
- **Configurable Pipeline**: YAML-based configuration for easy experimentation
- **Automatic File Detection**: Dynamically detects and processes all filtered data files

## Installation

### Prerequisites

- Python 3.7+
- Access to UK Biobank data (requires `dxpy` for DNAnexus platform)

### Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd UK-Biobank-Project
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Login to DNAnexus (if using UK Biobank data):
```bash
dx login
# This will be valid for 30 days
```

## Project Structure

```
UK-Biobank-Project/
├── src/
│   ├── z_score_pipeline.py          # Main pipeline orchestrator
│   ├── configs/                     # Configuration files
│   │   ├── z_score_pipeline.yaml    # Pipeline configuration
│   │   ├── diseases_mapping.yaml
│   │   └── diseases_selection.yaml
│   ├── steps/
│   │   └── z_score_pipeline/
│   │       ├── filter_step.py       # Step 1: Filter data
│   │       ├── bootstrap_step.py    # Step 2: Create bootstrap samples
│   │       └── connection_matrices_step.py  # Step 3: Generate connection matrices
│   ├── data/
│   │   ├── data_hesin/              # HESIN data files
│   │   ├── information_data/        # Participant metadata
│   │   └── pipelines/
│   │       └── z_score_pipeline/    # Pipeline outputs
│   │           └── {experiment_name}/
│   │               ├── filtered_data/
│   │               ├── bootstraped_hesin_data/
│   │               └── connection_matrices/
│   ├── utils/                       # Utility functions
│   └── notebooks/                   # Jupyter notebooks for analysis
├── requirements.txt
└── README.md
```

## Usage

### Basic Usage

1. **Configure the pipeline** by editing `src/configs/z_score_pipeline.yaml`:

```yaml
filter_step:
  experiment_name: exp1
  HESIN_DATA_PATH: data/data_hesin/data_hesin.csv
  CODES_PATH: data/pipelines/z_score_pipeline/codes_files/wanted_diseases.yaml
  method: keep  # or "drop"
  FILTER_PATH: data/information_data/eid_age_sex.csv
  OUTPUT_PATH: data/pipelines/z_score_pipeline/
  filteration:
    Age at recruitment:
      young: { min: 40, max: 50 }
      middle: { min: 51, max: 60 }
      old: { min: 61, max: 69 }

bootstrap_step:
  INPUT_DATA_DIR: data/pipelines/z_score_pipeline/exp1/filtered_data/
  OUTPUT_BASE_DIR: data/pipelines/z_score_pipeline/
  FIELDS_TO_KEEP:
    - dnx_hesin_diag_id
    - eid
    - diag_icd10
  SHUFFLE_ITERATIONS: 10
  SAVE_BOOTSTRAP_DATA: true

disease_score_step:
  INPUT_DATA_DIR: data/pipelines/z_score_pipeline/exp1/bootstraped_hesin_data/
  OUTPUT_BASE_DIR: data/pipelines/z_score_pipeline/
```

2. **Run the pipeline**:

```bash
cd src
python z_score_pipeline.py
```

### Pipeline Steps

#### Step 1: Filter Step
- Filters HESIN data by disease codes (ICD-10)
- Merges with participant metadata (age, sex, etc.)
- Applies demographic filters (age groups, etc.)
- Outputs filtered CSV files for each filter combination

**Output**: `data/pipelines/z_score_pipeline/{experiment_name}/filtered_data/*.csv`

#### Step 2: Bootstrap Step
- Automatically detects all filtered data files
- Creates shuffled versions by randomizing disease codes per participant
- Generates `SHUFFLE_ITERATIONS` bootstrap samples for each filtered dataset
- Optionally saves bootstrap data to disk

**Output**: `data/pipelines/z_score_pipeline/{experiment_name}/bootstraped_hesin_data/{filter_type}/*.csv`

#### Step 3: Connection Matrices Step
- Processes original filtered data to create baseline connection matrices
- Processes all bootstrap samples to create bootstrap connection matrices
- Generates disease co-occurrence matrices (symmetric matrices showing disease pair frequencies)
- Automatically handles all filter types and bootstrap iterations

**Output**: 
- `data/pipelines/z_score_pipeline/{experiment_name}/connection_matrices/original/*.csv`
- `data/pipelines/z_score_pipeline/{experiment_name}/connection_matrices/bootstrap/{filter_type}/*.csv`

### Configuration Options

#### Filter Step Configuration

- **experiment_name**: Unique identifier for this experiment
- **HESIN_DATA_PATH**: Path to HESIN data CSV file
- **CODES_PATH**: Path to YAML file containing disease codes
- **method**: `keep` (include only listed codes) or `drop` (exclude listed codes)
- **FILTER_PATH**: Path to participant metadata CSV
- **OUTPUT_PATH**: Base output directory
- **filteration**: Dictionary defining filter criteria (e.g., age groups)

#### Bootstrap Step Configuration

- **INPUT_DATA_DIR**: Directory containing filtered data (auto-detected from filter_step)
- **OUTPUT_BASE_DIR**: Base directory for bootstrap outputs
- **FIELDS_TO_KEEP**: List of columns to retain in bootstrap samples
- **SHUFFLE_ITERATIONS**: Number of bootstrap samples per filtered dataset
- **SAVE_BOOTSTRAP_DATA**: Whether to save bootstrap files to disk (default: true)

#### Disease Score Step Configuration

- **INPUT_DATA_DIR**: Directory containing bootstrap data
- **OUTPUT_BASE_DIR**: Base directory for connection matrix outputs

## Disease Codes Configuration

Disease codes are specified in YAML files (e.g., `wanted_diseases.yaml`):

```yaml
codes:
  - I00
  - I01.0
  - I01.1
  # ... more ICD-10 codes
```

The pipeline supports both full ICD-10 codes (e.g., `I01.0`) and simplified codes (prefix before `.` or space).

## Output Files

### Filter Step Output
- `{filter_type}_filtered.csv`: Filtered data for each filter combination

### Bootstrap Step Output
- `{filter_type}/{filter_type}_{1..N}.csv`: Bootstrap samples (N = SHUFFLE_ITERATIONS)

### Connection Matrices Output
- **Original matrices**: `{filter_type}_disease_connection_matrix.csv`
- **Bootstrap matrices**: `{filter_type}_bootstrap_{1..N}_disease_connection_matrix.csv`

Each connection matrix is a symmetric CSV file where:
- Rows and columns represent disease codes (simplified ICD-10)
- Values represent co-occurrence counts (number of participants with both diseases)

## Data Requirements

### Input Files

1. **HESIN Data** (`data_hesin.csv`):
   - Required columns: `dnx_hesin_diag_id`, `eid`, `diag_icd10`
   - Contains hospital episode records with ICD-10 diagnosis codes

2. **Participant Metadata** (`eid_age_sex.csv`):
   - Required columns: `eid`, `Age at recruitment`, `Sex`
   - Contains demographic information for filtering

3. **Disease Codes** (`wanted_diseases.yaml` or `unwanted_diseases.yaml`):
   - YAML file with `codes` list containing ICD-10 codes

## Dependencies

- **dxpy**: DNAnexus platform SDK (for UK Biobank data access)
- **pandas**: Data manipulation and analysis
- **numpy**: Numerical computations
- **scipy**: Scientific computing
- **scikit-learn**: Machine learning utilities
- **matplotlib**: Plotting
- **seaborn**: Statistical visualization
- **plotly**: Interactive visualizations
- **PyYAML**: YAML configuration parsing

See `requirements.txt` for specific versions.

## Features and Benefits

### Automatic File Detection
- Bootstrap step automatically detects all filtered data files
- Connection matrices step processes all bootstrap files automatically
- No hardcoding required - adapts to any number of filter combinations

### Flexible Filtering
- Support for multiple filter dimensions (age, sex, etc.)
- Multiple filter values per dimension (e.g., young, middle, old)
- Cartesian product of all filter combinations

### Statistical Robustness
- Bootstrap sampling for statistical validation
- Multiple iterations for robust statistical inference
- Shuffled disease codes preserve participant-level structure

## Examples

### Example 1: Age-based Analysis

Filter by age groups and create bootstrap samples:

```yaml
filteration:
  Age at recruitment:
    young: { min: 40, max: 50 }
    old: { min: 61, max: 69 }
```

This will create:
- 2 filtered datasets (young, old)
- 20 bootstrap files total (10 per age group if SHUFFLE_ITERATIONS=10)
- 22 connection matrices (2 original + 20 bootstrap)

### Example 2: Multiple Filter Dimensions

```yaml
filteration:
  Age at recruitment:
    young: { min: 40, max: 50 }
    old: { min: 61, max: 69 }
  Sex:
    male: { value: "Male" }
    female: { value: "Female" }
```

This creates 4 filter combinations (young+male, young+female, old+male, old+female).

## Troubleshooting

### Common Issues

1. **No CSV files found in filtered_data**
   - Check that filter_step completed successfully
   - Verify experiment_name matches between steps
   - Ensure filter criteria match available data

2. **Bootstrap files not found**
   - Verify SAVE_BOOTSTRAP_DATA is set to true
   - Check that bootstrap step completed successfully
   - Ensure bootstrap directory path is correct

3. **Connection matrices not created**
   - Verify bootstrap files exist in expected directory structure
   - Check file permissions
   - Ensure sufficient disk space

## Development

### Adding New Steps

To add a new pipeline step:

1. Create a new step file in `src/steps/z_score_pipeline/`
2. Implement the step function with appropriate parameters
3. Add step configuration to `z_score_pipeline.yaml`
4. Integrate step in `z_score_pipeline.py`

### Testing

Run tests (when available):
```bash
pytest tests/
```

## License

[Add your license information here]

## Contributors

[Add contributor information here]

## Citation

If you use this code in your research, please cite:
[Add citation information here]

## Contact

[Add contact information here]
