import pandas as pd
import logging
import sys
import re
from typing import Dict, Any

# ==========================================
# CONFIGURATION BLOCK
# ==========================================
# Centralized configuration mapping logical names to physical file headers
# and defining all dynamic filtering criteria.
PIPELINE_CONFIG = {
    "io": {
        "input_path": "raw_apptio_inventory.xlsx",
        "output_path": "filtered_apptio_inventory.xlsx",
        "log_file_path": "pipeline_execution.log"
    },
    "columns": {
        "RESOURCE_ID": "Resource ID",       # Update with actual Excel header
        "ACCOUNT_NAME": "Account Name",     # Update with actual Excel header
        "INSTANCE_NAME": "Instance Name"    # Update with actual Excel header
    },
    "filters": {
        "RESOURCE_ID": {
            "contains": ["Snap", "arn"]
        },
        "ACCOUNT_NAME": {
            "contains": ["Transit", "DRAWSTransit", "Tooling-Testing", "Terraform"]
        },
        "INSTANCE_NAME": {
            "exact": ["(not set)"],
            # Combined 'Standard' and 'Targeted' exclusions into a single evaluation list
            "contains": [
                "ADO", "Disaster Recovery", "crisil-cis-ec2", "eks", 
                "AZDEC", "BIB", "AMFA", "Test", "Demo"
            ]
        }
    }
}

# ==========================================
# SYSTEM SETUP
# ==========================================
def setup_logging(log_file_path: str) -> logging.Logger:
    """Configures dual-output logging (Console + File) at the INFO level."""
    logger = logging.getLogger("ApptioPipeline")
    logger.setLevel(logging.INFO)

    # Prevent adding multiple handlers if script is run interactively multiple times
    if not logger.handlers:
        formatter = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(message)s', 
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # File Handler
        file_handler = logging.FileHandler(log_file_path, mode='w')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Console Handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger

logger = setup_logging(PIPELINE_CONFIG["io"]["log_file_path"])

# ==========================================
# PIPELINE FUNCTIONS
# ==========================================
def validate_columns(df: pd.DataFrame, expected_columns: Dict[str, str]) -> None:
    """
    Pre-flight check to ensure all configured target columns exist in the DataFrame.
    Exits gracefully with a CRITICAL log if any are missing.
    """
    actual_columns = set(df.columns)
    required_columns = set(expected_columns.values())
    
    missing_columns = required_columns - actual_columns
    
    if missing_columns:
        logger.critical(f"Pre-flight validation failed. Missing expected columns: {missing_columns}")
        logger.critical(f"Available columns in dataset: {list(actual_columns)}")
        logger.info("Exiting pipeline gracefully due to schema mismatch.")
        sys.exit(1)
        
    logger.info("Pre-flight column validation passed.")

def apply_exclusions(df: pd.DataFrame, column_name: str, criteria: Dict[str, list]) -> pd.DataFrame:
    """
    Applies exact and substring exclusion filters to a specific column.
    Logs telemetry regarding the criteria applied and the number of rows dropped.
    """
    initial_rows = len(df)
    
    # 1. Process Exact Matches (Case-Insensitive for robustness)
    if "exact" in criteria and criteria["exact"]:
        exact_list = [str(x).lower() for x in criteria["exact"]]
        mask = df[column_name].astype(str).str.lower().isin(exact_list)
        df = df[~mask]
        
        dropped = initial_rows - len(df)
        logger.info(f"Filter [Exact] applied on '{column_name}' | Criteria: {criteria['exact']} | Rows dropped: {dropped}")
        initial_rows = len(df) # Reset baseline for next filter step

    # 2. Process Substring Matches (Case-Insensitive)
    if "contains" in criteria and criteria["contains"]:
        # Escape special regex characters in config strings to prevent regex interpretation
        safe_substrings = [re.escape(str(x)) for x in criteria["contains"]]
        pattern = '|'.join(safe_substrings)
        
        mask = df[column_name].astype(str).str.contains(pattern, case=False, na=False)
        df = df[~mask]
        
        dropped = initial_rows - len(df)
        logger.info(f"Filter [Contains] applied on '{column_name}' | Criteria: {criteria['contains']} | Rows dropped: {dropped}")
        
    return df

def run_pipeline(config: Dict[str, Any]) -> None:
    """Main orchestration function for the data processing pipeline."""
    logger.info("=== Starting Apptio Inventory Filtering Pipeline ===")
    
    # Extract config parameters
    input_path = config["io"]["input_path"]
    output_path = config["io"]["output_path"]
    col_map = config["columns"]
    filters = config["filters"]
    
    # 1. Load Data
    try:
        logger.info(f"Attempting to load data from '{input_path}'...")
        df = pd.read_excel(input_path)
    except FileNotFoundError:
        logger.critical(f"File not found: '{input_path}'. Please check the config path.")
        sys.exit(1)
    except Exception as e:
        logger.critical(f"Failed to load Excel file: {e}")
        sys.exit(1)
        
    initial_row_count = len(df)
    logger.info(f"Data loaded successfully. Initial row count: {initial_row_count}")

    # 2. Pre-Flight Validations
    validate_columns(df, col_map)

    # 3. Apply Filters Iteratively
    logger.info("Initiating filtering sequences...")
    for logical_name, column_filter_criteria in filters.items():
        actual_column_name = col_map.get(logical_name)
        if not actual_column_name:
            continue
            
        df = apply_exclusions(df, actual_column_name, column_filter_criteria)

    # 4. Final State Logging
    final_row_count = len(df)
    total_dropped = initial_row_count - final_row_count
    logger.info(f"Filtering complete. Total rows dropped: {total_dropped}")
    logger.info(f"Final dataset row count: {final_row_count}")

    # 5. Export Data
    try:
        logger.info(f"Exporting filtered dataset to '{output_path}'...")
        df.to_excel(output_path, index=False)
        logger.info("=== Pipeline Execution Completed Successfully ===")
    except Exception as e:
        logger.critical(f"Failed to write output file: {e}")
        sys.exit(1)

# ==========================================
# EXECUTION ENTRY POINT
# ==========================================
if __name__ == "__main__":
    run_pipeline(PIPELINE_CONFIG)
