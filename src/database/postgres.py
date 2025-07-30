import pandas as pd
import os
from typing import Dict, List, Any
import re
from io import StringIO

from src.database.db import SimpleOmopDB
from src.nostros import config  # Changed this line
from src.nostros.sql_processing import render_template_query
from src.nostros.rendering_functions import (
    render_gender_template,
    render_condition_template,
    render_drug_template,
)

# Initialize database connection with error handling
try:
    db = SimpleOmopDB()
except Exception as e:
    print(f"Database connection failed: {e}")
    exit(1)

def fetch_concepts_from_vocab(vocab_id: str, limit: int = 10):
    query = f"""
        SELECT concept_code
        FROM concept
        WHERE vocabulary_id = '{vocab_id}'
        LIMIT {limit}
    """
    try:
        csv_result = db.run_query(query)
        
        # Check if the result is empty or just headers
        if not csv_result.strip() or csv_result.strip() == "concept_code":
            print(f"Warning: No data found for vocabulary '{vocab_id}'")
            return [{"Query-arg": f"PLACEHOLDER_{vocab_id}"}]
        
        df = pd.read_csv(StringIO(csv_result))
        
        # Check if DataFrame is empty
        if df.empty:
            print(f"Warning: Empty results for vocabulary '{vocab_id}'")
            return [{"Query-arg": f"PLACEHOLDER_{vocab_id}"}]
            
        return [{"Query-arg": str(code)} for code in df["concept_code"]]
        
    except Exception as e:
        print(f"Error fetching concepts for vocabulary '{vocab_id}': {e}")
        return [{"Query-arg": f"PLACEHOLDER_{vocab_id}"}]


def create_args_dict_from_db() -> Dict[str, List[Dict[str, Any]]]:
    try:
        return {
            "DRUG": fetch_concepts_from_vocab("RxNorm"),
            "CONDITION": fetch_concepts_from_vocab("ICD10CM"),
            "RACE": fetch_concepts_from_vocab("Race"),
            "GENDER": fetch_concepts_from_vocab("Gender"),
            "ETHNICITY": fetch_concepts_from_vocab("Ethnicity"),
            "STATE": [{"Query-arg": s} for s in ["CA", "NY", "TX", "FL"]],
            "TIMEDAYS": [{"Query-arg": str(x)} for x in [30, 90, 180, 365]],
            "TIMEYEARS": [{"Query-arg": str(x)} for x in [2020, 2021, 2022, 1950]],
            "AGE": [{"Query-arg": str(x)} for x in [65, 18, 25, 45]],
        }
    except Exception as e:
        print(f"Error creating args dict from database: {e}")
        # Return fallback data
        return {
            "DRUG": [{"Query-arg": "PLACEHOLDER_DRUG"}],
            "CONDITION": [{"Query-arg": "PLACEHOLDER_CONDITION"}],
            "RACE": [{"Query-arg": "PLACEHOLDER_RACE"}],
            "GENDER": [{"Query-arg": "PLACEHOLDER_GENDER"}],
            "ETHNICITY": [{"Query-arg": "PLACEHOLDER_ETHNICITY"}],
            "STATE": [{"Query-arg": s} for s in ["CA", "NY", "TX", "FL"]],
            "TIMEDAYS": [{"Query-arg": str(x)} for x in [30, 90, 180, 365]],
            "TIMEYEARS": [{"Query-arg": str(x)} for x in [2020, 2021, 2022, 1950]],
            "AGE": [{"Query-arg": str(x)} for x in [65, 18, 25, 45]],
        }


def identify_required_args(query: str) -> Dict[str, int]:
    required_args = {}
    template_pattern = r'<(\w+)-TEMPLATE><ARG-(\w+)><(\d+)>'
    for template_type, arg_type, index in re.findall(template_pattern, query):
        idx = int(index)
        required_args[arg_type] = max(required_args.get(arg_type, 0), idx + 1)

    arg_pattern = r'<ARG-(\w+)><(\d+)>'
    for arg_type, index in re.findall(arg_pattern, query):
        idx = int(index)
        required_args[arg_type] = max(required_args.get(arg_type, 0), idx + 1)

    return required_args


def create_args_dict_for_query(query: str, all_args: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[Dict[str, Any]]]:
    required_args = identify_required_args(query)
    query_args = {}

    for arg_type, count in required_args.items():
        if arg_type in all_args:
            query_args[arg_type] = all_args[arg_type][:count]
        else:
            query_args[arg_type] = [{"Query-arg": f"PLACEHOLDER_{i}"} for i in range(count)]

    return query_args


def process_queries(csv_file: str, output_file: str = "rendered_queries.sql"):
    if not os.path.exists(csv_file):
        raise FileNotFoundError(f"CSV file not found: {csv_file}")
    
    df = pd.read_csv(csv_file)
    all_args = create_args_dict_from_db()

    rendered_queries = []
    for idx, row in df.iterrows():
        template = row['query']
        try:
            args = create_args_dict_for_query(template, all_args)
            rendered = render_template_query(config, template, args)
            rendered_queries.append(rendered)
        except Exception as e:
            print(f"Query {idx+1} failed: {e}")
            rendered_queries.append(f"-- ERROR: {e}\n-- TEMPLATE: {template}\n")

    os.makedirs("output", exist_ok=True)
    with open(f"output/{output_file}", "w") as f:
        for query in rendered_queries:
            f.write(query + "\n\n")


if __name__ == "__main__":
    print("NOSTROS Query Translator (Dynamic Version)")
    process_queries("data/nostros_query.csv")
