"""
Main script to translate query templates from nostros_query.csv into proper SQL queries.
This script processes all template queries and converts them to executable SQL statements.
"""

import pandas as pd
import json
import re
from typing import Dict, List, Any
import src.nostros.config as config

from src.nostros.sql_processing import render_template_query
from src.nostros.rendering_functions import (
    render_gender_template,
    render_condition_template,
    render_drug_template,
)

from src.nostros.config import get_db_connection


def create_sample_args_dict() -> Dict[str, List[Dict[str, Any]]]:
    """
    Creates sample arguments dictionary for testing template rendering.
    In a real scenario, these would come from entity detection and argument processing.
    """
    return {
        "DRUG": [
            {"Query-arg": "1154343"},  # Example RxNorm code for aspirin
            {"Query-arg": "1191"},     # Example RxNorm code for ibuprofen
            {"Query-arg": "2670"},     # Example RxNorm code for acetaminophen
            {"Query-arg": "8782"},     # Example RxNorm code for metformin
        ],
        "CONDITION": [
            {"Query-arg": "E11"},      # Type 2 diabetes mellitus (ICD10CM)
            {"Query-arg": "I25.10"},   # Atherosclerotic heart disease (ICD10CM) 
            {"Query-arg": "J45"},      # Asthma (ICD10CM)
            {"Query-arg": "M79.3"},    # Panniculitis (ICD10CM)
        ],
        "RACE": [
            {"Query-arg": "White"},
            {"Query-arg": "Black or African American"},
            {"Query-arg": "Asian"},
            {"Query-arg": "American Indian or Alaska Native"},
        ],
        "GENDER": [
            {"Query-arg": "FEMALE"},
            {"Query-arg": "MALE"},
        ],
        "ETHNICITY": [
            {"Query-arg": "Hispanic or Latino"},
            {"Query-arg": "Not Hispanic or Latino"},
        ],
        "STATE": [
            {"Query-arg": "CA"},
            {"Query-arg": "NY"},
            {"Query-arg": "TX"},
            {"Query-arg": "FL"},
        ],
        "TIMEDAYS": [
            {"Query-arg": "30"},
            {"Query-arg": "90"},
            {"Query-arg": "180"},
            {"Query-arg": "365"},
        ],
        "TIMEYEARS": [
            {"Query-arg": "2020"},
            {"Query-arg": "2021"},
            {"Query-arg": "2022"},
            {"Query-arg": "1950"},
        ],
        "AGE": [
            {"Query-arg": "65"},
            {"Query-arg": "18"},
            {"Query-arg": "25"},
            {"Query-arg": "45"},
        ]
    }


def create_sample_args_dict_from_db(conn):
    """
    Fetch concept codes for drugs, conditions, race, gender, ethnicity,
    states, timeyears, and age from the OMOP DB and return a dictionary
    structured for use with template rendering.
    """
    def fetch_codes(query):
        with conn.cursor() as cur:
            cur.execute(query)
            return [row[0] for row in cur.fetchall()]

    def wrap_query_args(codes):
        return [{"Query-arg": str(code)} for code in codes if code is not None]

    drug_codes = fetch_codes("""
        SELECT concept_code
        FROM concept
        WHERE vocabulary_id = 'RxNorm'
          AND standard_concept = 'S'
        LIMIT 50
    """)

    condition_codes = fetch_codes("""
        SELECT concept_code
        FROM concept
        WHERE vocabulary_id = 'ICD10CM'
          AND standard_concept = 'S'
        LIMIT 50
    """)

    race_codes = fetch_codes("""
        SELECT concept_code
        FROM concept
        WHERE vocabulary_id = 'Race'
          AND standard_concept = 'S'
        LIMIT 20
    """)

    gender_codes = fetch_codes("""
        SELECT concept_code
        FROM concept
        WHERE vocabulary_id = 'Gender'
          AND standard_concept = 'S'
        LIMIT 10
    """)

    ethnicity_codes = fetch_codes("""
        SELECT concept_code
        FROM concept
        WHERE vocabulary_id = 'Ethnicity'
          AND standard_concept = 'S'
        LIMIT 10
    """)

    state_names = fetch_codes("""
        SELECT DISTINCT location.state
        FROM location
        WHERE location.state IS NOT NULL
        LIMIT 20
    """)

    years = fetch_codes("""
        SELECT DISTINCT EXTRACT(YEAR FROM observation_period_start_date)::INT AS year
        FROM observation_period
        ORDER BY year DESC
        LIMIT 20
    """)

    ages = fetch_codes("""
        SELECT DISTINCT EXTRACT(YEAR FROM CURRENT_DATE) - year_of_birth AS age
        FROM person
        WHERE year_of_birth IS NOT NULL
        ORDER BY age
        LIMIT 20
    """)

    # Optional: static values for TIMEDAYS if not in DB
    timedays = [30, 90, 180, 365]

    return {
        "DRUG": wrap_query_args(drug_codes),
        "CONDITION": wrap_query_args(condition_codes),
        "RACE": wrap_query_args(race_codes),
        "GENDER": wrap_query_args(gender_codes),
        "ETHNICITY": wrap_query_args(ethnicity_codes),
        "STATE": wrap_query_args(state_names),
        "TIMEYEARS": wrap_query_args([int(y) for y in years]),
        "AGE": wrap_query_args([int(a) for a in ages]),
        "TIMEDAYS": wrap_query_args(timedays),
    }


def identify_required_args(query: str) -> Dict[str, int]:
    """
    Identifies the required arguments and their maximum indices for a given query template.
    
    Args:
        query: SQL template query string
        
    Returns:
        Dictionary mapping argument types to their maximum index + 1
    """
    required_args = {}
    
    # Find template-based placeholders: <TEMPLATE><ARG-TYPE><INDEX>
    template_pattern = r'<(\w+)-TEMPLATE><ARG-(\w+)><(\d+)>'
    template_matches = re.findall(template_pattern, query)
    
    for template_type, arg_type, index in template_matches:
        idx = int(index)
        if arg_type not in required_args:
            required_args[arg_type] = idx + 1
        else:
            required_args[arg_type] = max(required_args[arg_type], idx + 1)
    
    # Find argument-only placeholders: <ARG-TYPE><INDEX>
    arg_pattern = r'<ARG-(\w+)><(\d+)>'
    arg_matches = re.findall(arg_pattern, query)
    
    for arg_type, index in arg_matches:
        idx = int(index)
        if arg_type not in required_args:
            required_args[arg_type] = idx + 1
        else:
            required_args[arg_type] = max(required_args[arg_type], idx + 1)
    
    return required_args


def create_args_dict_for_query(query: str, sample_args: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Creates an arguments dictionary tailored for a specific query.
    
    Args:
        query: SQL template query string
        sample_args: Complete sample arguments dictionary
        
    Returns:
        Arguments dictionary with only the required arguments for this query
    """
    required_args = identify_required_args(query)
    query_args = {}
    
    for arg_type, count in required_args.items():
        if arg_type in sample_args:
            # Take only the required number of arguments
            query_args[arg_type] = sample_args[arg_type][:count]
        else:
            print(f"Warning: Missing sample data for argument type '{arg_type}'")
            # Create placeholder arguments
            query_args[arg_type] = [{"Query-arg": f"PLACEHOLDER_{i}"} for i in range(count)]
    
    return query_args


def process_queries(csv_file: str, output_file: str = "rendered_queries.sql"):
    """
    Processes all queries from the CSV file and renders them into proper SQL.
    
    Args:
        csv_file: Path to the CSV file containing query templates
        output_file: Path to output file for rendered SQL queries
    """

    conn = get_db_connection()

    try:
        # Read the CSV file
        df = pd.read_csv(csv_file)
        print(f"Loaded {len(df)} queries from {csv_file}")
        
        # Create sample arguments
        sample_args = create_sample_args_dict_from_db(conn)
        # sample_args = create_sample_args_dict()

        print(sample_args)

        rendered_queries = []
        successful_renders = 0
        failed_renders = 0
        
        # Process each query
        for idx, row in df.iterrows():
            query_template = row['query']
            print(f"\nProcessing query {idx + 1}/{len(df)}")
            print(f"Template: {query_template[:100]}{'...' if len(query_template) > 100 else ''}")
            
            try:
                # Create arguments dictionary for this specific query
                query_args = create_args_dict_for_query(query_template, sample_args)
                
                # Render the query
                rendered_query = render_template_query(config, query_template, query_args)
                
                rendered_queries.append({
                    'query_id': idx + 1,
                    'original_template': query_template,
                    'rendered_query': rendered_query,
                    'required_args': identify_required_args(query_template),
                    'status': 'success'
                })
                
                successful_renders += 1
                print("✓ Successfully rendered")
                
            except Exception as e:
                print(f"✗ Error rendering query: {str(e)}")
                rendered_queries.append({
                    'query_id': idx + 1,
                    'original_template': query_template,
                    'rendered_query': None,
                    'error': str(e),
                    'status': 'failed'
                })
                failed_renders += 1
        
        # Save results
        save_results(rendered_queries, output_file)
        
        # Print summary
        print(f"\n{'='*60}")
        print(f"PROCESSING SUMMARY")
        print(f"{'='*60}")
        print(f"Total queries processed: {len(df)}")
        print(f"Successfully rendered: {successful_renders}")
        print(f"Failed to render: {failed_renders}")
        print(f"Success rate: {(successful_renders/len(df)*100):.1f}%")
        print(f"Results saved to: {output_file}")
        
    except Exception as e:
        print(f"Error processing queries: {str(e)}")
        raise


def save_results(rendered_queries: List[Dict], output_file: str):
    """
    Saves the rendered queries to both SQL and JSON formats.
    
    Args:
        rendered_queries: List of query dictionaries
        output_file: Base output filename
    """
    import os
    
    # Ensure output directory exists
    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)
    
    # Save as SQL file
    sql_file = os.path.join(output_dir, output_file)
    with open(sql_file, 'w') as f:
        f.write("-- Rendered SQL Queries from nostros_query.csv\n")
        f.write("-- Generated by NOSTROS query translator\n\n")
        
        for query_data in rendered_queries:
            f.write(f"-- Query ID: {query_data['query_id']}\n")
            f.write(f"-- Status: {query_data['status']}\n")
            f.write(f"-- Original Template:\n-- {query_data['original_template']}\n")
            
            if query_data['status'] == 'success':
                f.write(f"-- Required Arguments: {query_data.get('required_args', {})}\n")
                rendered_query = query_data['rendered_query']
                # Only add semicolon if the query doesn't already end with one
                if not rendered_query.rstrip().endswith(';'):
                    rendered_query += ';'
                f.write(f"{rendered_query}\n\n")
            else:
                f.write(f"-- Error: {query_data.get('error', 'Unknown error')}\n\n")
            
            f.write("-" * 80 + "\n\n")
    
    # Save as JSON file for detailed analysis
    json_file = os.path.join(output_dir, output_file.replace('.sql', '.json'))
    with open(json_file, 'w') as f:
        json.dump(rendered_queries, f, indent=2)
    
    print(f"Saved SQL queries to: {sql_file}")
    print(f"Saved detailed results to: {json_file}")


def test_individual_templates():
    """
    Test individual template rendering functions with sample data.
    """
    print("Testing individual template functions:")
    print("-" * 40)
    
    schema = config.SCHEMA
    
    # Test gender template
    try:
        gender_template = render_gender_template(schema, "FEMALE")
        print(f"Gender template (FEMALE):\n{gender_template}\n")
    except Exception as e:
        print(f"Error with gender template: {e}\n")
    
    # Test condition template
    try:
        condition_template = render_condition_template(schema, "E11")
        print(f"Condition template (E11 - Diabetes):\n{condition_template}\n")
    except Exception as e:
        print(f"Error with condition template: {e}\n")
    
    # Test drug template
    try:
        drug_template = render_drug_template(schema, "1154343")
        print(f"Drug template (1154343):\n{drug_template}\n")
    except Exception as e:
        print(f"Error with drug template: {e}\n")


if __name__ == "__main__":
    print("NOSTROS Query Translator")
    print("=" * 50)
    
    # Process all queries from CSV
    print("\n2. Processing all queries from nostros_query.csv:")
    process_queries("data/nostros_query.csv", "rendered_queries.sql")