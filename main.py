import os
import json
import itertools
import pandas as pd
from src.nostros.config import get_db_connection
import src.nostros.config as config
from src.nostros.sql_processing import render_template_query

def create_sample_args_dict_from_db(conn):
    def fetch_codes(query):
        with conn.cursor() as cur:
            cur.execute(query)
            return [row[0] for row in cur.fetchall()]

    drug_codes = fetch_codes("""
        SELECT concept_code FROM concept
        WHERE vocabulary_id = 'RxNorm' AND standard_concept = 'S' LIMIT 5
    """)
    condition_codes = fetch_codes("""
        SELECT concept_code FROM concept
        WHERE vocabulary_id = 'ICD10CM' AND standard_concept = 'S' LIMIT 5
    """)
    race_codes = fetch_codes("""
        SELECT concept_code FROM concept
        WHERE vocabulary_id = 'Race' AND standard_concept = 'S' LIMIT 5
    """)
    gender_codes = fetch_codes("""
        SELECT concept_code FROM concept
        WHERE vocabulary_id = 'Gender' AND standard_concept = 'S' LIMIT 5
    """)
    ethnicity_codes = fetch_codes("""
        SELECT concept_code FROM concept
        WHERE vocabulary_id = 'Ethnicity' AND standard_concept = 'S' LIMIT 5
    """)
    state_names = fetch_codes("""
        SELECT DISTINCT state FROM location
        WHERE state IS NOT NULL LIMIT 5
    """)
    years = fetch_codes("""
        SELECT DISTINCT EXTRACT(YEAR FROM observation_period_start_date)::INT
        FROM observation_period LIMIT 5
    """)
    ages = fetch_codes("""
        SELECT DISTINCT EXTRACT(YEAR FROM CURRENT_DATE) - year_of_birth
        FROM person WHERE year_of_birth IS NOT NULL ORDER BY 1 LIMIT 5
    """)

    combinations = itertools.product(
        drug_codes or [None],
        condition_codes or [None],
        race_codes or [None],
        gender_codes or [None],
        ethnicity_codes or [None],
        state_names or [None],
        years or [None],
        ages or [None],
    )

    args_list = []
    for d, c, r, g, e, s, y, a in combinations:
        args = {}
        if d: args["ARG-DRUG"] = d
        if c: args["ARG-CONDITION"] = c
        if r: args["ARG-RACE"] = r
        if g: args["ARG-GENDER"] = g
        if e: args["ARG-ETHNICITY"] = e
        if s: args["ARG-STATE"] = s
        if y: args["ARG-TIMEYEAR"] = int(y)
        if a: args["ARG-AGE"] = int(a)
        args_list.append(args)

    return args_list

def process_queries(csv_file: str, output_file: str = "rendered_queries.sql"):
    conn = get_db_connection()
    try:
        df = pd.read_csv(csv_file)
        args_list = create_sample_args_dict_from_db(conn)
        rendered_queries = []
        success, failure = 0, 0

        for idx, row in df.iterrows():
            query_template = row['query']
            for i, args in enumerate(args_list):
                try:
                    wrapped_args = {k: [{"Query-arg": v}] for k, v in args.items()}
                    rendered_sql = render_template_query(config,  query_template, wrapped_args)
                    rendered_queries.append({
                        'query_id': f"{idx + 1}.{i + 1}",
                        'original_template': query_template,
                        'args_used': args,
                        'rendered_query': rendered_sql,
                        'status': 'success'
                    })
                    success += 1
                except Exception as e:
                    rendered_queries.append({
                        'query_id': f"{idx + 1}.{i + 1}",
                        'original_template': query_template,
                        'args_used': args,
                        'rendered_query': None,
                        'error': str(e),
                        'status': 'failed'
                    })
                    failure += 1

        save_results(rendered_queries, output_file)

        print("\nSummary")
        print("=" * 40)
        print(f"Total queries: {len(df)}")
        print(f"Total renders: {len(rendered_queries)}")
        print(f"Successes: {success}")
        print(f"Failures: {failure}")
        print(f"Saved to output/{output_file} and .json")

    finally:
        conn.close()

def save_results(rendered_queries, output_file):
    os.makedirs("output", exist_ok=True)
    sql_path = os.path.join("output", output_file)
    json_path = sql_path.replace(".sql", ".json")

    with open(sql_path, 'w') as f:
        for q in rendered_queries:
            f.write(f"-- Query ID: {q['query_id']}\n")
            f.write(f"-- Status: {q['status']}\n")
            if q['status'] == 'success':
                f.write(f"{q['rendered_query']}\n\n")
            else:
                f.write(f"-- Error: {q['error']}\n\n")

    with open(json_path, 'w') as f:
        json.dump(rendered_queries, f, indent=2)

if __name__ == "__main__":
    print("NOSTROS Query Translator: Rendering All Combinations")
    process_queries("data/nostros_query.csv", "rendered_queries.sql")
