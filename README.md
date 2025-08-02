# NOSTROS - SQL Query Template Renderer

NOSTROS converts template-based SQL queries into executable SQL for OMOP Common Data Model (CDM) databases.

## What it does

Takes queries with placeholders like `<CONDITION-TEMPLATE><ARG-CONDITION><0>` and converts them to real SQL by:
- Replacing templates with actual SQL subqueries
- Filling in medical codes (drugs, conditions, demographics)
- Generating executable SQL queries

## Quick Start

1. **Setup**
   ```bash
   pip install pandas sqlglot psycopg2-binary python-dotenv
   ```

2. **Configure database** - Edit `src/nostros/config.py` with your OMOP CDM connection details

3. **Run**
   ```bash
   python main.py
   ```

This processes all templates in `data/nostros_query.csv` and outputs SQL files to the `output/` folder.

## Example

**Template:**
```sql
SELECT COUNT(DISTINCT con1.person_id) 
FROM <SCHEMA>.condition_occurrence con1 
JOIN <CONDITION-TEMPLATE><ARG-CONDITION><0> ON con1.condition_concept_id=concept_id
```

**Rendered SQL:**
```sql
SELECT COUNT(DISTINCT con1.person_id) 
FROM public.condition_occurrence con1 
JOIN (SELECT concept_id FROM concept WHERE vocabulary_id='ICD10CM' AND concept_code='E11') temp_cond0 
ON con1.condition_concept_id=temp_cond0.concept_id;
```

## Template Types

| Template | Description | Example |
|----------|-------------|---------|
| `<CONDITION-TEMPLATE><ARG-CONDITION><0>` | Medical conditions | ICD10CM codes |
| `<DRUG-TEMPLATE><ARG-DRUG><0>` | Medications | RxNorm codes |
| `<GENDER-TEMPLATE><ARG-GENDER><0>` | Patient gender | MALE, FEMALE |
| `<RACE-TEMPLATE><ARG-RACE><0>` | Patient race | White, Asian, etc. |
| `<ARG-AGE><0>` | Age values | 65, 25, 45 |
| `<ARG-TIMEDAYS><0>` | Time periods | 30, 90, 365 days |

## Project Structure

```
nostros/
├── main.py                         # Main processing script
├── data/
│   ├── nostros_query.csv          # Template queries dataset
│   ├── test.csv                   # Test dataset
│   ├── train.csv                  # Training dataset
│   └── validation.csv             # Validation dataset
├── src/
│   ├── nostros/
│   │   ├── config.py              # Database configuration and connections
│   │   ├── sql_processing.py      # Core template rendering logic
│   │   ├── rendering_functions.py # Domain-specific template functions
│   │   └── template_definitions.py # SQL template definitions
│   ├── transpiler/
│   │   └── transpiler.py          # SQL dialect transpilation (Redshift↔PostgreSQL)
│   └── data_processing/
│       └── data_processing.py     # Data processing utilities
├── output/                        # Generated SQL files
│   ├── rendered_queries.sql       # Executable SQL queries
│   ├── rendered_queries_postgres.sql # PostgreSQL-compatible queries
│   └── rendered_queries.json      # Processing details and errors
├── .env                          # Environment variables (database config)
└── README.md
```

## Output

- `output/rendered_queries.sql` - Executable SQL queries
- `output/rendered_queries.json` - Processing details and errors

## Requirements
◊
- Python 3.8+
- pandas, sqlglot, psycopg2, python-dotenv

## License

Includes code with copyright from Amazon.com, Inc. licensed under CC-BY-NC-4.0.