import os
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv


class SimpleOmopDB:
    def __init__(self, env_var: str = "DB_CONNECTION_STRING", env_path: str = ".env"):
        load_dotenv(dotenv_path=env_path)

        self.uri = os.getenv(env_var)
        if not self.uri or not self.uri.startswith("postgresql://"):
            raise ValueError(f"Expected PostgreSQL URI format in env var '{env_var}'")

        try:
            self.conn = psycopg2.connect(self.uri)
        except Exception as e:
            raise ConnectionError(f"Failed to connect to PostgreSQL: {str(e)}")

    def run_query(self, sql: str, row_limit: int = 1000) -> str:
        try:
            query = f"{sql.strip().rstrip(';')} LIMIT {row_limit};"
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
                df = pd.DataFrame(results)
                return df.to_csv(index=False)
        except Exception as e:
            raise RuntimeError(f"Query execution failed: {str(e)}")

db = SimpleOmopDB()
print(db.run_query("SELECT * FROM person"))
