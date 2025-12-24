import os
import sys
from pathlib import Path

import psycopg2
from psycopg2.extensions import connection

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from src.utils.config import load_project_dotenv

load_project_dotenv()


def get_pg_connection() -> connection:
    """
    Простейшее подключение к Postgres.
    Конфиг берём из ENV:
      PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD
    """
    conn = psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=os.getenv("PGPORT", "5432"),
        dbname=os.getenv("PGDATABASE", "tobd"),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", "postgres"),
    )
    conn.autocommit = False
    return conn
