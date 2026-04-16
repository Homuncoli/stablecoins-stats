import psycopg
from pathlib import Path
import logging

CREATE_MIGRATIONS_TABLE = """
CREATE TABLE IF NOT EXISTS migrations (
    id SERIAL PRIMARY KEY,
    filename TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""
CREATE_VERSION_TABLE = """
CREATE TABLE IF NOT EXISTS version (
    id SERIAL PRIMARY KEY,
    current TEXT NOT NULL
);
"""

ENSURE_INITIAL_VERSION = """
INSERT INTO version (current)
SELECT '0000_initial'
WHERE NOT EXISTS (SELECT 1 FROM version);
"""

logger = logging.getLogger(__name__)

def get_current_version(cursor):
    cursor.execute("SELECT current FROM version LIMIT 1")
    row = cursor.fetchone()
    return row[0] if row else None

def update_version(cursor, filename):
    cursor.execute("INSERT INTO migrations (filename) VALUES (%s);", (filename,))
    cursor.execute("UPDATE version SET current = %s;", (filename,))

def get_pending_files(current_version, sql_dir: Path = Path("migrations")) -> list[Path]:
    all_files = sorted(sql_dir.glob("*.sql"))

    if current_version is None:
        return all_files  # no version yet, run everything

    # Only return files whose name sorts after the current version
    return [f for f in all_files if f.name > current_version]

def run_migration(conn: psycopg.Connection, sql_file: Path) -> None:
    with open(sql_file, "r") as f:
        sql = f.read()
    with conn.cursor() as cur:
        cur.execute(sql)
        update_version(cur, sql_file.name)

def ensure_schema(conn: psycopg.Connection, dir: Path = Path("migrations")) -> None:
    logger.info("Ensuring database schema is up to date...")
    with conn.cursor() as cur:
        cur.execute(CREATE_MIGRATIONS_TABLE)
        cur.execute(CREATE_VERSION_TABLE)
        cur.execute(ENSURE_INITIAL_VERSION)
    conn.commit()

    with conn.cursor() as cur:
        current_version = get_current_version(cur)

    sql_files = get_pending_files(current_version, dir)

    for sql_file in sql_files:
        logger.info(f"Running migration: {sql_file.name}")
        try:
            run_migration(conn, sql_file)
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.fatal(f"Error occurred while running migration {sql_file.name}: {e}")
            exit(1)

    logger.info("Database schema is up to date.")

def copy_binary_rows(cur, buffer: list[tuple], target_table: str, columns: str, type_names: list[str]):
    if not buffer:
        return

    with cur.copy(f"COPY {target_table} ({columns}) FROM STDIN WITH (FORMAT BINARY)") as copy:
        copy.set_types(type_names)
        for row in buffer:
            copy.write_row(row)