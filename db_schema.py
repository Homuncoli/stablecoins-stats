import psycopg
from pathlib import Path

from model.Block import Block

CREATE_MIGRATIONS_TABLE = """
CREATE TABLE IF NOT EXISTS migrations (
    id SERIAL PRIMARY KEY,
    filename TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""
CREATE_VERSION_TABLE = """
CREATE TABLE IF NOT EXISTS version (
    current TEXT PRIMARY KEY
);
INSERT INTO version (current) VALUES ('0000_initial') ON CONFLICT (current) DO NOTHING;
"""

def get_current_version(cursor):
    cursor.execute("SELECT current FROM version LIMIT 1")
    row = cursor.fetchone()
    return row[0] if row else None

def update_version(cursor, filename):
    cursor.execute("UPDATE version SET current = %s;", (filename,))
    cursor.execute("INSERT INTO migrations (filename) VALUES (%s);", (filename,))

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
    print("Ensuring database schema is up to date...")
    with conn.cursor() as cur:
        cur.execute(CREATE_MIGRATIONS_TABLE)
        cur.execute(CREATE_VERSION_TABLE)

        sql_files = get_pending_files(get_current_version(conn.cursor()), dir)

        for sql_file in sql_files:
            print(f"Running migration: {sql_file.name}")
            run_migration(conn, sql_file)
    conn.commit()
    print("Database schema is up to date.")
