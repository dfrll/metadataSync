#! /usr/bin/env python3
import os
import asyncio
import logging
import tempfile
from pathlib import Path
from sqlalchemy import create_engine
import polars as pl
import subprocess
import sqlite3

log = logging.getLogger(__name__)
TABLE_NAME = "metadata"
MAX_SQL_BYTES = 80_000  # leave margin for D1 100 KB limit


def sanitize_col(col: str) -> str:
    """Make column name safe for SQLite/D1."""
    col = col.replace("-", "_")
    return f'"{col}"'


def write_sqlite(df: pl.DataFrame, db_path: Path) -> None:
    """Write a Polars DataFrame into a SQLite database."""
    log.info("Writing %d rows to SQLite: %s", df.height, db_path)
    engine = create_engine(f"sqlite:///{db_path}")
    df.write_database(
        table_name=TABLE_NAME,
        connection=engine,
        if_table_exists="replace",
        engine="sqlalchemy",
    )


def dump_to_sql_text(db_path: Path, table_name: str = TABLE_NAME) -> str:
    """Dump SQLite DB into D1-compatible SQL text with batched INSERTs."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    lines = [f"DROP TABLE IF EXISTS {table_name};"]

    # Get columns
    cur.execute(f"PRAGMA table_info({table_name});")
    cols = [row[1] for row in cur.fetchall()]
    col_defs = ", ".join(f"{sanitize_col(c)} TEXT" for c in cols)
    lines.append(f"CREATE TABLE {table_name} ({col_defs});")

    # Insert data in batches by SQL size
    cur.execute(f"SELECT * FROM {table_name};")
    batch = []
    current_size = 0
    for row in cur.fetchall():
        values = []
        for v in row:
            if v is None:
                values.append("NULL")
            else:
                escaped = str(v).replace("'", "''")
                values.append(f"'{escaped}'")
        row_sql = f"({', '.join(values)})"
        row_len = len(row_sql.encode("utf-8"))

        if current_size + row_len > MAX_SQL_BYTES and batch:
            lines.append(f"INSERT INTO {table_name} VALUES {', '.join(batch)};")
            batch = []
            current_size = 0

        batch.append(row_sql)
        current_size += row_len

    if batch:
        lines.append(f"INSERT INTO {table_name} VALUES {', '.join(batch)};")

    conn.close()
    return "\n".join(lines)


def upload_sql_to_d1(db_name: str, sql_path: Path, api_token: str) -> None:
    """Upload SQL text file to D1 via wrangler."""
    log.info("Uploading SQL file to D1: %s", db_name)
    env = {"PATH": os.environ["PATH"], "CLOUDFLARE_API_TOKEN": api_token}
    result = subprocess.run(
        [
            "npx",
            "wrangler",
            "d1",
            "execute",
            db_name,
            "--remote",
            f"--file={sql_path}",
            "--yes",
        ],
        env=env,
        capture_output=True,
        text=True,
    )

    log.info("wrangler returncode: %s", result.returncode)
    if result.stdout:
        log.info("wrangler stdout: %s", result.stdout)
    if result.stderr:
        log.info("wrangler stderr: %s", result.stderr)

    if result.returncode != 0:
        raise RuntimeError("wrangler upload failed")
    log.info("D1 upload complete")


async def import_to_d1(df: pl.DataFrame, db_name: str, api_token: str) -> None:
    """Full pipeline: Polars → SQLite → SQL text → D1"""
    with (
        tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_db,
        tempfile.NamedTemporaryFile(suffix=".sql", delete=False, mode="w") as tmp_sql,
    ):
        db_path = Path(tmp_db.name)
        sql_path = Path(tmp_sql.name)

    log.info("Writing temporary SQLite DB: %s", db_path)
    write_sqlite(df, db_path)

    log.info("Dumping SQLite to SQL text file: %s", sql_path)
    sql_text = dump_to_sql_text(db_path)
    sql_path.write_text(sql_text, encoding="utf-8")

    await asyncio.to_thread(upload_sql_to_d1, db_name, sql_path, api_token)

    db_path.unlink(missing_ok=True)
    sql_path.unlink(missing_ok=True)
    log.info("D1 import finished (%d rows)", df.height)


def ensure_d1(db_name: str, api_token: str, account_id: str) -> None:
    """Create a D1 database if it doesn't already exist."""

    env = os.environ.copy()
    env.update(
        {
            "CLOUDFLARE_API_TOKEN": api_token,
            "CLOUDFLARE_ACCOUNT_ID": account_id,
        }
    )

    result = subprocess.run(
        ["npx", "-y", "wrangler", "d1", "create", db_name],
        env=env,
        capture_output=True,
        text=True,
        timeout=180,
    )

    stdout = result.stdout or ""
    stderr = result.stderr or ""

    # treat "already exists" as success
    if "A database with that name already exists" in stderr:
        log.warning("D1 database '%s' already exists (skipping create)", db_name)
        return

    # real failure
    if result.returncode != 0:
        raise RuntimeError(
            f"Wrangler failed:\n\nSTDOUT:\n{stdout}\n\nSTDERR:\n{stderr}"
        )

    log.info("Created D1 database '%s'", db_name)
