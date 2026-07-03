"""Configuration + Databricks connection helpers.

Everything here is designed to run from a laptop (Windows/Mac/Linux) with no local
Spark. SQL is executed on the serverless SQL warehouse via the Databricks
**Statement Execution API** (part of `databricks-sdk`), so there is no dependency on
`databricks-sql-connector` / `thrift` (which some corporate proxies block).
"""
from __future__ import annotations

import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

# Load .env from the repo root regardless of where the script is invoked from.
_REPO_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(_REPO_ROOT / ".env")

REPO_ROOT = _REPO_ROOT
LOCAL_LANDING = _REPO_ROOT / "data" / "synthetic"


@dataclass(frozen=True)
class Settings:
    host: str
    token: str
    http_path: str
    catalog: str
    schema: str
    volume: str

    @property
    def warehouse_id(self) -> str:
        # http_path looks like "/sql/1.0/warehouses/<id>"
        return self.http_path.rstrip("/").split("/")[-1]

    @property
    def full_schema(self) -> str:
        return f"{self.catalog}.{self.schema}"

    @property
    def volume_path(self) -> str:
        """UC Volume path usable from SQL read_files() / Files API."""
        return f"/Volumes/{self.catalog}/{self.schema}/{self.volume}"


def _require(name: str) -> str:
    val = os.environ.get(name, "").strip()
    if not val:
        raise RuntimeError(
            f"Missing required env var {name}. Copy .env.example to .env and fill it in."
        )
    return val


def load_settings() -> Settings:
    host = _require("DATABRICKS_HOST")
    if not host.startswith("http"):
        host = "https://" + host
    return Settings(
        host=host.rstrip("/"),
        token=_require("DATABRICKS_TOKEN"),
        http_path=_require("DATABRICKS_HTTP_PATH"),
        catalog=os.environ.get("DQ_CATALOG", "people_org").strip(),
        schema=os.environ.get("DQ_SCHEMA", "dq_observability").strip(),
        volume=os.environ.get("DQ_VOLUME", "landing").strip(),
    )


def get_client(settings: Settings | None = None):
    """Return an authenticated WorkspaceClient."""
    from databricks.sdk import WorkspaceClient

    s = settings or load_settings()
    return WorkspaceClient(host=s.host, token=s.token)


@dataclass
class SqlResult:
    columns: list[str]
    rows: list[list[Any]]

    def scalar(self) -> Any:
        return self.rows[0][0] if self.rows and self.rows[0] else None

    def dicts(self) -> list[dict[str, Any]]:
        return [dict(zip(self.columns, r)) for r in self.rows]


def run_sql(
    stmt: str,
    settings: Settings | None = None,
    client=None,
    poll_seconds: float = 2.0,
    timeout_seconds: float = 300.0,
) -> SqlResult:
    """Execute a single SQL statement on the serverless warehouse and return results.

    Blocks (polling) until the statement reaches a terminal state.
    """
    from databricks.sdk.service.sql import StatementState

    s = settings or load_settings()
    w = client or get_client(s)

    resp = w.statement_execution.execute_statement(
        warehouse_id=s.warehouse_id,
        statement=stmt,
        catalog=s.catalog,
        schema=s.schema,
        wait_timeout="30s",
    )

    deadline = time.monotonic() + timeout_seconds
    while resp.status and resp.status.state in (
        StatementState.PENDING,
        StatementState.RUNNING,
    ):
        if time.monotonic() > deadline:
            w.statement_execution.cancel_execution(resp.statement_id)
            raise TimeoutError(f"SQL statement timed out after {timeout_seconds}s")
        time.sleep(poll_seconds)
        resp = w.statement_execution.get_statement(resp.statement_id)

    state = resp.status.state if resp.status else None
    if state != StatementState.SUCCEEDED:
        err = resp.status.error.message if (resp.status and resp.status.error) else state
        raise RuntimeError(f"SQL failed ({state}): {err}\n--- statement ---\n{stmt}")

    columns: list[str] = []
    if resp.manifest and resp.manifest.schema and resp.manifest.schema.columns:
        columns = [c.name for c in resp.manifest.schema.columns]

    rows: list[list[Any]] = []
    if resp.result and resp.result.data_array:
        rows = [list(r) for r in resp.result.data_array]

    return SqlResult(columns=columns, rows=rows)


def run_many(stmts: list[str], settings: Settings | None = None, client=None) -> None:
    """Run a list of statements sequentially (DDL, etc.)."""
    s = settings or load_settings()
    w = client or get_client(s)
    for stmt in stmts:
        clean = stmt.strip()
        if clean:
            run_sql(clean, settings=s, client=w)
