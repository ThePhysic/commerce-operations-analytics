"""Pin retained physical models for coherent, multi-query analytical reads."""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from psycopg import errors, pq, sql
from psycopg.rows import dict_row

from .db import ROOT, connect

QUERY_NAMES = frozenset({"sales", "refunds", "fulfillment", "inventory", "refund_events"})


@dataclass(frozen=True)
class PublicationReader:
    connection: object
    publication: dict

    @property
    def publication_id(self):
        return self.publication["publication_id"]

    @property
    def schema(self):
        return self.publication["schema_name"]

    def relation(self, model):
        """Safely qualify a physical model recorded in this publication."""
        if model not in self.publication["row_counts"]:
            raise ValueError(f"Model is not part of publication {self.publication_id}: {model}")
        return sql.SQL("{}.{}").format(sql.Identifier(self.schema), sql.Identifier(model))

    def query(self, name):
        if name not in QUERY_NAMES:
            raise ValueError("Unknown query name")
        path = Path(ROOT) / "queries" / f"{name}.sql"
        # These are versioned, fixed SQL files, not user-provided SQL. Quote the
        # selected schema as an identifier before replacing their live qualifier.
        qualifier = sql.Identifier(self.schema).as_string(self.connection) + "."
        statement = path.read_text(encoding="utf-8").replace("analytics.", qualifier)
        with self.connection.cursor(row_factory=dict_row) as cursor:
            return cursor.execute(statement).fetchall()


@contextmanager
def pinned_publication(*, connection=None, publication_id=None):
    """Select metadata and read immutable models on the SAME connection.

    PostgreSQL exposes newly created relations to an old repeatable-read
    snapshot, while their new rows are invisible. Selecting a visible ledger row
    and pinning its physical schema avoids that race. An existing transaction
    keeps its isolation/snapshot; otherwise a read-only repeatable-read one is
    opened. The supplied connection is never closed by this helper.
    """
    owned = connection is None
    conn = connection if connection is not None else connect()
    idle = conn.info.transaction_status == pq.TransactionStatus.IDLE
    try:
        with conn.transaction():
            if idle:
                conn.execute("set transaction isolation level repeatable read read only")
            try:
                with conn.cursor(row_factory=dict_row) as cursor:
                    if publication_id is None:
                        publication = cursor.execute("select * from meta.publications order by publication_id desc limit 1").fetchone()
                    else:
                        publication = cursor.execute("select * from meta.publications where publication_id=%s", (publication_id,)).fetchone()
            except errors.UndefinedTable as exc:
                raise ValueError("No publication ledger exists; run the pipeline first") from exc
            if publication is None:
                raise ValueError("No requested publication is visible in this transaction; run the pipeline or select an existing publication")
            yield PublicationReader(conn, publication)
    finally:
        if owned:
            conn.close()

