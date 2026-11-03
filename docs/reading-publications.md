# Reading one published dataset consistently

`analytics` views are convenient live names for individual current queries. They
are redirected when publication commits. They are not a version-pinning API for
an already established Repeatable Read or Serializable snapshot. PostgreSQL can
expose a new relation through catalog lookup while its newly inserted rows remain
invisible to that older snapshot. Starting a transaction alone does not fix this.
See PostgreSQL's [MVCC caveats](https://www.postgresql.org/docs/16/mvcc-caveats.html).

Reports spanning multiple statements must first select a visible publication
ledger row and use that row's retained physical schema on the same connection.
The publisher never modifies those physical models after publication. Retention
is currently indefinite, so an in-progress pinned report does not lose its tables.

## Python API

```python
from psycopg import sql
from commerce.reader import pinned_publication

with pinned_publication() as report:
    print(report.publication_id, report.schema)
    sales = report.query("sales")
    refunds = report.query("refunds")
    lines = report.connection.execute(
        sql.SQL("select count(*) from {}").format(report.relation("fct_order_line"))
    ).fetchone()
```

Without a supplied connection, the helper opens a read-only Repeatable Read
transaction. You can supply an existing connection, including one whose snapshot
was established before a newer publication committed. Metadata lookup then uses
that existing snapshot and selects its visible publication. The helper preserves
the caller's connection and outer transaction; it does not close that connection.
It qualifies model identifiers safely and limits `relation()` to the physical
models recorded in the selected publication. The five named SQL examples are
automatically qualified against that same schema.

```python
with pinned_publication(connection=my_connection, publication_id=previous_id) as report:
    previous_sales = report.query("sales")
```

An explicit publication absent from the current snapshot raises an error instead
of returning an empty dataset or silently substituting a different version.

## SQL and command-line consumers

In `psql`, select the schema and query it within the same transaction:

```sql
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SELECT publication_id, schema_name AS publication_schema
FROM meta.publications ORDER BY publication_id DESC LIMIT 1 \gset
SELECT * FROM :"publication_schema".mart_daily_location_sales;
SELECT * FROM :"publication_schema".mart_refund_rate;
COMMIT;
```

Other SQL clients should bind/quote the selected schema as an identifier using
their driver's API. Do not choose metadata on a separate connection and then use
it inside an older snapshot: that can select a publication invisible to it.

The application CLI pins all named analytical queries. Separate CLI invocations
can specify the same publication ID when a report needs several outputs:

```sh
commerce publication
commerce query --name sales --publication-id 1
commerce query --name inventory --publication-id 1
commerce query --name changes --publication-id 1
```

Replace `1` with an actual publication ID. The changes query compares that
publication with its preceding published version and retains source evidence.

