# Directory route query performance

The November 2026 incident showed ten agency slug lookups and six route-list
queries active concurrently, some for more than four minutes. Birch's SQLx pool
has sixteen connections shared by directory and tile requests. The OCTA literal
query completed in 4.13 ms, but its plan recalculated feed agency counts 56 times.
That observation identifies a scaling risk, not a complete reproduction of the
production incident or proof of its sole cause.

Agency cards now fetch routes and slugs in one query. Target memberships and
feed agency counts are materialized. Membership uses an exact
chateau/feed/attempt/agency existence check, so multiple target agencies cannot
multiply candidate route rows. The candidate rows are materialized once and
reused to select cards and canonical slugs. Slug preference and raw route-ID
fallback are preserved. Cards can still select a slug from another physical copy
of the same route ID, as before.

The route list now includes attempt_id in membership and feed counts, matching
the existing slug resolver. Retained attempts are still eligible; this change
does not introduce production-only filtering or delete historical data. Equal
card sort keys use feed and attempt as deterministic tie breakers.

Both expensive queries run in a transaction with SET LOCAL statement_timeout =
'5s'. PostgreSQL cancels statements exceeding that limit, even if the requesting
client has gone away. Existing directory error handling returns an error for a
timeout. SQLx rolls back transactions on error, and the local setting does not
leak to other pool users. The limit bounds statement execution, not time spent
waiting to acquire a pooled connection. It does not cancel statements already
running in old server processes before deployment.

## Regression checks

Install psycopg[binary] and run against a PostgreSQL database with CREATE SCHEMA
permission:

```sh
python -m pip install 'psycopg[binary]>=3.2,<4'
DIRECTORY_TEST_DATABASE_URL=postgres://localhost/directory_test \
  python tests/test_directory_queries.py
```

Tests create and remove a uniquely named schema and execute the same SQL files
included in Birch. They cover attempt/chateau isolation, single-agency fallback,
multiple memberships, retained physical copies, slug preference, and missing
agencies. The large-feed case has 11,178 routes and 458 agencies and asserts that
feed counts execute exactly once under both forced custom and generic plans.
CI runs these tests on PostgreSQL 16 and 18. Timing is printed for inspection;
there is no machine-dependent latency assertion.

## Deployment verification

After restarting Birch with this change, compare directory latency on affected
large agencies, active directory statements, and tile latency. Obtain actual
plans for the affected agency values if requests remain slow. Cached application
plans, traffic volume, and system resource contention remain possible contributors.
No index migration or PostgreSQL-wide configuration change is required.
