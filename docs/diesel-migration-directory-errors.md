# Diesel migration directory errors

Run from the repository root:

    python3 tools/check_diesel_migrations.py

This is a read-only preflight and needs no PostgreSQL connection.
It reports the exact path for missing or unreadable up.sql, unexpected
directory names, and duplicate normalized migration versions. It accepts
both historical timestamp formats, with and without the four-digit suffix.
Diesel ignores hyphens in the version portion, but adding four digits
changes the recorded version. Never rename an applied migration in a way
that changes its normalized version.

If a local untracked directory is reported, inspect it and move it outside
migrations rather than deleting it. Check tracked files with git status
before restoring any missing files. Do not reset the database or edit
__diesel_schema_migrations to resolve a directory-layout error.

If the check passes, verify the installed Diesel CLI version and explicitly
select the repository migration directory:

    diesel --version
    diesel migration list --migration-dir migrations

This preflight checks layout, not SQL correctness or database state.
