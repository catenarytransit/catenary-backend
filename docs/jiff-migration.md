# Jiff migration status

All application IANA timezone lookups now use the `catenary-time` bridge backed
by Jiff and an explicitly bundled `jiff-tzdb`. The database is vendored because
the latest published jiff-tzdb 0.1.9 contains 2026e; this tree regenerates its
bundle from IANA 2026f. Host zoneinfo never overrides this data.

`python3 tools/update_jiff_tzdb.py 2026f` regenerates the bundle using zic built from the matching IANA code release.
The generator does not pass `-L`, so timestamps use Unix time without leap
second corrections. The generated database uses IANA's standard data, rather
than upstream jiff-tzdb's rearguard representation; civil timestamps and offsets
are unchanged by that representation choice, but DST labels can differ.
The upstream jiff-tzdb lookup implementation and licenses are retained.

Daily CI checks IANA's current version and opens an update PR when it changes.
The freshness job fails while data is stale or cannot be verified. Updates
require review, successful tests, rebuild and deployment. A running binary
cannot automatically acquire a newer embedded database. GitHub Actions must
have permission to create PRs for the scheduled update job to publish them.

## Remaining work before a complete native Jiff transition

This is an intermediate migration, not removal of Chrono. Chrono date/time
representations remain in SQL models, GTFS interfaces, serialization, calendar
arithmetic and some parsing. All application clocks use Jiff through `utc_now`,
returning Chrono values for those existing interfaces. The bridge implements
Chrono's TimeZone trait while delegating rule evaluation to Jiff. It preserves
None for a DST gap and both chronologically ordered candidates for a DST fold.
Leap-second local representations are rejected. Jiff's narrower supported date
range applies to timezone conversions; UTC conversions outside it panic.
Offset display uses numeric offsets instead of timezone abbreviations.

A full migration still needs explicit Diesel/GTFS conversion boundaries,
replacement of internal Chrono dates/durations/parsers, and compatibility tests
for existing serialized realtime and timetable data. Transitive dependencies
may continue to use Chrono independently of application code.

Deploy Gentian and all departure/routing/realtime services with the same bundle.
Regenerate timetables whose stored UTC anchors depend on changed local rules.

## MTA configuration

Set `MTA_API_KEY` for the MTA realtime request path. The previously hardcoded
header value was removed while preparing this migration.

The STO feed requires `STO_API_KEY` and `STO_API_SECRET`. The existing live
credential-dependent STO test is ignored by default. Request URLs containing
authentication parameters are no longer logged.
