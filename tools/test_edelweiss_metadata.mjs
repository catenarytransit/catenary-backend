// Run against an isolated PostgreSQL engine; no production database is accessed.
// npm install --prefix /tmp/edelweiss-test @electric-sql/pglite@0.5.15
// PGLITE_MODULE=/tmp/edelweiss-test/node_modules/@electric-sql/pglite/dist/index.js \
//   node tools/test_edelweiss_metadata.mjs
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
const { PGlite } = await import(process.env.PGLITE_MODULE || '@electric-sql/pglite');
const db = new PGlite();
const source = await readFile(new URL('../src/edelweiss/motis_response.rs', import.meta.url), 'utf8');
const sql = source.match(/const METADATA_SQL: &str = r#"([\s\S]*?)"#;/)[1];
// Geometry is deliberately stubbed: these tests cover joins, import selection,
// index eligibility and whether serialization is skipped, not PostGIS itself.
await db.exec(`
CREATE SCHEMA gtfs;
CREATE FUNCTION ST_AsGeoJSON(text, integer) RETURNS text LANGUAGE plpgsql AS $$
BEGIN
    IF $1 = 'must-not-serialize' THEN RAISE EXCEPTION 'unexpected shape serialization'; END IF;
    RETURN $1;
END $$;
CREATE TABLE gtfs.ingested_static (
    onestop_feed_id text, attempt_id text, production boolean,
    ingestion_successfully_finished boolean, ingesting_in_progress boolean,
    ingestion_errored boolean, deleted boolean, ingest_end_unix_time_ms bigint,
    PRIMARY KEY (onestop_feed_id, attempt_id)
);
CREATE TABLE gtfs.trips_compressed (
    chateau text, onestop_feed_id text, attempt_id text, trip_id text,
    route_id text, trip_short_name text, itinerary_pattern_id text, direction_id boolean,
    PRIMARY KEY (onestop_feed_id, attempt_id, trip_id)
);
CREATE INDEX trips_compressed_idx_chateau_id_trip_id ON gtfs.trips_compressed(chateau, trip_id);
CREATE TABLE gtfs.routes (
    chateau text, onestop_feed_id text, attempt_id text, route_id text,
    short_name text, long_name text, agency_id text, color text, text_color text, route_type smallint,
    PRIMARY KEY (onestop_feed_id, attempt_id, route_id)
);
CREATE INDEX idx_chateau_route ON gtfs.routes(chateau, route_id);
CREATE TABLE gtfs.agencies (
    chateau text, static_onestop_id text, attempt_id text, agency_id text,
    agency_name text, agency_url text,
    PRIMARY KEY (static_onestop_id, attempt_id, agency_id)
);
CREATE INDEX idx_agencies_chateau_agency_id ON gtfs.agencies(chateau, agency_id);
CREATE TABLE gtfs.itinerary_pattern_meta (
    onestop_feed_id text, attempt_id text, itinerary_pattern_id text,
    trip_headsign text, shape_id text, direction_pattern_id text,
    PRIMARY KEY (onestop_feed_id, attempt_id, itinerary_pattern_id)
);
CREATE TABLE gtfs.direction_pattern_meta (
    onestop_feed_id text, attempt_id text, direction_pattern_id text, gtfs_shape_id text,
    PRIMARY KEY (onestop_feed_id, attempt_id, direction_pattern_id)
);
CREATE TABLE gtfs.shapes (
    onestop_feed_id text, attempt_id text, shape_id text, linestring text, color text, text_color text,
    PRIMARY KEY (onestop_feed_id, attempt_id, shape_id)
);
INSERT INTO gtfs.ingested_static VALUES
    ('feed', 'old', false, true, false, false, false, 1),
    ('feed', 'current', true, true, false, false, false, 2),
    ('feed', 'incomplete', false, false, true, false, false, 3),
    ('other-feed', 'current', true, true, false, false, false, 4),
    ('elsewhere', 'current', true, true, false, false, false, 5);
INSERT INTO gtfs.trips_compressed VALUES
    ('metro', 'feed', 'old', 'T', 'R', 'old trip', 'P', false),
    ('metro', 'feed', 'current', 'T', 'R', 'current trip', 'P', true),
    ('metro', 'feed', 'incomplete', 'T', 'R', 'unfinished trip', 'P', false),
    ('metro', 'other-feed', 'current', 'T', 'R', 'collision trip', 'P', false),
    ('different-chateau', 'elsewhere', 'current', 'T', 'R', 'wrong chateau', 'P', false);
INSERT INTO gtfs.routes VALUES
    ('metro', 'feed', 'old', 'R', 'old route', NULL, 'A', '000000', NULL, 2),
    ('metro', 'feed', 'current', 'R', 'current route', NULL, 'A', '123456', NULL, 2),
    ('metro', 'feed', 'incomplete', 'R', 'unfinished route', NULL, 'A', NULL, NULL, 2),
    ('metro', 'other-feed', 'current', 'R', 'collision route', NULL, 'A', 'ffffff', NULL, 3),
    ('different-chateau', 'elsewhere', 'current', 'R', 'wrong chateau', NULL, 'A', NULL, NULL, 3);
INSERT INTO gtfs.agencies VALUES
    ('metro', 'feed', 'old', 'A', 'old agency', NULL),
    ('metro', 'feed', 'current', 'A', 'correct agency', NULL),
    ('metro', 'other-feed', 'current', 'A', 'collision agency', NULL);
INSERT INTO gtfs.itinerary_pattern_meta VALUES
    ('feed', 'old', 'P', 'old headsign', 'S', 'D'),
    ('feed', 'current', 'P', 'correct headsign', 'S', 'D'),
    ('other-feed', 'current', 'P', 'collision headsign', 'S', 'D');
INSERT INTO gtfs.direction_pattern_meta VALUES ('feed', 'current', 'D', 'fallback');
INSERT INTO gtfs.shapes VALUES
    ('feed', 'old', 'S', 'old shape', 'old color', NULL),
    ('feed', 'current', 'S', 'correct shape', 'abcdef', NULL),
    ('feed', 'current', 'fallback', 'fallback shape', 'fedcba', NULL),
    ('other-feed', 'current', 'S', 'collision shape', NULL, NULL);
`);
// Positional rows match postgres::Row in Rust; retain duplicate column names.
async function query(feed = 'feed', trip = 'T', route = 'R', detailed = true) {
    return (await db.query(sql, ['metro', [feed], ['compiled-gone'], [trip], [route], detailed],
        { rowMode: 'array' })).rows;
}
let rows = await query();
assert.equal(rows.length, 1);
assert.deepEqual(rows[0].slice(0, 8), [
    'feed', 'compiled-gone', 'T', 'R', 'current route', null, 'current trip', 'correct headsign',
]);
assert.equal(rows[0][9], 'correct agency');
assert.equal(rows[0][15], 'correct shape');
assert.equal(rows[0][16], 'abcdef');
console.log('PASS: current completed import, collision isolation and exact shape/agency provenance');
rows = await query('renamed-feed');
assert.equal(rows.length, 1);
assert.equal(rows[0][6], 'collision trip'); // newest completed matching chateau/GTFS ID
assert.equal(rows[0][4], 'collision route');
assert.equal(rows[0][9], 'collision agency');
console.log('PASS: chateau/GTFS ID fallback after feed identity changes');
rows = await query('feed', 'removed-trip');
assert.equal(rows.length, 1);
assert.equal(rows[0][4], 'current route');
assert.equal(rows[0][6], null);
assert.equal(rows[0][9], 'correct agency');
console.log('PASS: missing trip retains route fallback');
rows = await query('feed', 'T', '');
assert.equal(rows[0][3], 'R');
assert.equal(rows[0][4], 'current route');
console.log('PASS: trip supplies missing public route ID');
await db.exec(`UPDATE gtfs.shapes SET linestring = NULL WHERE onestop_feed_id = 'feed' AND attempt_id = 'current' AND shape_id = 'S'`);
assert.equal((await query())[0][15], 'fallback shape');
console.log('PASS: direction shape fallback');
await db.exec(`UPDATE gtfs.shapes SET linestring = 'must-not-serialize' WHERE onestop_feed_id = 'feed' AND attempt_id = 'current'`);
rows = await query('feed', 'T', 'R', false);
assert.equal(rows[0][15], null);
assert.equal(rows[0][16], 'abcdef');
console.log('PASS: non-detailed results skip serialization and retain shape colors');
const many = await db.query(sql, ['metro', ['feed', 'feed'], ['a', 'b'], ['T', 'T'], ['R', 'R'], false], { rowMode: 'array' });
assert.equal(many.rows.length, 2);
assert.deepEqual(many.rows.map(r => r[1]).sort(), ['a', 'b']);
console.log('PASS: compiled response identities remain distinct');
// Force index preference only to check eligibility, not claim production plans.
await db.exec('SET enable_seqscan = off');
const plan = await db.query('EXPLAIN (FORMAT JSON) ' + sql,
    ['metro', ['feed'], ['a'], ['T'], ['R'], false]);
const text = JSON.stringify(plan.rows);
for (const index of ['trips_compressed_idx_chateau_id_trip_id', 'idx_chateau_route']) {
    assert.ok(text.includes(index), `index is not eligible: ${index}`);
}
assert.ok(text.includes('idx_agencies_chateau_agency_id') || text.includes('agencies_pkey'));
console.log('PASS: existing trip/route composite indexes and agency indexes support the query');
await db.close();
