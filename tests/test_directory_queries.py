"""Run against PostgreSQL with DIRECTORY_TEST_DATABASE_URL and psycopg[binary].

Uses a uniquely named schema, drops it on completion, and never touches gtfs.
The SQL files are the exact queries included by Birch.
"""
import os
from pathlib import Path
import unittest
import uuid

import psycopg
from psycopg.rows import dict_row

SQL_DIR = Path(__file__).resolve().parents[1] / 'src/birch/sql'


@unittest.skipUnless(os.environ.get('DIRECTORY_TEST_DATABASE_URL'),
                     'set DIRECTORY_TEST_DATABASE_URL to run PostgreSQL regressions')
class DirectoryQueries(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = psycopg.connect(os.environ['DIRECTORY_TEST_DATABASE_URL'],
                                   autocommit=True, row_factory=dict_row)
        cls.schema = 'directory_test_' + uuid.uuid4().hex
        cls.conn.execute(f'CREATE SCHEMA {cls.schema}')
        cls.conn.execute(f'SET search_path = {cls.schema}')
        cls.conn.execute('''CREATE TABLE agencies (
            chateau text NOT NULL, static_onestop_id text NOT NULL,
            attempt_id text NOT NULL, agency_id text NOT NULL,
            unified_agency_id text,
            PRIMARY KEY (static_onestop_id, attempt_id, agency_id))''')
        cls.conn.execute('''CREATE TABLE routes (
            chateau text NOT NULL, onestop_feed_id text NOT NULL,
            attempt_id text NOT NULL, route_id text NOT NULL,
            agency_id text, url_slug_for_unified_agency text,
            gtfs_order oid, short_name text, long_name text, gtfs_desc text,
            route_type smallint DEFAULT 3, url text, color text, text_color text,
            PRIMARY KEY (onestop_feed_id, attempt_id, route_id))''')
        cls.conn.execute('CREATE INDEX ON agencies (unified_agency_id)')
        cls.conn.execute('CREATE INDEX ON agencies (chateau, static_onestop_id)')
        for name, filename, params in [
            ('directory_list', 'directory_agency_routes.sql', 'text'),
            ('directory_resolve', 'directory_resolve_route_slug.sql', 'text, text'),
        ]:
            sql = (SQL_DIR / filename).read_text().replace('gtfs.', cls.schema + '.')
            cls.conn.execute(f'PREPARE {name} ({params}) AS {sql}')

    @classmethod
    def tearDownClass(cls):
        cls.conn.execute('DEALLOCATE ALL')
        cls.conn.execute(f'DROP SCHEMA {cls.schema} CASCADE')
        cls.conn.close()

    def setUp(self):
        self.conn.execute('TRUNCATE agencies, routes')
        self.conn.execute('RESET plan_cache_mode')

    def agency(self, agency='a', unified='target', attempt='v1', feed='f', chateau='c'):
        self.conn.execute('INSERT INTO agencies VALUES (%s,%s,%s,%s,%s)',
                          (chateau, feed, attempt, agency, unified))

    def route(self, route='r', agency='a', slug='slug', attempt='v1',
              feed='f', chateau='c', order=None):
        self.conn.execute('''INSERT INTO routes
            (chateau,onestop_feed_id,attempt_id,route_id,agency_id,
             url_slug_for_unified_agency,gtfs_order)
            VALUES (%s,%s,%s,%s,%s,%s,%s)''',
                          (chateau, feed, attempt, route, agency, slug, order))

    def listing(self):
        return self.conn.execute("EXECUTE directory_list('target')").fetchall()

    def resolve(self, slug):
        # EXECUTE parameters must be SQL literals, not protocol parameters.
        from psycopg.sql import SQL, Literal
        return self.conn.execute(SQL('EXECUTE directory_resolve({}, {})').format(
            Literal('target'), Literal(slug))).fetchall()

    def test_membership_is_scoped_to_feed_attempt_and_chateau(self):
        self.agency()
        self.agency(unified='other', attempt='v2')
        self.agency(agency='b', attempt='v2')
        self.route(route='old')
        self.route(route='wrong_attempt', attempt='v2', slug='wrong')
        self.route(route='new', agency='b', attempt='v2', slug='new')
        self.route(route='wrong_chateau', chateau='elsewhere', slug='elsewhere')
        self.assertEqual({r['route_id'] for r in self.listing()}, {'old', 'new'})
        self.assertEqual(self.resolve('wrong'), [])
        self.assertEqual(self.resolve('elsewhere'), [])

    def test_null_agency_only_matches_single_agency_attempt(self):
        self.agency()
        self.route(agency=None)
        self.assertEqual(len(self.listing()), 1)
        self.assertEqual(len(self.resolve('slug')), 1)
        self.agency(agency='b', unified='other')
        self.assertEqual(self.listing(), [])
        self.assertEqual(self.resolve('slug'), [])

    def test_multiple_target_memberships_do_not_duplicate_routes(self):
        self.agency()
        self.agency(agency='b')
        self.route()
        self.route(route='second', agency='b', slug='second')
        self.route(route='ambiguous', agency=None, slug='ambiguous')
        self.assertEqual({r['route_id'] for r in self.listing()}, {'r', 'second'})

    def test_cards_keep_slug_from_another_physical_copy(self):
        self.agency()
        self.agency(feed='other', chateau='other')
        self.route(slug=None)
        self.route(feed='other', chateau='other', slug='canonical')
        rows = self.listing()
        self.assertEqual(len(rows), 2)
        self.assertEqual({r['url_slug_for_unified_agency'] for r in rows}, {'canonical'})

    def test_slug_precedence_and_raw_id_fallback(self):
        self.agency()
        self.route(route='raw', slug=None, order=0)
        self.route(route='canonical', slug='raw', order=100)
        self.assertEqual(self.resolve('raw')[0]['route_id'], 'canonical')
        self.route(route='legacy', slug=None)
        self.assertEqual(self.resolve('legacy')[0]['route_id'], 'legacy')
        self.assertEqual(self.resolve('missing'), [])

    def test_slug_choice_matches_existing_order(self):
        self.agency()
        self.agency(attempt='v2')
        self.route(slug='later', order=2)
        self.route(attempt='v2', slug='earlier', order=1)
        rows = self.listing()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['url_slug_for_unified_agency'], 'earlier')

    def test_unknown_agency_returns_no_routes(self):
        self.agency(unified='other')
        self.route()
        self.assertEqual(self.listing(), [])
        self.assertEqual(self.resolve('slug'), [])

    def test_large_feed_counts_are_computed_once_for_custom_and_generic_plans(self):
        self.conn.execute('''INSERT INTO agencies
            SELECT 'c','f','v1','a'||i,CASE WHEN i=1 THEN 'target' ELSE 'other' END
            FROM generate_series(1,458) i''')
        self.conn.execute('''INSERT INTO routes
            (chateau,onestop_feed_id,attempt_id,route_id,agency_id,url_slug_for_unified_agency)
            SELECT 'c','f','v1','r'||i,'a'||(1+(i%458)),'slug'||i
            FROM generate_series(1,11178) i''')
        self.conn.execute('ANALYZE agencies')
        self.conn.execute('ANALYZE routes')
        for mode in ('force_custom_plan', 'force_generic_plan'):
            with self.subTest(mode=mode):
                self.conn.execute(f'SET plan_cache_mode = {mode}')
                self.assertEqual(len(self.listing()), 24)
                for statement in ("directory_list('target')",
                                  "directory_resolve('target', 'slug458')"):
                    result = self.conn.execute(
                        'EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON, TIMING OFF) EXECUTE '
                        + statement).fetchone()['QUERY PLAN'][0]
                    def walk(node):
                        yield node
                        for child in node.get('Plans', []):
                            yield from walk(child)
                    counts = [n for n in walk(result['Plan'])
                              if n.get('Subplan Name') == 'CTE feed_agency_counts']
                    self.assertEqual(len(counts), 1)
                    self.assertEqual(counts[0]['Actual Loops'], 1)
                    print(f'{mode} {statement}: {result["Execution Time"]:.3f} ms; '
                          'feed counts computed once')


if __name__ == '__main__':
    unittest.main(verbosity=2)
