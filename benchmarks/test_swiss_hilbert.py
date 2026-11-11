import copy
from datetime import date, datetime
import json
from pathlib import Path
import tempfile
import unittest
from zoneinfo import ZoneInfo

from swiss_hilbert import canonical, confidence, fixtures, report, validate, VARIANTS, METRICS


def response():
    p = {"stopId": "gtfs-stop", "stopIndex": 12, "lat": 47., "lon": 8.}
    leg = dict(mode="RAIL", startTime="08:00", endTime="09:00", duration=3600,
               chateau="schweiz", tripId="trip-a", routeId="route-a", **{"from": p, "to": p})
    return dict(itineraries=[dict(startTime="08:00", endTime="09:00", duration=3600,
                                 transfers=0, legs=[leg], id="layout-hash")], direct=[],
                debugOutput={"raptor": {"profile_budget_exhausted": False},
                             "timings": {k: 1. for k in METRICS[:-1]}})


class BenchmarkTests(unittest.TestCase):
    def test_fixture_dates_directions_and_zurich_timezone(self):
        catalog = json.loads(Path(__file__).with_name("swiss_routing_queries.json").read_text())
        cases = fixtures(catalog, date(2026, 11, 16))
        self.assertEqual(len(cases), 48)
        self.assertEqual(len({c['id'] for c in cases}), 48)
        first = cases[0]['request']
        self.assertEqual(datetime.fromtimestamp(first['departure_unix_seconds'], ZoneInfo('Europe/Zurich')).hour, 8)
        self.assertEqual(cases[2]['request']['from'], first['to'])
        self.assertEqual(cases[2]['request']['to'], first['from'])
        with self.assertRaises(ValueError):
            fixtures(catalog, date(2026, 11, 17))

    def test_parity_ignores_layout_ids_and_timing(self):
        a = response()
        b = copy.deepcopy(a)
        b['itineraries'][0]['id'] = 'other-hash'
        b['itineraries'][0]['legs'][0]['from']['stopIndex'] = 900
        b['debugOutput']['timings']['raptor_ms'] = 42.
        self.assertEqual(canonical(a), canonical(b))

    def test_parity_detects_trip_stop_and_time_changes(self):
        a = response()
        for field, value in [('tripId', 'other-trip'), ('endTime', '10:00')]:
            b = copy.deepcopy(a)
            b['itineraries'][0]['legs'][0][field] = value
            self.assertNotEqual(canonical(a), canonical(b))
        b = copy.deepcopy(a)
        b['itineraries'][0]['legs'][0]['to']['stopId'] = 'other-platform'
        self.assertNotEqual(canonical(a), canonical(b))

    def test_invalid_partial_or_empty_results_fail(self):
        a = response()
        validate(a)
        a['debugOutput']['pages'] = {'after': {'profile_budget_exhausted': True}}
        with self.assertRaises(ValueError):
            validate(a)
        a = response()
        a['itineraries'] = []
        with self.assertRaises(ValueError):
            validate(a)
        a = response()
        a['debugOutput']['timings']['raptor_ms'] = float('nan')
        with self.assertRaises(ValueError):
            validate(a)

    def test_speedup_direction_and_bootstrap(self):
        self.assertEqual(confidence([2., 2., 2.]), [2., 2.])
        rows = [dict(round=r, variant=v, query=q, phase='measure', parity=True,
                     **{k: ms for k in METRICS})
                for r in range(3) for v, ms in zip(VARIANTS, [4., 4., 2.])
                for q in ('a', 'b')]
        cases = [dict(id='a', label='A', kind='local'), dict(id='b', label='B', kind='intercity')]
        with tempfile.TemporaryDirectory() as d:
            out = Path(d)
            report(rows, cases, out)
            result = json.loads((out / 'summary.json').read_text())
            hilbert = next(a for a in result['aggregates'] if a['group'] == 'all'
                           and a['baseline'] == 'source' and a['candidate'] == 'hilbert'
                           and a['metric'] == 'raptor_ms')
            self.assertEqual(hilbert['speedup'], 2.)
            self.assertEqual(hilbert['ci95'], [2., 2.])
            rows[0]['parity'] = False
            with self.assertRaises(RuntimeError):
                report(rows, cases, out)


if __name__ == '__main__':
    unittest.main()
