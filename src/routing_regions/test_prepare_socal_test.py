"""Regression coverage for the Geofabrik .poly -> region GeoJSON converter."""

import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from prepare_socal_test import geojson_from_poly, validate_geojson


class GeofabrikPolyTest(unittest.TestCase):
    def test_exterior_and_hole_are_preserved(self):
        example = (
            "California fixture\n"
            "1\n"
            " -120.0 32.0\n"
            " -114.0 32.0\n"
            " -114.0 42.0\n"
            " -120.0 42.0\n"
            "END\n"
            "!2\n"
            " -118.0 35.0\n"
            " -117.0 35.0\n"
            " -117.0 36.0\n"
            " -118.0 36.0\n"
            "END\n"
            "END\n"
        )
        with TemporaryDirectory() as temp:
            input_file = Path(temp) / "california.poly"
            output_file = Path(temp) / "california.geojson"
            input_file.write_text(example, encoding="utf-8")
            geojson_from_poly(input_file, output_file)
            validate_geojson(output_file)
            geom = json.loads(output_file.read_text())
            self.assertEqual(geom["type"], "MultiPolygon")
            self.assertEqual(len(geom["coordinates"]), 1)
            rings = geom["coordinates"][0]
            self.assertEqual(len(rings), 2)
            self.assertEqual(rings[0][0], rings[0][-1])
            self.assertEqual(rings[1][0], rings[1][-1])

    def test_incomplete_poly_is_rejected(self):
        with TemporaryDirectory() as temp:
            input_file = Path(temp) / "california.poly"
            output_file = Path(temp) / "california.geojson"
            input_file.write_text("bad\n1\n -120 32\nEND\nEND\n")
            with self.assertRaises(ValueError):
                geojson_from_poly(input_file, output_file)


if __name__ == "__main__":
    unittest.main()
