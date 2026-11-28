import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

import swiss_raptor_compression as bench


class SnapshotTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.atlas = self.root / "atlas"
        self.path = self.atlas / bench.ATLAS_FEED
        self.path.parent.mkdir(parents=True)
        self.url = "https://example.org/static.zip"
        self.feed = {"id": bench.FEED, "spec": "gtfs", "urls": {"static_current": self.url}}
        self.path.write_text(json.dumps({"feeds": [
            {"id": bench.FEED + "~rt", "spec": "gtfs-rt", "urls": {"realtime": "https://example.org/rt"}},
            self.feed
        ]}))
        self.target = self.root / "snapshot.zip"

    def valid_zip(self, path):
        with zipfile.ZipFile(path, "w") as z:
            for name in ("agency", "stops", "routes", "trips", "stop_times", "calendar_dates"):
                z.writestr(name + ".txt", "header\nrow\n")

    def test_exact_static_feed(self):
        _, _, urls = bench.swiss_atlas_entry(self.atlas)
        self.assertEqual(urls["static_current"], self.url)

    def test_duplicate_fails(self):
        self.path.write_text(json.dumps({"feeds": [self.feed, self.feed]}))
        with self.assertRaises(ValueError):
            bench.swiss_atlas_entry(self.atlas)

    def test_shared_snapshot(self):
        with patch.object(bench, "download", side_effect=lambda url, target: self.valid_zip(target)) as download:
            info = bench.download_snapshot(self.atlas, self.target)
        self.assertEqual(info["url"], self.url)
        self.assertEqual(info["gtfs_sha256"], bench.digest(self.target))
        download.assert_called_once_with(self.url, self.target.with_name("snapshot.zip.part"))
        bench.validate_gtfs(self.target)

    def test_invalid_zip_never_published(self):
        with patch.object(bench, "download", side_effect=lambda url, target: target.write_text("<html>Forbidden</html>")):
            with self.assertRaises(zipfile.BadZipFile):
                bench.download_snapshot(self.atlas, self.target)
        self.assertFalse(self.target.exists())
        self.assertFalse(self.target.with_name("snapshot.zip.part").exists())


if __name__ == "__main__":
    unittest.main()
