#!/usr/bin/env python3
"""Generate ignored configs for a Swiss-only Maple + native OSR experiment."""
import json
import os
from pathlib import Path
import sys

# Reuse exactly the production integration fixture's Maple/OSR settings.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src/routing_regions"))
from prepare_socal_test import prepare

prepare(Path("routing_regions/switzerland.geojson"), Path("routing_regions.toml"),
        os.environ["DATABASE_URL"], "./transitland-atlas")
p = Path("catenaryconfig.toml")
s = p.read_text()
start = s.index("only_feed_ids = [")
end = s.index("]", start) + 1
s = s[:start] + 'only_feed_ids = ["f-u0-switzerland"]' + s[end:]
s = s.replace("north-america/us/california/socal", "europe/switzerland")
p.write_text(s)
p = Path("routing_regions.toml")
p.write_text(p.read_text().replace("southern-california", "switzerland")
             .replace("Southern California integration test", "Switzerland Hilbert experiment"))
for variant in ("main", "source", "hilbert"):
    dest = Path("benchmark-results") / variant
    dest.mkdir(parents=True, exist_ok=True)
    # Absolute data_dir keeps all three services isolated regardless of cwd.
    config = p.read_text().replace('data_dir = "compiled/routing"',
                                  f'data_dir = {json.dumps(str(dest.resolve()))}')
    (dest / "regions.toml").write_text(config.replace(json.dumps('routing_regions/switzerland.geojson'),
        json.dumps(str(Path("routing_regions/switzerland.geojson").resolve()))))
