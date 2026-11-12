# OSM station ranking regions

Within each import group, feature standardisation and rail/non-rail percentile
assignment run independently for Greater London, the rest of the UK, the remaining
target countries, and other stations. Existing scoring weights, tier thresholds,
and the subsequent spatial suppression pass are retained.

`greater-london.geo.json` is copied unchanged from
<https://github.com/utisz/compound-cities/blob/master/greater-london.geo.json>
(upstream Git blob `28f2cf9a7c58e6381a18f67dce6bcd8823e93905`). It is embedded in
the executable. The detailed polygon takes precedence over the coarser country
boundary; points on its boundary belong to Greater London. A bounding rectangle
only prefilters the polygon check, so nearby towns outside London remain in the
rest-of-UK group.

Country candidates are found by intersecting their bounding envelopes with the
station point, then checking polygon containment. Searching for country envelopes
fully contained in a small station envelope does not find the enclosing country.

Run the regression tests with:

```sh
cargo test --bin osm_station_rank_and_clean
```
