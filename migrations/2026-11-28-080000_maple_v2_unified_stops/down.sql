-- Roll back application binaries / disable unified reads before reverting.
DROP TRIGGER maple_v2_major_unified_delete ON gtfs.maple_v2_stop_major;
DROP TRIGGER maple_v2_stop_unified_delete ON gtfs.maple_v2_stop;
DROP TRIGGER maple_v2_major_unified_update ON gtfs.maple_v2_stop_major;
DROP TRIGGER maple_v2_major_unified_insert ON gtfs.maple_v2_stop_major;
DROP TRIGGER maple_v2_stop_unified_update ON gtfs.maple_v2_stop;
DROP TRIGGER maple_v2_stop_unified_insert ON gtfs.maple_v2_stop;
DROP FUNCTION gtfs.maple_v2_backfill_unified(text, text);
DROP FUNCTION gtfs.maple_v2_unified_invalidate();
DROP TABLE gtfs.maple_v2_stop_unified_attempt;
DROP TABLE gtfs.maple_v2_stop_unified;
