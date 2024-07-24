-- Your SQL goes here
CREATE INDEX IF NOT EXISTS shapes_chateau_and_shape_id ON gtfs.shapes (chateau, shape_id);