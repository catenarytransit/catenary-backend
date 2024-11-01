-- Your SQL goes here
CREATE TABLE gtfs.tile_storage (
    category smallint not null,
    z smallint not null,
    x smallint not null,
    y smallint not null,
    mvt_data bytea not null,
    primary key (category, z, x, y)
);