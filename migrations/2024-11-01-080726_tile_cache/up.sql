-- Your SQL goes here
CREATE TABLE gtfs.tile_storage (
    category smallint not null,
    z smallint not null,
    x int not null,
    y int not null,
    mvt_data bytea not null,
    added_time Timestamptz not null,
    
    primary key (category, z, x, y)
);