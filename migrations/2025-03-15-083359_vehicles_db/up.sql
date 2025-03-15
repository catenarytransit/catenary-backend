-- Your SQL goes here
CREATE TABLE gtfs.vehicles (
    file_path text NOT NULL,
    starting_range integer,
    ending_range integer,
    starting_text text,
    ending_text text,
    use_numeric_sorting boolean,
    manufacturer text,
    model text,
    years text[],
    engine text,
    transmission text,
    notes text,
    key_str text NOT NULL,
    PRIMARY KEY (file_path,key_str)
);