-- Your SQL goes here
CREATE TABLE vehicles (
    file_path text NOT NULL,
    starting_range integer,
    ending_range integer,
    starting_text text,
    ending_text text,
    use_numerical_sort boolean,
    manufacturer text,
    model text,
    years text[],
    engine text,
    transmission text,
    notes text,
);