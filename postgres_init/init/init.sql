CREATE TABLE example1
(
    _id            INT PRIMARY KEY,
    varchar_field1 VARCHAR(50),
    varchar_field2 VARCHAR(100),
    int_field      INT,
    double_field   DOUBLE PRECISION,
    text_field     TEXT,
    bit_field      BIT(1),
    bool_field     BOOLEAN,
    null_value     TEXT
);

CREATE TABLE example2
(
    _id             INT PRIMARY KEY,
    int_array       INT[]
);

INSERT INTO example1 (varchar_field1, varchar_field2, int_field, double_field, text_field, bit_field, bool_field)
VALUES
    ('John', 'Doe', 20, 20.1, 'Sample text for John', B'1', TRUE),
    ('Jane', 'Smith', 22, 20.2, 'Sample text for Jane', B'0', FALSE),
    ('Alice', 'Johnson', 19, 20.3, 'Sample text for Alice', B'1', TRUE),
    ('Bob', 'Brown', 21, 20.4, 'Sample text for Bob', B'0', FALSE);

INSERT INTO example2 (int_array)
VALUES
    (ARRAY[1, 2, 3]),
    (ARRAY[4, 5]),
    (ARRAY[6, 7, 8, 9]),
    (ARRAY[10]);
