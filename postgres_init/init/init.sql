CREATE TYPE student_status AS ENUM ('active', 'graduate', 'expelled');

CREATE TYPE passport_type AS (
    passport_number VARCHAR(20),
    issue_date DATE,
    expiration_date DATE
    );

CREATE TABLE students
(
    first_name  VARCHAR(50)         NOT NULL,
    last_name   VARCHAR(50)         NOT NULL,
    id          SERIAL PRIMARY KEY,
    age         INT CHECK (age > 0),
    email       VARCHAR(100)
);

INSERT INTO students (first_name, last_name, age, email)
VALUES
    ('John', 'Doe', 20, 'john.doe@example.com'),
    ('Jane', 'Smith', 22, 'jane.smith@example.com'),
    ('Alice', 'Johnson', 19, 'alice.johnson@example.com'),
    ('Bob', 'Brown', 21, 'bob.brown@example.com');
