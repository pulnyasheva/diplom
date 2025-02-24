CREATE TABLE students
(
    id         SERIAL PRIMARY KEY,
    first_name VARCHAR(50)         NOT NULL,
    last_name  VARCHAR(50)         NOT NULL,
    age        INT CHECK (age > 0),
    email      VARCHAR(100) UNIQUE NOT NULL
);


INSERT INTO students (first_name, last_name, age, email)
VALUES ('John', 'Doe', 20, 'john.doe@example.com'),
       ('Jane', 'Smith', 22, 'jane.smith@example.com'),
       ('Alice', 'Johnson', 19, 'alice.johnson@example.com'),
       ('Bob', 'Brown', 21, 'bob.brown@example.com');
