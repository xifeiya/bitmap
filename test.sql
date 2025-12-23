DROP TABLE IF EXISTS people;

DROP EXTENSION IF EXISTS yabit;
CREATE EXTENSION yabit;


-- Create a table people
CREATE TABLE people (
    id SERIAL PRIMARY KEY,
    name VARCHAR(20) NOT NULL,
    age INTEGER,
    birth_date DATE,
    gender VARCHAR(10),
    salary NUMERIC
);

INSERT INTO people (name, age, birth_date, gender, salary)
SELECT 
    'Person_' || gs AS name,
    (25 + (gs % 11)) AS age,  -- Randomly distributed ages between 25 and 35
    date '1990-01-01' + ((gs * 7) % 12000) * interval '1 day' AS birth_date,
    CASE WHEN gs % 2 = 0 THEN '男' ELSE '女' END AS gender,
    4000 + (gs % 5000) * 1.0 AS salary
FROM generate_series(1,1000) AS gs;


CREATE INDEX people_age_index ON people USING yabit (age);

CREATE INDEX people_salary_index ON people USING yabit (salary);

CREATE INDEX people_birth_date_index ON people USING yabit (birth_date);