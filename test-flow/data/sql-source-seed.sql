-- Seed data for the manual-flow SQL source (Postgres)
CREATE TABLE patients
(
    pid                varchar(4) NOT NULL,
    gender             varchar(8),
    "birthDate"        date,
    "deceasedDateTime" timestamp,
    "homePostalCode"   varchar(8)
);

INSERT INTO patients (pid, gender, "birthDate", "deceasedDateTime", "homePostalCode") VALUES
    ('p1',  'male',   DATE '2000-05-10', NULL,                          NULL),
    ('p2',  'male',   DATE '1985-05-08', TIMESTAMP '2017-03-10 00:00:00','G02547'),
    ('p3',  'male',   DATE '1997-02-01', NULL,                          NULL),
    ('p4',  'male',   DATE '1999-06-05', NULL,                          'H10564'),
    ('p5',  'male',   DATE '1965-10-01', TIMESTAMP '2019-04-21 00:00:00','G02547'),
    ('p6',  'female', DATE '1991-03-01', NULL,                          NULL),
    ('p7',  'female', DATE '1972-10-25', NULL,                          'V13135'),
    ('p8',  'female', DATE '2010-01-10', NULL,                          'Z54564'),
    ('p9',  'female', DATE '1999-05-12', NULL,                          NULL),
    ('p10', 'female', DATE '2003-11-01', NULL,                          NULL);
