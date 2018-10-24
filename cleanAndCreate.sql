DROP SCHEMA fct CASCADE;

DROP SCHEMA dim CASCADE;

CREATE SCHEMA fct;

CREATE SCHEMA dim;

CREATE TABLE dim.product (
    product_id serial PRIMARY KEY,

    -- Properties
    name varchar(50) NOT NULL,
    price int NOT NULL,
    alcohol_content_ml decimal NOT NULL,

    -- When it is available for purchase
    activate_date date NOT NULL,
    deactivate_date date,

    -- Slowly changing dimension fields
    version int NOT NULL,
    valid_from date NOT NULL,
    valid_to date
);

CREATE TABLE dim.time(
    time_id serial PRIMARY KEY,

    -- Properties
    t_year int NOT NULL,
    t_month int NOT NULL,
    t_day int NOT NULL,
    t_hour int NOT NULL,
    day_of_week int NOT NULL,
    is_fall_semester BOOLEAN NOT NULL,
    is_holiday BOOLEAN NOT NULL
);

CREATE TABLE dim.store (
    store_id serial PRIMARY KEY,

    -- Properties
    name varchar(50) NOT NULL,
    description varchar(50) NOT NULL,

    -- Slowly changing dimension fields
    version int NOT NULL,
    valid_from date NOT NULL,
    valid_to date
);

CREATE TABLE dim.member (
    member_id serial PRIMARY KEY,

    -- Properties
    gender varchar(1) NOT NULL,
    is_active boolean NOT NULL,
    course varchar(50),   

    -- Slowly changing dimension fields
    version int NOT NULL,
    valid_from date NOT NULL,
    valid_to date
);

CREATE TABLE fct.sale (
    sale_id serial,

    -- Dimension foreign keys
    fk_product_id serial NOT NULL,
    fk_time_id serial NOT NULL,
    fk_store_id serial NOT NULL,
    fk_member_id serial NOT NULL,

    -- Measures
    price int NOT NULL
);
