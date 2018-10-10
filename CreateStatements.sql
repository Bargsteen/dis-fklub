DROP SCHEMA fct CASCADE;

DROP SCHEMA dim CASCADE;

CREATE SCHEMA fct;

CREATE SCHEMA dim;

-- MISSING: Unique attributes on lookup attributes

CREATE TABLE fct.sale (
    sale_id SERIAL,

    -- Dimension foreign keys
    fk_product_id SERIAL NOT NULL,
    fk_time_id SERIAL NOT NULL,
    fk_store_id SERIAL NOT NULL,
    fk_member_id SERIAL NOT NULL,

    -- Measures
    order_id SERIAL NOT NULL,
    item_count INT NOT NULL
);

CREATE TABLE dim.product (
    product_id SERIAL PRIMARY KEY,

    -- Properties
    name VARCHAR(50) NOT NULL,
    price DECIMAL (5,
        2) NOT NULL,
    alcohol_content_ml DECIMAL (2,
        2) NOT NULL,

    -- When it is available for purchase
    activate_date DATE NOT NULL,
    deactivate_date DATE,

    -- Slowly changing dimension fields:
    version INT NOT NULL
    valid_from DATE NOT NULL,
    valid_to DATE,
);

CREATE TABLE dim.time(
    time_id SERIAL PRIMARY KEY,

    -- Properties
    t_date DATE NOT NULL,
    t_year INT NOT NULL,
    t_month INT NOT NULL,
    t_day INT NOT NULL,
    t_hour INT NOT NULL,
    day_of_week INT NOT NULL,
    fall_semester BOOLEAN NOT NULL,
    is_holiday BOOLEAN NOT NULL
);

CREATE TABLE dim.store (
    store_id SERIAL PRIMARY KEY,

    -- Properties
    city VARCHAR(50) NOT NULL,
    building VARCHAR(50) NOT NULL,
    room VARCHAR(50) NOT NULL,

    -- Slowly changing dimension fields:
    version INT NOT NULL
    valid_from DATE NOT NULL,
    valid_to DATE
);

CREATE TABLE dim.member (
    member_id SERIAL PRIMARY KEY,

    -- Properties
    gender VARCHAR(1) NOT NULL,
    course VARCHAR(50)
);

-- ConstraINTs
-- ALTER TABLE fct.sale 
--    ADD CONSTRAINT fk_product
--    FOREIGN KEY (fk_product_id) 
--    REFERENCES dim.product (product_id);
-- ALTER TABLE fct.sale 
--    ADD CONSTRAINT fk_time
--    FOREIGN KEY (fk_time_id) 
--    REFERENCES dim.time (time_id);
-- ALTER TABLE fct.sale 
--    ADD CONSTRAINT fk_store
--    FOREIGN KEY (fk_store_id) 
--    REFERENCES dim.store (store_id);
-- ALTER TABLE fct.sale 
--    ADD CONSTRAINT fk_weather
--    FOREIGN KEY (fk_weather_id) 
--    REFERENCES dim.weather (weather_id);
-- ALTER TABLE fct.sale 
--    ADD CONSTRAINT fk_member
--    FOREIGN KEY (fk_member_id) 
--    REFERENCES dim.member (member_id);
