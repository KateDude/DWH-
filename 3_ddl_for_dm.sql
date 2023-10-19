CREATE SCHEMA IF NOT EXISTS BL_DM;
SET search_path TO BL_DM;

CREATE TABLE IF NOT EXISTS dim_products (
product_surr_id       BIGINT, 
product_name          VARCHAR(100),
product_categoty_id   BIGINT,
product_category_name VARCHAR(50),
age_limit_id          BIGINT,
age_limit_value       VARCHAR(5),
material_id           BIGINT,
material_name         VARCHAR(50),
manufacturer_id       BIGINT,
manufacturer_name     VARCHAR(50),
source_system  VARCHAR(50),
source_entity  VARCHAR(50),
product_src_id VARCHAR(50),
insert_dt DATE,
update_dt DATE, 
PRIMARY KEY(product_surr_id)
);
CREATE SEQUENCE IF NOT EXISTS seq_dim_products
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE TABLE IF NOT EXISTS dim_customers (
customer_surr_id      BIGINT,
customer_gender       VARCHAR(5),
customer_card         VARCHAR(25),
customer_birthdate_dt DATE,
source_system         VARCHAR(50),
source_entity         VARCHAR(50),
customer_src_id       VARCHAR(50),
insert_dt DATE,
uptade_dt DATE,
PRIMARY KEY (customer_surr_id)
);
CREATE SEQUENCE IF NOT EXISTS seq_dim_customers
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE TABLE IF NOT EXISTS dim_suppliers (
supplier_surr_id BIGINT,
supplier_name    VARCHAR(100),
supplier_phone   VARCHAR(20),
supplier_email   VARCHAR(50),
source_system    VARCHAR(50),
source_entity    VARCHAR(50),
supplier_src_id  VARCHAR(100),
insert_dt DATE, 
update_dt DATE, 
PRIMARY KEY(supplier_surr_id)
);
CREATE SEQUENCE IF NOT EXISTS seq_dim_suppliers
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE TABLE IF NOT EXISTS dim_payments (
payment_surr_id BIGINT,
payment_name    VARCHAR(5) UNIQUE,
source_system   VARCHAR(50),
source_entity   VARCHAR(50),
payment_src_id  VARCHAR(50),
insert_dt DATE,
update_dt DATE, 
PRIMARY KEY (payment_surr_id)
);
CREATE SEQUENCE IF NOT EXISTS seq_dim_payments
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
    
CREATE TABLE IF NOT EXISTS dim_channels (
channel_surr_id BIGINT,
channel_name    VARCHAR(50),
channel_type    VARCHAR(200),
source_system   VARCHAR(50),
source_entity   VARCHAR(50),
channel_src_id  VARCHAR(50),
insert_dt DATE,
update_dt DATE,
PRIMARY KEY (channel_surr_id)
);
CREATE SEQUENCE IF NOT EXISTS seq_dim_channels
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE TABLE IF NOT EXISTS dim_stores (
store_surr_id BIGINT,
store_name    VARCHAR(60),
address_id    BIGINT,
address_line  VARCHAR(50),
city_id       BIGINT,
city_name     VARCHAR(50),
country_id    BIGINT,
country_name  VARCHAR(50),
source_system VARCHAR(50),
source_entity VARCHAR(50),
store_src_id  VARCHAR(50),
insert_dt DATE,
update_dt DATE,
PRIMARY KEY (store_surr_id)
);
CREATE SEQUENCE IF NOT EXISTS seq_dim_stores
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE TABLE IF NOT EXISTS dim_time_day (
date_value  DATE,
year_desc   INT,
quater_desc INT, 
month_desc  INT, 
month_name  VARCHAR(15),
week_desc   INT,
day_desc    INT, 
day_name    VARCHAR(15),
day_number_of_week INT,
insert_dt DATE,
update_dt DATE
);

CREATE TABLE IF NOT EXISTS dim_employees_scd (
employee_surr_id    BIGINT,
employee_first_name VARCHAR(50),
employee_surname    VARCHAR(50),
employee_gender     VARCHAR(5),
source_system       VARCHAR(50),
source_entity       VARCHAR(50),
employee_src_id     VARCHAR(50),
start_dt  DATE,
end_dt    DATE,
is_active VARCHAR(5),
insert_dt DATE,
PRIMARY KEY (employee_surr_id)
);
CREATE SEQUENCE IF NOT EXISTS seq_dim_employees_scd
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE TABLE IF NOT EXISTS fct_sales_dd (
event_dt         DATE,
product_surr_id  BIGINT,
customer_surr_id BIGINT,
supplier_surr_id BIGINT,
payment_surr_id  BIGINT,
channel_surr_id  BIGINT,
store_surr_id    BIGINT,
employee_surr_id BIGINT,
quantity         INT,
cost             FLOAT,
sale_price       FLOAT,
discount         FLOAT, 
calc_final_price FLOAT, 
calc_revenue     FLOAT,
insert_dt DATE,
update_dt DATE
) PARTITION BY RANGE(event_dt);

CREATE TABLE IF NOT EXISTS fct_sales_dd_2020 PARTITION OF bl_dm.fct_sales_dd
    FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');

CREATE TABLE IF NOT EXISTS fct_sales_dd_2021 PARTITION OF bl_dm.fct_sales_dd
    FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');

CREATE TABLE IF NOT EXISTS bl_dm_fct_sales_dd_2022 PARTITION OF bl_dm.fct_sales_dd
    FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');

CREATE TABLE IF NOT EXISTS bl_dm_fct_sales_dd_2023 PARTITION OF bl_dm.fct_sales_dd
    FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');

INSERT INTO dim_products (product_surr_id, product_name, product_categoty_id, product_category_name, age_limit_id, age_limit_value,
							material_id, material_name, manufacturer_id, manufacturer_name, source_system, source_entity, 
							product_src_id, insert_dt, update_dt)
SELECT -1, 'n.a.', -1, 'n.a.', -1, 'n.a.', -1, 'n.a.', -1, 'n.a.', 'MANUAL', 'MANUAL', 'n.a.', '1900-1-1', '1900-1-1'
WHERE NOT EXISTS (
    				SELECT 1 
    				FROM dim_products 
    				WHERE 
    				source_system = 'MANUAL' AND
    				source_entity = 'MANUAL' AND 
    				product_src_id = 'n.a.'
        			);

INSERT INTO dim_customers (customer_surr_id, customer_gender, customer_card, customer_birthdate_dt, source_system, source_entity, customer_src_id,
							insert_dt, uptade_dt)
SELECT -1, 'n.a.', 'n.a.', '1900-1-1', 'MANUAL', 'MANUAL', 'n.a.', '1900-1-1', '1900-1-1'
WHERE NOT EXISTS (
    				SELECT 1 
    				FROM dim_customers 
    				WHERE 
    				source_system = 'MANUAL' AND
    				source_entity = 'MANUAL' AND 
    				customer_src_id = 'n.a.'
        			);
INSERT INTO dim_suppliers (supplier_surr_id, supplier_name, supplier_phone, supplier_email, source_system, source_entity, supplier_src_id, 
							insert_dt, update_dt)
SELECT -1, 'n.a.', 'n.a.', 'n.a.', 'MANUAL', 'MANUAL', 'n.a.', '1900-1-1', '1900-1-1'
WHERE NOT EXISTS (
    				SELECT 1 
    				FROM dim_suppliers 
    				WHERE 
    				source_system = 'MANUAL' AND
    				source_entity = 'MANUAL' AND 
    				supplier_src_id = 'n.a.'    				
        			);
INSERT INTO dim_payments (payment_surr_id, payment_name, source_system, source_entity, payment_src_id, insert_dt, update_dt)
SELECT -1, 'n.a.', 'MANUAL', 'MANUAL', 'n.a.', '1900-1-1', '1900-1-1'
WHERE NOT EXISTS (
    				SELECT 1 
    				FROM dim_payments 
    				WHERE 
    				source_system = 'MANUAL' AND
    				source_entity = 'MANUAL' AND 
    				payment_src_id = 'n.a.'
        			);
INSERT INTO dim_channels (channel_surr_id, channel_name, channel_type, source_system, source_entity, channel_src_id, insert_dt, update_dt)
SELECT -1, 'n.a.', 'n.a.', 'MANUAL', 'MANUAL', 'n.a.', '1900-1-1', '1900-1-1'
WHERE NOT EXISTS (
    				SELECT 1 
    				FROM dim_channels 
    				WHERE 
    				source_system = 'MANUAL' AND
    				source_entity = 'MANUAL' AND 
    				channel_src_id = 'n.a.'
        			);
INSERT INTO dim_stores (store_surr_id, store_name, address_id, address_line, city_id, city_name, country_id, country_name, source_system,
						source_entity, store_src_id, insert_dt, update_dt)
SELECT -1, 'n.a.', -1, 'n.a.', -1, 'n.a.', -1, 'n.a.', 'MANUAL', 'MANUAL', 'n.a.', '1900-1-1', '1900-1-1'
WHERE NOT EXISTS (
    				SELECT 1 
    				FROM dim_stores 
    				WHERE 
    				source_system = 'MANUAL' AND
    				source_entity = 'MANUAL' AND 
    				store_src_id = 'n.a.'
        			);

INSERT INTO dim_employees_scd (employee_surr_id, employee_first_name, employee_surname, employee_gender, source_system, source_entity, 
								employee_src_id, start_dt, end_dt, is_active, insert_dt)
SELECT -1, 'n.a.', 'n.a.', 'n.a.', 'MANUAL', 'MANUAL', 'n.a.', '1900-1-1', '9999-12-31', 'True', '1900-1-1'
WHERE NOT EXISTS (
    				SELECT 1 
    				FROM dim_employees_scd 
    				WHERE 
    				source_system = 'MANUAL' AND
    				source_entity = 'MANUAL' AND 
    				employee_src_id = 'n.a.'
        			);
        			
COMMIT;