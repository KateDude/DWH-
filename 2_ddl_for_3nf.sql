CREATE SCHEMA IF NOT EXISTS BL_3NF;
SET search_path TO BL_3NF;

CREATE TABLE IF NOT EXISTS ce_product_category (
    product_category_id     BIGINT, 
    product_category_name   VARCHAR(50),
    source_system           VARCHAR(50),
    source_entity           VARCHAR(50),
    product_category_src_id VARCHAR(50),
    insert_dt DATE,
    update_dt DATE,
    PRIMARY KEY (product_category_id) 
);
CREATE SEQUENCE IF NOT EXISTS seq_product_category
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE TABLE IF NOT EXISTS ce_age_limit (
    age_limit_id     BIGINT,
    age_limit_value  VARCHAR(5),
    source_system    VARCHAR(50),
    source_entity    VARCHAR(50),
    age_limit_src_id VARCHAR(50),
    insert_dt DATE,
    update_dt DATE,
    PRIMARY KEY (age_limit_id)
);
CREATE SEQUENCE IF NOT EXISTS seq_age_limit
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE TABLE IF NOT EXISTS ce_material (
    material_id     BIGINT,
    material_name   VARCHAR(50),
    source_system   VARCHAR(50),
    source_entity   VARCHAR(50),
    material_src_id VARCHAR(50),
    insert_dt DATE,
    update_dt DATE,
    PRIMARY KEY (material_id)
);
CREATE SEQUENCE IF NOT EXISTS seq_material
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
   
CREATE TABLE IF NOT EXISTS ce_manufacturer (
    manufacturer_id     BIGINT,
    manufacturer_name   VARCHAR(50),
    source_system       VARCHAR(50),
    source_entity       VARCHAR(50),
    manufacturer_src_id VARCHAR(50),
    insert_dt DATE,
    update_dt DATE,
    PRIMARY KEY (manufacturer_id)
);
CREATE SEQUENCE IF NOT EXISTS seq_manufacturer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
   
CREATE TABLE IF NOT EXISTS ce_products (
    product_id   BIGINT,
    product_name VARCHAR(100),
    product_category_id BIGINT,
    age_limit_id BIGINT,
    material_id  BIGINT,
    manufacturer_id BIGINT,
    source_system   VARCHAR(50),
    source_entity   VARCHAR(50),
    product_src_id  VARCHAR(100),
    insert_dt DATE,
    update_dt DATE,
    PRIMARY KEY (product_id),
    FOREIGN KEY (product_category_id) REFERENCES ce_product_category (product_category_id),
    FOREIGN KEY (age_limit_id)        REFERENCES ce_age_limit (age_limit_id),
    FOREIGN KEY (material_id)         REFERENCES ce_material (material_id),
    FOREIGN KEY (manufacturer_id)     REFERENCES ce_manufacturer (manufacturer_id)
);
CREATE SEQUENCE IF NOT EXISTS seq_products
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
   
CREATE TABLE IF NOT EXISTS ce_customers (
    customer_id     BIGINT,
    customer_gender VARCHAR(5),
    customer_card   VARCHAR(25),
    customer_birthdate_dt DATE,
    source_system   VARCHAR(50),
    source_entity   VARCHAR(50),
    customer_src_id VARCHAR(50),
    insert_dt DATE,
    update_dt DATE,
    PRIMARY KEY (customer_id)
);
CREATE SEQUENCE IF NOT EXISTS seq_customers
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
   
CREATE TABLE IF NOT EXISTS ce_suppliers (
    supplier_id     BIGINT,
    supplier_name   VARCHAR(100),
    supplier_phone  VARCHAR(20),
    supplier_email  VARCHAR(50),
    source_system   VARCHAR(50),
    source_entity   VARCHAR(50),
    supplier_src_id VARCHAR(100),
    insert_dt DATE,
    update_dt DATE,
    PRIMARY KEY (supplier_id)
);
CREATE SEQUENCE IF NOT EXISTS seq_suppliers
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
   
CREATE TABLE IF NOT EXISTS ce_payments (
    payment_id     BIGINT,
    payment_name   VARCHAR(5),
    source_system  VARCHAR(50),
    source_entity  VARCHAR(50),
    payment_src_id VARCHAR(5),
    insert_dt DATE,
    update_dt DATE,
    PRIMARY KEY (payment_id)
);
CREATE SEQUENCE IF NOT EXISTS seq_payments
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
   
CREATE TABLE IF NOT EXISTS ce_channels (
    channel_id     BIGINT,
    channel_name   VARCHAR(50),
    channel_type   VARCHAR(200),
    source_system  VARCHAR(50),
    source_entity  VARCHAR(50),
    channel_src_id VARCHAR(50),
    insert_dt DATE,
    update_dt DATE,
    PRIMARY KEY (channel_id)
);
CREATE SEQUENCE IF NOT EXISTS seq_channels
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
   
CREATE TABLE IF NOT EXISTS ce_country (
    country_id     BIGINT,
    country_name   VARCHAR(50),
    source_system  VARCHAR(50),
    source_entity  VARCHAR(50),
    country_src_id VARCHAR(50),
    insert_dt DATE,
    update_dt DATE,
    PRIMARY KEY (country_id)
);
CREATE SEQUENCE IF NOT EXISTS seq_country
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
   
CREATE TABLE IF NOT EXISTS ce_city (
    city_id       BIGINT,
    city_name     VARCHAR(50),
    country_id    BIGINT,
    source_system VARCHAR(50),
    source_entity VARCHAR(50),
    city_src_id   VARCHAR(50),
    insert_dt DATE,
    update_dt DATE,
    PRIMARY KEY (city_id),
    FOREIGN KEY (country_id) REFERENCES ce_country (country_id)
);
CREATE SEQUENCE IF NOT EXISTS seq_city
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
   
CREATE TABLE IF NOT EXISTS ce_address (
    address_id     BIGINT,
    address_line   VARCHAR(50),
    city_id        BIGINT,
    source_system  VARCHAR(50),
    source_entity  VARCHAR(50),
    address_src_id VARCHAR(50),
    insert_dt DATE,
    update_dt DATE,
    PRIMARY KEY (address_id),
    FOREIGN KEY (city_id) REFERENCES ce_city (city_id)
);
CREATE SEQUENCE IF NOT EXISTS seq_address
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
   
CREATE TABLE IF NOT EXISTS ce_stores (
    store_id   BIGINT,
    store_name VARCHAR(60),
    address_id BIGINT,
    source_system VARCHAR(50),
    source_entity VARCHAR(50),
    store_src_id  VARCHAR(60),
    insert_dt DATE,
    update_dt DATE,
    PRIMARY KEY (store_id),
    FOREIGN KEY (address_id) REFERENCES ce_address (address_id)
);
CREATE SEQUENCE IF NOT EXISTS seq_stores
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
   
CREATE TABLE IF NOT EXISTS ce_employees_scd (
    employee_id BIGINT,
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
    PRIMARY KEY (employee_id, start_dt)
);
CREATE SEQUENCE IF NOT EXISTS seq_employees_scd
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
   
CREATE TABLE IF NOT EXISTS ce_sales (
    product_id  BIGINT,
    store_id    BIGINT,
    channel_id  BIGINT,
    payment_id  BIGINT,
    supplier_id BIGINT,
    customer_id BIGINT,
    employee_id BIGINT,
    event_dt DATE,
    quantity INT,
    cost       FLOAT,
    sale_price FLOAT,
    discount   FLOAT,
    insert_dt  DATE,
    update_dt  DATE,
    FOREIGN KEY (product_id)  REFERENCES ce_products  (product_id),
    FOREIGN KEY (store_id)    REFERENCES ce_stores    (store_id),
    FOREIGN KEY (channel_id)  REFERENCES ce_channels  (channel_id),
    FOREIGN KEY (payment_id)  REFERENCES ce_payments  (payment_id),
    FOREIGN KEY (supplier_id) REFERENCES ce_suppliers (supplier_id),
    FOREIGN KEY (customer_id) REFERENCES ce_customers (customer_id)
);

INSERT INTO ce_product_category (product_category_id, product_category_name, source_system, source_entity, product_category_src_id, insert_dt, update_dt)
SELECT -1, 'n.a.', 'MANUAL', 'MANUAL', 'n.a.', '1900-1-1', '1900-1-1'
WHERE NOT EXISTS (
    				SELECT 1 
    				FROM ce_product_category 
    				WHERE  
    				source_system = 'MANUAL' AND
    				source_entity = 'MANUAL' AND 
    				product_category_src_id = 'n.a.'
        			);

INSERT INTO ce_age_limit(age_limit_id, age_limit_value, source_system, source_entity, age_limit_src_id, insert_dt, update_dt)
SELECT -1, 'n.a.', 'MANUAL', 'MANUAL', 'n.a.', '1900-1-1', '1900-1-1'
WHERE NOT EXISTS (
    				SELECT 1 
    				FROM ce_age_limit 
    				WHERE 
    				source_system = 'MANUAL' AND
    				source_entity = 'MANUAL' AND 
    				age_limit_src_id = 'n.a.'
        			);

INSERT INTO ce_material (material_id, material_name, source_system, source_entity, material_src_id, insert_dt, update_dt)
SELECT -1, 'n.a.', 'MANUAL', 'MANUAL', 'n.a.', '1900-1-1', '1900-1-1'
WHERE NOT EXISTS (
    				SELECT 1 
    				FROM ce_material 
    				WHERE 
    				source_system = 'MANUAL' AND
    				source_entity = 'MANUAL' AND 
    				material_src_id = 'n.a.'
        			);

INSERT INTO ce_manufacturer (manufacturer_id, manufacturer_name, source_system, source_entity, manufacturer_src_id, insert_dt, update_dt)
SELECT -1, 'n.a.', 'MANUAL', 'MANUAL', 'n.a.', '1900-1-1', '1900-1-1'
WHERE NOT EXISTS (
    				SELECT 1 
    				FROM ce_manufacturer 
    				WHERE 
    				source_system = 'MANUAL' AND
    				source_entity = 'MANUAL' AND 
    				manufacturer_src_id = 'n.a.'
        			);

INSERT INTO ce_products (product_id, product_name, product_category_id, age_limit_id, material_id, manufacturer_id, source_system, source_entity,
						product_src_id, insert_dt, update_dt)
SELECT -1, 'n.a.', -1, -1, -1, -1, 'MANUAL', 'MANUAL', 'n.a.', '1900-1-1', '1900-1-1'
WHERE NOT EXISTS (
    				SELECT 1 
    				FROM ce_products 
    				WHERE 
    				source_system = 'MANUAL' AND
    				source_entity = 'MANUAL' AND 
    				product_src_id = 'n.a.'
        			);

INSERT INTO ce_customers(customer_id, customer_gender, customer_card, customer_birthdate_dt, source_system, source_entity, customer_src_id, insert_dt, update_dt)
SELECT -1, 'n.a.', 'n.a.', '1-1-1900', 'MANUAL', 'MANUAL', 'n.a.', '1900-1-1', '1900-1-1'
WHERE NOT EXISTS (
    				SELECT 1 
    				FROM ce_customers 
    				WHERE 
    				source_system = 'MANUAL' AND
    				source_entity = 'MANUAL' AND 
    				customer_src_id = 'n.a.'
        			);
INSERT INTO ce_suppliers (supplier_id, supplier_name, supplier_phone, supplier_email, source_system, source_entity, supplier_src_id, insert_dt, update_dt)
SELECT -1, 'n.a.', 'n.a.', 'n.a.', 'MANUAL', 'MANUAL', 'n.a.', '1900-1-1', '1900-1-1'
WHERE NOT EXISTS (
    				SELECT 1 
    				FROM ce_suppliers 
    				WHERE 
    				source_system = 'MANUAL' AND
    				source_entity = 'MANUAL' AND 
    				supplier_src_id = 'n.a.'
        			);
INSERT INTO ce_payments(payment_id, payment_name, source_system, source_entity, payment_src_id, insert_dt, update_dt)
SELECT -1, 'n.a.', 'MANUAL', 'MANUAL', 'n.a.', '1900-1-1', '1900-1-1'
WHERE NOT EXISTS (
    				SELECT 1 
    				FROM ce_payments 
    				WHERE 
    				source_system = 'MANUAL' AND
    				source_entity = 'MANUAL' AND 
    				payment_src_id = 'n.a.'
        			);
INSERT INTO ce_channels (channel_id, channel_name, channel_type, source_system, source_entity, channel_src_id, insert_dt, update_dt)
SELECT -1, 'n.a.', 'n.a.', 'MANUAL', 'MANUAL', 'n.a.', '1900-1-1', '1900-1-1'
WHERE NOT EXISTS (
    				SELECT 1 
    				FROM ce_channels 
    				WHERE 
    				source_system = 'MANUAL' AND
    				source_entity = 'MANUAL' AND 
    				channel_src_id = 'n.a.'
        			);
INSERT INTO ce_country(country_id, country_name, source_system, source_entity, country_src_id, insert_dt, update_dt)
SELECT -1, 'n.a.', 'MANUAL', 'MANUAL', 'n.a.', '1900-1-1', '1900-1-1'
WHERE NOT EXISTS (
    				SELECT 1 
    				FROM ce_country 
    				WHERE 
    				source_system = 'MANUAL' AND
    				source_entity = 'MANUAL' AND 
    				country_src_id = 'n.a.'
        			);
INSERT INTO ce_city(city_id, city_name, country_id, source_system, source_entity, city_src_id, insert_dt, update_dt)
SELECT -1, 'n.a.', -1, 'MANUAL', 'MANUAL', 'n.a.', '1900-1-1', '1900-1-1'
WHERE NOT EXISTS (
    				SELECT 1 
    				FROM ce_city 
    				WHERE  
    				source_system = 'MANUAL' AND
    				source_entity = 'MANUAL' AND 
    				city_src_id = 'n.a.'
        			);
INSERT INTO ce_address (address_id, address_line, city_id, source_system, source_entity, address_src_id, insert_dt, update_dt)
SELECT -1, 'n.a.', -1, 'MANUAL', 'MANUAL', 'n.a.', '1900-1-1', '1900-1-1'
WHERE NOT EXISTS (
    				SELECT 1 
    				FROM ce_address 
    				WHERE  
    				source_system = 'MANUAL' AND
    				source_entity = 'MANUAL' AND 
    				address_src_id = 'n.a.'
        			);
INSERT INTO ce_stores (store_id, store_name, address_id, source_system, source_entity, store_src_id, insert_dt, update_dt)
SELECT -1, 'n.a.', -1, 'MANUAL', 'MANUAL', 'n.a.', '1900-1-1', '1900-1-1'
WHERE NOT EXISTS (
    				SELECT 1 
    				FROM ce_stores 
    				WHERE 
    				source_system = 'MANUAL' AND
    				source_entity = 'MANUAL' AND 
    				store_src_id = 'n.a.'    				
        			);
INSERT INTO ce_employees_scd(employee_id, employee_first_name, employee_surname, employee_gender, source_system, source_entity, employee_src_id, 
						start_dt, end_dt, is_active, insert_dt)
SELECT -1, 'n.a.', 'n.a.', 'n.a.', 'MANUAL', 'MANUAL', 'n.a.', '1900-1-1', '9999-12-31', 'True', '1900-1-1'
WHERE NOT EXISTS (
    				SELECT 1 
    				FROM ce_employees_scd 
    				WHERE 
    				source_system = 'MANUAL' AND
    				source_entity = 'MANUAL' AND 
    				employee_src_id = 'n.a.'
        			);
COMMIT;
