-- created a schema for the first source
CREATE SCHEMA IF NOT EXISTS sa_set_cash;
SET search_path TO sa_set_cash;
-- installed file_fdw as an extension
CREATE EXTENSION IF NOT EXISTS file_fdw;
-- created a foreign server
CREATE SERVER IF NOT EXISTS pglog FOREIGN DATA WRAPPER file_fdw;
-- created foreigh table, the columns for the table correspond to the csv file and its format is, all columns have datatype: varchar
CREATE FOREIGN TABLE IF NOT EXISTS ext_sales_cash(
product_name          VARCHAR(4000),
product_category_name VARCHAR(4000),
age_limit_value       VARCHAR(4000),
material_name 		  VARCHAR(4000),
manufacturer_name 	  VARCHAR(4000),
customer_gender 	  VARCHAR(4000),
customer_card 		  VARCHAR(4000),
customer_birthdate 	  VARCHAR(4000),
supplier_name         VARCHAR(4000),
supplier_phone        VARCHAR(4000),
supplier_email 		  VARCHAR(4000),
payment_name 		  VARCHAR(4000),
channel_name 		  VARCHAR(4000),
channel_type 		  VARCHAR(4000),
store_name 			  VARCHAR(4000),
address_line 		  VARCHAR(4000),
city_name 			  VARCHAR(4000),
country_name 		  VARCHAR(4000),
employee_id  		  VARCHAR(4000),
employee_first_name   VARCHAR(4000),
employee_surname 	  VARCHAR(4000),
employee_gender 	  VARCHAR(4000),
event_dt 			  VARCHAR(4000),
quantity 			  VARCHAR(4000),
cost 				  VARCHAR(4000),
sale_price 			  VARCHAR(4000),
discount 			  VARCHAR(4000)
) 
SERVER pglog 
OPTIONS (filename 'C:/Program Files/PostgreSQL/15/data/log/Set_cash_man.csv', format 'csv' );
-- created physical table in schema
CREATE TABLE IF NOT EXISTS src_sales_cash(
product_name          VARCHAR(4000),
product_category_name VARCHAR(4000),
age_limit_value 	  VARCHAR(4000),
material_name 		  VARCHAR(4000),
manufacturer_name 	  VARCHAR(4000),
customer_gender 	  VARCHAR(4000),
customer_card 		  VARCHAR(4000),
customer_birthdate    VARCHAR(4000),
supplier_name 		  VARCHAR(4000),
supplier_phone 		  VARCHAR(4000),
supplier_email 		  VARCHAR(4000),
payment_name 		  VARCHAR(4000),
channel_name 		  VARCHAR(4000),
channel_type 		  VARCHAR(4000),
store_name 			  VARCHAR(4000),
address_line 		  VARCHAR(4000),
city_name 			  VARCHAR(4000),
country_name 		  VARCHAR(4000),
employee_id  		  VARCHAR(4000),
employee_first_name   VARCHAR(4000),
employee_surname 	  VARCHAR(4000),
employee_gender 	  VARCHAR(4000),
event_dt 			  VARCHAR(4000),
quantity 			  VARCHAR(4000),
cost 				  VARCHAR(4000),
sale_price 			  VARCHAR(4000),
discount 			  VARCHAR(4000),
loaded_flag 		  VARCHAR(4000)  DEFAULT 'N'
);
--created new schema for second dataset
CREATE SCHEMA IF NOT EXISTS sa_set_card;
SET search_path TO sa_set_card;
--created foreign table for the second dataset, the columns for the table correspond to the csv file and its format is, all columns have datatype: varchar

CREATE FOREIGN TABLE IF NOT EXISTS ext_sales_card(
product_name 		  VARCHAR(4000),
product_category_name VARCHAR(4000),
age_limit_value 	  VARCHAR(4000),
material_name 		  VARCHAR(4000),
manufacturer_name 	  VARCHAR(4000),
customer_gender 	  VARCHAR(4000),
customer_card 		  VARCHAR(4000),
customer_birthdate    VARCHAR(4000),
supplier_name 		  VARCHAR(4000),
supplier_phone 		  VARCHAR(4000),
supplier_email 		  VARCHAR(4000),
payment_name 		  VARCHAR(4000),
channel_name 		  VARCHAR(4000),
channel_type 		  VARCHAR(4000),
store_name 			  VARCHAR(4000),
address_line 		  VARCHAR(4000),
city_name 			  VARCHAR(4000),
country_name 		  VARCHAR(4000),
employee_id  		  VARCHAR(4000),
employee_first_name   VARCHAR(4000),
employee_surname 	  VARCHAR(4000),
employee_gender 	  VARCHAR(4000),
event_dt 			  VARCHAR(4000),
quantity 			  VARCHAR(4000),
cost 			 	  VARCHAR(4000),
sale_price 			  VARCHAR(4000),
discount 			  VARCHAR(4000)
) 
SERVER pglog 
OPTIONS (filename 'C:/Program Files/PostgreSQL/15/data/log/Set_card_female.csv', format 'csv' );

--created physical table in the second schema
CREATE TABLE IF NOT EXISTS src_sales_card (
product_name 		  VARCHAR(4000),
product_category_name VARCHAR(4000),
age_limit_value 	  VARCHAR(4000),
material_name 		  VARCHAR(4000),
manufacturer_name 	  VARCHAR(4000),
customer_gender 	  VARCHAR(4000),
customer_card 		  VARCHAR(4000),
customer_birthdate 	  VARCHAR(4000),
supplier_name 		  VARCHAR(4000),
supplier_phone 		  VARCHAR(4000),
supplier_email 		  VARCHAR(4000),
payment_name 		  VARCHAR(4000),
channel_name 		  VARCHAR(4000),
channel_type 		  VARCHAR(4000),
store_name 			  VARCHAR(4000),
address_line 		  VARCHAR(4000),
city_name 			  VARCHAR(4000),
country_name 		  VARCHAR(4000),
employee_id  		  VARCHAR(4000),
employee_first_name   VARCHAR(4000),
employee_surname 	  VARCHAR(4000),
employee_gender 	  VARCHAR(4000),
event_dt 			  VARCHAR(4000),
quantity 		      VARCHAR(4000),
cost 			 	  VARCHAR(4000),
sale_price 			  VARCHAR(4000),
discount 			  VARCHAR(4000),
loaded_flag 		  VARCHAR(4000)  DEFAULT 'N'
);

CREATE OR REPLACE PROCEDURE sa_insert () AS 
$$ 
BEGIN 
-- insert data into sa_set_cash.src_sales_cash from foreign table
    INSERT INTO sa_set_cash.src_sales_cash
    SELECT * FROM sa_set_cash.ext_sales_cash;
-- insert data into sa_set_card.src_sales_card from foreign table
    INSERT INTO sa_set_card.src_sales_card
    SELECT * FROM sa_set_card.ext_sales_card;

EXCEPTION 
    WHEN OTHERS THEN
        RAISE NOTICE 'Error code: %. Message text: %', SQLSTATE, SQLERRM;
END;
$$ LANGUAGE plpgsql;

CALL sa_insert ();              
