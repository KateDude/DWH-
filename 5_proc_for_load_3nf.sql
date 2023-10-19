CREATE SCHEMA IF NOT EXISTS BL_CL;
SET search_path TO BL_CL;
--table for recording procedure logs is created
CREATE TABLE IF NOT EXISTS log_table (
datetime     TIMESTAMP,
procedure_name VARCHAR(50),
schema_name    VARCHAR(30),
table_name     VARCHAR(50),
number_of_rows_affected INTEGER,
text_message   VARCHAR (200)
);
--procedure is created to write the insert logs
CREATE OR REPLACE PROCEDURE insert_into_log(
p_procedure_name VARCHAR(50),
p_schema_name    VARCHAR(30),
p_table_name     VARCHAR(50),
p_number_of_rows_affected INTEGER,
p_text_message   VARCHAR(200)
) AS 
$$
BEGIN

    INSERT INTO log_table (datetime, procedure_name, schema_name, table_name, number_of_rows_affected, text_message)
    VALUES (current_timestamp, p_procedure_name, p_schema_name, p_table_name, p_number_of_rows_affected, p_text_message);

EXCEPTION 
    WHEN OTHERS THEN
		RAISE NOTICE 'Error code: %. Message text: %', SQLSTATE, SQLERRM;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE PROCEDURE nf_pr_cat() AS 
$$
DECLARE
	inserted_rows INTEGER;  
BEGIN 
	inserted_rows := 0;
--cte is created for better readability, inside which row_number is used to further filter unique product categories
	WITH cte_pr_cat AS (
						SELECT  COALESCE(product_category_name, 'n.a.') AS product_category_name,
								'sa_set_card'    AS source_system,
        						'src_sales_card' AS source_entity,
								COALESCE(product_category_name, 'n.a.') AS product_category_src_id,
								ROW_NUMBER() OVER (PARTITION BY product_category_name) AS rn
						FROM sa_set_card.src_sales_card
						UNION ALL 
						SELECT  COALESCE(product_category_name, 'n.a.') AS product_category_name,
        						'sa_set_cash'    AS source_system,
        						'src_sales_cash' AS source_entity,
        						COALESCE(product_category_name, 'n.a.') AS product_category_src_id,
        						ROW_NUMBER() OVER (PARTITION BY product_category_name) AS rn
    					FROM sa_set_cash.src_sales_cash)

	INSERT INTO bl_3nf.ce_product_category (product_category_id, product_category_name, source_system, source_entity, product_category_src_id, 
									 	insert_dt, update_dt)
	(SELECT NEXTVAL('bl_3nf.seq_product_category'),
       		cte_pr_cat.product_category_name,
       		cte_pr_cat.source_system,
       		cte_pr_cat.source_entity,  
       		cte_pr_cat.product_category_src_id,
       		CURRENT_DATE, 
       		CURRENT_DATE 
	FROM cte_pr_cat 
	WHERE rn = 1 AND NOT EXISTS (
    					SELECT 1
    					FROM bl_3nf.ce_product_category pc
    					WHERE pc.source_system = cte_pr_cat.source_system AND
          				  	  pc.source_entity = cte_pr_cat.source_entity AND
          				  	  pc.product_category_src_id = cte_pr_cat.product_category_src_id
				));
					
			GET DIAGNOSTICS inserted_rows = ROW_COUNT;
			CALL insert_into_log('nf_pr_cat', 'bl_3nf', 'ce_product_category', inserted_rows, 'Function executed successfully');
		
EXCEPTION
	WHEN OTHERS THEN
		CALL insert_into_log('nf_pr_cat', 'bl_3nf', 'ce_product_category', inserted_rows, 'Error occurred: ' || SQLERRM);
		RAISE NOTICE 'Error code: %. Message text: %', SQLSTATE, SQLERRM; 

END;
$$ LANGUAGE plpgsql;
--function is used to return the number of affected rows
CREATE OR REPLACE FUNCTION nf_age_lim() 
RETURNS SETOF INTEGER AS                 
$$
DECLARE
	inserted_rows INTEGER;
   
BEGIN 
	inserted_rows := 0;

	INSERT INTO bl_3nf.ce_age_limit (age_limit_id, age_limit_value, source_system, source_entity, age_limit_src_id, insert_dt, update_dt)
	(SELECT  NEXTVAL('bl_3nf.seq_age_limit'),
			dis_col.age_limit_value,
			dis_col.source_system,
       		dis_col.source_entity, 
			dis_col.age_limit_src_id,
			CURRENT_DATE, 
			CURRENT_DATE 
	FROM (
			SELECT  DISTINCT COALESCE(age_limit_value, 'n.a.') AS age_limit_value,
					'sa_set_card'    AS source_system,
        			'src_sales_card' AS source_entity,
					COALESCE(age_limit_value, 'n.a.') AS age_limit_src_id
			FROM sa_set_card.src_sales_card
			UNION ALL 
			SELECT  DISTINCT COALESCE(age_limit_value, 'n.a.') AS age_limit_value,
					'sa_set_cash'    AS source_system,
        			'src_sales_cash' AS source_entity,
					COALESCE(age_limit_value, 'n.a.') AS age_limit_src_id
			FROM sa_set_cash.src_sales_cash
		) AS dis_col
	WHERE NOT EXISTS (
						SELECT 1
    					FROM bl_3nf.ce_age_limit al
    					WHERE al.source_system = dis_col.source_system AND 
    					  	  al.source_entity = dis_col.source_entity AND 
    					  	  al.age_limit_src_id = dis_col.age_limit_src_id
					));
				
			GET DIAGNOSTICS inserted_rows = ROW_COUNT;                 
			CALL insert_into_log('nf_age_lim', 'bl_3nf', 'ce_age_limit', inserted_rows, 'Function executed successfully');
			RETURN NEXT inserted_rows; 
    
EXCEPTION 
	WHEN OTHERS THEN
        CALL insert_into_log('nf_age_lim', 'bl_3nf', 'ce_age_limit', inserted_rows, 'Error occurred: ' || SQLERRM);
		RAISE NOTICE 'Error code: %. Message text: %', SQLSTATE, SQLERRM;

END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE nf_material() AS 
$$
DECLARE
	inserted_rows INTEGER;
   
BEGIN 
	inserted_rows := 0;

	INSERT INTO bl_3nf.ce_material (material_id, material_name, source_system, source_entity, material_src_id, insert_dt, update_dt)
	(SELECT  NEXTVAL('bl_3nf.seq_material'),
			dis_col.material_name,
			dis_col.source_system,
        	dis_col.source_entity,
			dis_col.material_src_id,
			CURRENT_DATE, 
			CURRENT_DATE 
	FROM (
			SELECT DISTINCT COALESCE(material_name, 'n.a.') AS material_name,
					'sa_set_card'    AS source_system,
        			'src_sales_card' AS source_entity,
					COALESCE(material_name, 'n.a.') AS material_src_id
			FROM sa_set_card.src_sales_card
			UNION ALL
			SELECT DISTINCT COALESCE(material_name, 'n.a.') AS material_name,
					'sa_set_cash'    AS source_system,
        			'src_sales_cash' AS source_entity,
					COALESCE(material_name, 'n.a.') AS material_src_id
			FROM sa_set_cash.src_sales_cash
		) AS dis_col
	WHERE NOT EXISTS (
						SELECT 1
    					FROM bl_3nf.ce_material mat
    					WHERE mat.source_system = dis_col.source_system AND 
    					  	  mat.source_entity = dis_col.source_entity AND 
    					  	  mat.material_src_id = dis_col.material_src_id   
				));
		
			GET DIAGNOSTICS inserted_rows = ROW_COUNT;
    		CALL insert_into_log('nf_material', 'bl_3nf', 'ce_material', inserted_rows, 'Function executed successfully');
    
EXCEPTION 
	WHEN OTHERS THEN
        CALL insert_into_log('nf_material', 'bl_3nf', 'ce_material', inserted_rows, 'Error occurred: ' || SQLERRM);
		RAISE NOTICE 'Error code: %. Message text: %', SQLSTATE, SQLERRM;
	
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE nf_manufac() AS 
$$
DECLARE
	inserted_rows INTEGER;
   
BEGIN 
	inserted_rows := 0;
 
	INSERT INTO bl_3nf.ce_manufacturer (manufacturer_id, manufacturer_name, source_system, source_entity, manufacturer_src_id, 
										insert_dt, update_dt)
	(SELECT  NEXTVAL('bl_3nf.seq_manufacturer'),
			dis_col.manufacturer_name ,
			dis_col.source_system,
        	dis_col.source_entity,
			dis_col.manufacturer_src_id,
			CURRENT_DATE, 
			CURRENT_DATE 
	FROM (
			SELECT DISTINCT COALESCE(manufacturer_name , 'n.a.') AS manufacturer_name,
							'sa_set_card'    AS source_system,
        					'src_sales_card' AS source_entity,
							COALESCE(manufacturer_name , 'n.a.') AS manufacturer_src_id
			FROM sa_set_card.src_sales_card
			UNION ALL 
			SELECT DISTINCT COALESCE(manufacturer_name , 'n.a.') AS manufacturer_name,
							'sa_set_cash'    AS source_system,
        					'src_sales_cash' AS source_entity,
							COALESCE(manufacturer_name , 'n.a.') AS manufacturer_src_id
			FROM sa_set_cash.src_sales_cash
		) AS dis_col
	WHERE NOT EXISTS (
						SELECT 1
    					FROM bl_3nf.ce_manufacturer man 
    					WHERE man.source_system = dis_col.source_system AND 
    					  	  man.source_entity = dis_col.source_entity AND 
    					  	  man.manufacturer_src_id = dis_col.manufacturer_src_id  
					));
			
				GET DIAGNOSTICS inserted_rows = ROW_COUNT;
    			CALL insert_into_log('nf_manufac', 'bl_3nf', 'ce_manufacturer', inserted_rows, 'Function executed successfully');
    		    
EXCEPTION 
	WHEN OTHERS THEN
        CALL insert_into_log('nf_manufac', 'bl_3nf', 'ce_manufacturer', inserted_rows, 'Error occurred: ' || SQLERRM);
		RAISE NOTICE 'Error code: %. Message text: %', SQLSTATE, SQLERRM;
	
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE nf_products() AS 
$$											
DECLARE
	inserted_rows INTEGER;
   
BEGIN 
	inserted_rows := 0;

    INSERT INTO bl_3nf.ce_products (product_id, product_name, product_category_id, age_limit_id, material_id, manufacturer_id, 
                             source_system, source_entity, product_src_id, insert_dt, update_dt)
    (SELECT  NEXTVAL('bl_3nf.seq_products'),
            dis_col.product_name, 
            COALESCE(pcat.product_category_id, '-1') AS product_category_id,
            COALESCE(agel.age_limit_id, '-1')   AS age_limit_id,
            COALESCE(mat.material_id, '-1')     AS material_id,
            COALESCE(man.manufacturer_id, '-1') AS manufacturer_id,
            dis_col.source_system,
            dis_col.source_entity,
            dis_col.product_src_id,
            CURRENT_DATE,
            CURRENT_DATE
    FROM (
            SELECT DISTINCT COALESCE(product_name, 'n.a.') AS product_name, 
                            COALESCE(product_category_name, 'n.a.') AS product_category_name,
                            COALESCE(age_limit_value, 'n.a.') AS age_limit_value,
                            COALESCE(material_name, 'n.a.')   AS material_name,
                            COALESCE(manufacturer_name, 'n.a.')     AS manufacturer_name,
                            'sa_set_card'    AS source_system,
                            'src_sales_card' AS source_entity,
                            COALESCE(product_name, 'n.a.') AS product_src_id
            FROM sa_set_card.src_sales_card
            UNION ALL 
            SELECT DISTINCT COALESCE(product_name, 'n.a.') AS product_name,
                            COALESCE(product_category_name, 'n.a.') AS product_category_name,
                            COALESCE(age_limit_value, 'n.a.') AS age_limit_value,
                            COALESCE(material_name, 'n.a.')   AS material_name,
                            COALESCE(manufacturer_name, 'n.a.')     AS manufacturer_name,
                            'sa_set_cash'    AS source_system,
                            'src_sales_cash' AS source_entity,
                            COALESCE(product_name, 'n.a.') AS product_src_id  
            FROM sa_set_cash.src_sales_cash
        ) AS dis_col
    LEFT JOIN bl_3nf.ce_product_category pcat ON pcat.product_category_name = dis_col.product_category_name AND 
    											 pcat.source_system = dis_col.source_system                 AND 
    											 pcat.source_entity = dis_col.source_entity				
    LEFT JOIN bl_3nf.ce_age_limit agel   ON agel.age_limit_value = dis_col.age_limit_value AND 
    										agel.source_system = dis_col.source_system     AND 
    										agel.source_entity = dis_col.source_entity
    LEFT JOIN bl_3nf.ce_material mat     ON mat.material_name = dis_col.material_name AND 
    										mat.source_system = dis_col.source_system AND 
    										mat.source_entity = dis_col.source_entity
    LEFT JOIN bl_3nf.ce_manufacturer man ON man.manufacturer_name = dis_col.manufacturer_name AND 
    										man.source_system = dis_col.source_system         AND 
    										man.source_entity = dis_col.source_entity
    WHERE NOT EXISTS (
                        SELECT 1
                        FROM bl_3nf.ce_products pro
                        WHERE pro.source_system = dis_col.source_system AND 
                              pro.source_entity = dis_col.source_entity AND 
                              pro.product_src_id  = dis_col.product_src_id  
                ));

				GET DIAGNOSTICS inserted_rows = ROW_COUNT;
   				CALL insert_into_log('nf_products', 'bl_3nf', 'ce_products', inserted_rows, 'Function executed successfully');
      
EXCEPTION 
    WHEN OTHERS THEN
		CALL insert_into_log('nf_products', 'bl_3nf', 'ce_products', inserted_rows, 'Error occurred: ' || SQLERRM);
        RAISE NOTICE 'Error code: %. Message text: %', SQLSTATE, SQLERRM;

END;
$$ LANGUAGE plpgsql;
--cte is created for better readability, inside which row_number is used to further filter unique suppliers
CREATE OR REPLACE PROCEDURE nf_suppl() AS 
$$
DECLARE
	inserted_rows INTEGER;
   
BEGIN 
	inserted_rows := 0;

	WITH cte_suppliers AS (
    						SELECT  COALESCE(supplier_name, 'n.a.') AS supplier_name,
        							COALESCE(supplier_phone, 'n.a.') AS supplier_phone,
        							COALESCE(supplier_email, 'n.a.') AS supplier_email,
       								'sa_set_card' AS source_system,
        							'src_sales_card' AS source_entity,
        							COALESCE(supplier_name, 'n.a.') AS supplier_src_id,
        							ROW_NUMBER() OVER (PARTITION BY supplier_name) AS rn
    						FROM sa_set_card.src_sales_card
    						UNION ALL
    						SELECT  COALESCE(supplier_name, 'n.a.') AS supplier_name,
        							COALESCE(supplier_phone, 'n.a.') AS supplier_phone,
        							COALESCE(supplier_email, 'n.a.') AS supplier_email,
        							'sa_set_cash' AS source_system,
        							'src_sales_cash' AS source_entity,
        							COALESCE(supplier_name, 'n.a.') AS supplier_src_id,
        							ROW_NUMBER() OVER (PARTITION BY supplier_name) AS rn
    						FROM sa_set_cash.src_sales_cash
							)
	INSERT INTO bl_3nf.ce_suppliers (supplier_id, supplier_name, supplier_phone, supplier_email, source_system, source_entity, supplier_src_id,
                                 	 insert_dt, update_dt)
	SELECT 	NEXTVAL('bl_3nf.seq_suppliers'),
    		cte_suppliers.supplier_name,
    		cte_suppliers.supplier_phone,
    		cte_suppliers.supplier_email,
    		cte_suppliers.source_system,
    		cte_suppliers.source_entity,
    		cte_suppliers.supplier_src_id,
    		CURRENT_DATE,
    		CURRENT_DATE
	FROM cte_suppliers
	WHERE rn = 1 AND NOT EXISTS (
    							SELECT 1
    							FROM bl_3nf.ce_suppliers sup
    							WHERE sup.source_system = cte_suppliers.source_system AND 
    								  sup.source_entity = cte_suppliers.source_entity AND 
    								  sup.supplier_src_id = cte_suppliers.supplier_src_id
							);

				GET DIAGNOSTICS inserted_rows = ROW_COUNT;
    			CALL insert_into_log('nf_suppl', 'bl_3nf', 'ce_suppliers', inserted_rows, 'Function executed successfully');
    
EXCEPTION 
	WHEN OTHERS THEN
        CALL insert_into_log('nf_suppl', 'bl_3nf', 'ce_suppliers', inserted_rows, 'Error occurred: ' || SQLERRM);
		RAISE NOTICE 'Error code: %. Message text: %', SQLSTATE, SQLERRM;

END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE nf_pay() AS 
$$
DECLARE
	inserted_rows INTEGER;
   
BEGIN 
	inserted_rows := 0;

	INSERT INTO bl_3nf.ce_payments (payment_id, payment_name, source_system, source_entity, payment_src_id, insert_dt, update_dt)
	(SELECT  NEXTVAL('bl_3nf.seq_payments'),
       		dis_col.payment_name,
      		dis_col.source_system,
       		dis_col.source_entity,
       		dis_col.payment_src_id,
       		CURRENT_DATE, 
       		CURRENT_DATE 
	FROM (
			SELECT DISTINCT COALESCE(payment_name, 'n.a.') AS payment_name,
							'sa_set_card'    AS source_system,
        					'src_sales_card' AS source_entity,
							COALESCE(payment_name, 'n.a.') AS payment_src_id
			FROM sa_set_card.src_sales_card
			UNION ALL
			SELECT DISTINCT COALESCE(payment_name, 'n.a.') AS payment_name,
							'sa_set_cash'    AS source_system,
        					'src_sales_cash' AS source_entity,
							COALESCE(payment_name, 'n.a.') AS payment_src_id
			FROM sa_set_cash.src_sales_cash
		) AS dis_col
	WHERE NOT EXISTS (
    					SELECT 1
    					FROM bl_3nf.ce_payments pay 
    					WHERE pay.source_system = dis_col.source_system AND 
    					  	  pay.source_entity = dis_col.source_entity AND 
    					      pay.payment_src_id = dis_col.payment_src_id  
					));
			
				GET DIAGNOSTICS inserted_rows = ROW_COUNT;
    			CALL insert_into_log('nf_pay', 'bl_3nf', 'ce_payments', inserted_rows, 'Function executed successfully');
    		    
EXCEPTION 
	WHEN OTHERS THEN
        CALL insert_into_log('nf_pay', 'bl_3nf', 'ce_payments', inserted_rows, 'Error occurred: ' || SQLERRM);
		RAISE NOTICE 'Error code: %. Message text: %', SQLSTATE, SQLERRM;

END;
$$ LANGUAGE plpgsql;
--the cursor is used to insert rows into a table one by one
CREATE OR REPLACE PROCEDURE nf_channels() AS 
$$
DECLARE
    inserted_rows INTEGER;
    channel_cursor CURSOR FOR
        					(SELECT DISTINCT COALESCE(channel_name, 'n.a.') AS channel_name, 
            								 COALESCE(channel_type, 'n.a.') AS channel_type,
            								 'sa_set_card' AS source_system,
            								 'src_sales_card' AS source_entity,
           								 	 COALESCE(channel_name, 'n.a.') AS channel_src_id
        					FROM sa_set_card.src_sales_card
        					UNION ALL 
        					SELECT DISTINCT COALESCE(channel_name, 'n.a.') AS channel_name, 
            								COALESCE(channel_type, 'n.a.') AS channel_type,
            								'sa_set_cash' AS source_system,
            								'src_sales_cash' AS source_entity,
            								COALESCE(channel_name, 'n.a.') AS channel_src_id
        					FROM sa_set_cash.src_sales_cash);

    channel_record RECORD;
   
BEGIN 
    inserted_rows := 0;

    OPEN channel_cursor;
    LOOP
        FETCH channel_cursor INTO channel_record;
        EXIT WHEN NOT FOUND;
        IF NOT EXISTS (
            			SELECT 1
            			FROM bl_3nf.ce_channels ch
            			WHERE ch.source_system = channel_record.source_system AND 
                			  ch.source_entity = channel_record.source_entity AND 
                			  ch.channel_src_id = channel_record.channel_src_id
       				 ) THEN

	INSERT INTO bl_3nf.ce_channels (channel_id, channel_name, channel_type, source_system, source_entity, channel_src_id, insert_dt, update_dt)
	VALUES ( NEXTVAL('bl_3nf.seq_channels'),
             channel_record.channel_name,
             channel_record.channel_type,
             channel_record.source_system,
             channel_record.source_entity,
             channel_record.channel_src_id,
             CURRENT_DATE, 
             CURRENT_DATE
            );

            inserted_rows := inserted_rows + 1; 
           
            GET DIAGNOSTICS inserted_rows = ROW_COUNT;
    		CALL insert_into_log('nf_channels', 'bl_3nf', 'ce_channels', inserted_rows, 'Function executed successfully');
    
        END IF;
    END LOOP;
  
    CLOSE channel_cursor;
   
EXCEPTION 
    WHEN OTHERS THEN
        CALL insert_into_log('nf_channels', 'bl_3nf', 'ce_channels', inserted_rows, 'Error occurred: ' || SQLERRM);
        RAISE NOTICE 'Error code: %. Message text: %', SQLSTATE, SQLERRM;

END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE nf_customers() AS 
$$
DECLARE
	inserted_rows INTEGER;
   
BEGIN 
	inserted_rows := 0;

	INSERT INTO bl_3nf.ce_customers(customer_id, customer_gender, customer_card,customer_birthdate_dt, source_system, source_entity, 
									customer_src_id, insert_dt, update_dt)
	(SELECT  NEXTVAL('bl_3nf.seq_customers'),
			dis_col.customer_gender,
			dis_col.customer_card,
			dis_col.customer_birthdate::date, 
			dis_col.source_system,
        	dis_col.source_entity,
			dis_col.customer_src_id,
			CURRENT_DATE, 
			CURRENT_DATE 
	FROM (
    		SELECT DISTINCT COALESCE(customer_gender, 'n.a.')          AS customer_gender,
    						COALESCE(customer_card, 'n.a.') AS customer_card,
    						COALESCE(customer_birthdate, '1900-01-01') AS customer_birthdate,
    						'sa_set_card'    AS source_system,
        					'src_sales_card' AS source_entity,
    						COALESCE(customer_card, 'n.a.') AS customer_src_id
    		FROM sa_set_card.src_sales_card
    		UNION ALL 
    		SELECT DISTINCT COALESCE(customer_gender, 'n.a.')          AS customer_gender,
    						COALESCE(customer_card, 'n.a.') AS customer_card,
    						COALESCE(customer_birthdate, '1900-01-01') AS customer_birthdate,
    						'sa_set_cash'    AS source_system,
        					'src_sales_cash' AS source_entity,
    						COALESCE(customer_card, 'n.a.') AS customer_src_id
    		FROM sa_set_cash.src_sales_cash
		) AS dis_col 
	WHERE NOT EXISTS (
						SELECT 1
    					FROM bl_3nf.ce_customers cus
    					WHERE cus.source_system = dis_col.source_system AND 
    					  	  cus.source_entity = dis_col.source_entity AND 
    					  	  cus.customer_src_id = dis_col.customer_src_id 
					));
				
				GET DIAGNOSTICS inserted_rows = ROW_COUNT;
    			CALL insert_into_log('nf_customers', 'bl_3nf', 'ce_customers', inserted_rows, 'Function executed successfully');
    
EXCEPTION 
	WHEN OTHERS THEN
        CALL insert_into_log('nf_customers', 'bl_3nf', 'ce_customers', inserted_rows, 'Error occurred: ' || SQLERRM);
		RAISE NOTICE 'Error code: %. Message text: %', SQLSTATE, SQLERRM;

END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE nf_country() AS 
$$
DECLARE
	inserted_rows INTEGER;
   
BEGIN 
	inserted_rows := 0;

	INSERT INTO bl_3nf.ce_country(country_id, country_name, source_system, source_entity, country_src_id,  insert_dt, update_dt)
 	(SELECT  NEXTVAL('bl_3nf.seq_country'),
       		dis_col.country_name,
       		dis_col.source_system,
       		dis_col.source_entity,
       		dis_col.country_src_id,
       		CURRENT_DATE, 
       		CURRENT_DATE 
	FROM (
    		SELECT DISTINCT COALESCE(country_name, 'n.a.') AS country_name,
    						'sa_set_card'    AS source_system,
        					'src_sales_card' AS source_entity,
    						COALESCE(country_name, 'n.a.') AS country_src_id
    		FROM sa_set_card.src_sales_card
    		UNION ALL 
    		SELECT DISTINCT COALESCE(country_name, 'n.a.') AS country_name,
    						'sa_set_cash'    AS source_system,
        					'src_sales_cash' AS source_entity,
    						COALESCE(country_name, 'n.a.') AS country_src_id
    		FROM sa_set_cash.src_sales_cash
		) AS dis_col
	WHERE NOT EXISTS (
    					SELECT 1
    					FROM bl_3nf.ce_country cou
    					WHERE cou.source_system = dis_col.source_system AND 
    					  	  cou.source_entity = dis_col.source_entity AND 
    					  	  cou.country_src_id = dis_col.country_src_id
					));
				
				GET DIAGNOSTICS inserted_rows = ROW_COUNT;
    			CALL insert_into_log('nf_country', 'bl_3nf', 'ce_country', inserted_rows, 'Function executed successfully');
    
EXCEPTION 
	WHEN OTHERS THEN
        CALL insert_into_log('nf_country', 'bl_3nf', 'ce_country', inserted_rows, 'Error occurred: ' || SQLERRM);
		RAISE NOTICE 'Error code: %. Message text: %', SQLSTATE, SQLERRM;
	
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE nf_city() AS 
$$
DECLARE
	inserted_rows INTEGER;
   
BEGIN 
	inserted_rows := 0;

	INSERT INTO bl_3nf.ce_city (city_id, city_name, country_id, source_system, source_entity, city_src_id, insert_dt, update_dt)
	(SELECT NEXTVAL('bl_3nf.seq_city'),
	   	   dis_col.city_name,
	   	   COALESCE(cou.country_id, '-1') AS country_id,
	       dis_col.source_system,
           dis_col.source_entity,
           dis_col.city_src_id, 
           CURRENT_DATE,
           CURRENT_DATE
	FROM (
			SELECT DISTINCT COALESCE(city_name, 'n.a.') AS city_name,
							'sa_set_card'    AS source_system,
       						'src_sales_card' AS source_entity,
       						COALESCE(city_name, 'n.a.') AS city_src_id,
       						COALESCE(country_name, 'n.a.') AS country_name
			FROM sa_set_card.src_sales_card
			UNION ALL
			SELECT DISTINCT COALESCE(city_name, 'n.a.') AS city_name,
							'sa_set_cash'    AS source_system,
       						'src_sales_cash' AS source_entity,
       						COALESCE(city_name, 'n.a.') AS city_src_id,
       						COALESCE(country_name, 'n.a.') AS country_name
       		FROM sa_set_cash.src_sales_cash
		) AS dis_col
	LEFT JOIN bl_3nf.ce_country cou ON cou.source_system = dis_col.source_system AND 
							       	   cou.source_entity = dis_col.source_entity AND 
				                       cou.country_name = dis_col.country_name  
	WHERE NOT EXISTS (
    					SELECT 1
    					FROM bl_3nf.ce_city ci
    					WHERE ci.source_system = dis_col.source_system AND 
    					  	  ci.source_entity = dis_col.source_entity AND 
    					  	  ci.city_src_id  = dis_col.city_src_id
					));
				
				GET DIAGNOSTICS inserted_rows = ROW_COUNT;
    			CALL insert_into_log( 'nf_city', 'bl_3nf', 'ce_city', inserted_rows, 'Function executed successfully');
    
EXCEPTION 
	WHEN OTHERS THEN
        CALL insert_into_log('nf_city', 'bl_3nf', 'ce_city', inserted_rows, 'Error occurred: ' || SQLERRM);
		RAISE NOTICE 'Error code: %. Message text: %', SQLSTATE, SQLERRM;
	
END;
$$ LANGUAGE plpgsql;			

CREATE OR REPLACE PROCEDURE nf_address() AS 
$$
DECLARE
	inserted_rows INTEGER;
   
BEGIN 
	inserted_rows := 0;

	INSERT INTO bl_3nf.ce_address (address_id, address_line, city_id, source_system, source_entity, address_src_id, insert_dt, update_dt)
	(SELECT NEXTVAL('bl_3nf.seq_address'),
       		dis_col.address_line,
       		COALESCE(cc.city_id, '-1') AS city_id,
       		dis_col.source_system,
       		dis_col.source_entity,
       		dis_col.address_src_id, 
       		CURRENT_DATE,
       		CURRENT_DATE
	FROM (
			SELECT DISTINCT COALESCE(address_line, 'n.a.') AS address_line, 
							'sa_set_card'    AS source_system,
       						'src_sales_card' AS source_entity,
       						COALESCE(address_line,'n.a.')  AS address_src_id,
       						COALESCE(city_name, 'n.a.') AS city_name
			FROM sa_set_card.src_sales_card
			UNION ALL
			SELECT DISTINCT COALESCE(address_line, 'n.a.') AS address_line, 
							'sa_set_cash'    AS source_system,
       						'src_sales_cash' AS source_entity,
       						COALESCE(address_line, 'n.a.')  AS address_src_id,
       						COALESCE(city_name, 'n.a.') AS city_name
       		FROM sa_set_cash.src_sales_cash
		) AS dis_col
	LEFT JOIN bl_3nf.ce_city cc ON cc.source_system = dis_col.source_system AND 
						       	   cc.source_entity = dis_col.source_entity AND 
				                   cc.city_name  = dis_col.city_name  
	WHERE NOT EXISTS (
    					SELECT 1
    					FROM bl_3nf.ce_address ad
    					WHERE ad.source_system = dis_col.source_system AND 
    					  	  ad.source_entity = dis_col.source_entity AND 
    					      ad.address_src_id  = dis_col.address_src_id
					));
				
				GET DIAGNOSTICS inserted_rows = ROW_COUNT;
    			CALL insert_into_log('nf_address', 'bl_3nf', 'ce_address', inserted_rows, 'Function executed successfully');
    
EXCEPTION 
	WHEN OTHERS THEN
        CALL insert_into_log('nf_address', 'bl_3nf', 'ce_address', inserted_rows, 'Error occurred: ' || SQLERRM);
		RAISE NOTICE 'Error code: %. Message text: %', SQLSTATE, SQLERRM;
	
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE nf_store() AS 
$$
DECLARE
	inserted_rows INTEGER;
   
BEGIN 
	inserted_rows := 0;

	INSERT INTO bl_3nf.ce_stores (store_id, store_name, address_id, source_system, source_entity, store_src_id, insert_dt, update_dt)
	(SELECT NEXTVAL('bl_3nf.seq_stores'),
       		dis_col.store_name,
       		COALESCE(ca.address_id, '-1') AS address_id,
       		dis_col.source_system,
       		dis_col.source_entity,
       		dis_col.store_src_id, 
       		CURRENT_DATE,
       		CURRENT_DATE
FROM (
			SELECT DISTINCT COALESCE(store_name, 'n.a.') AS store_name, 
							'sa_set_card'    AS source_system,
       						'src_sales_card' AS source_entity,
       						COALESCE(store_name, 'n.a.')  AS store_src_id,
       						COALESCE(address_line, 'n.a.') AS address_line
			FROM sa_set_card.src_sales_card 
			UNION ALL
			SELECT DISTINCT COALESCE(store_name, 'n.a.') AS store_name, 
							'sa_set_cash'    AS source_system,
       						'src_sales_cash' AS source_entity,
       						COALESCE(store_name, 'n.a.')   AS store_src_id,
       						COALESCE(address_line, 'n.a.') AS address_line
			FROM sa_set_cash.src_sales_cash
		) AS dis_col
	LEFT JOIN bl_3nf.ce_address ca  ON ca.source_system = dis_col.source_system AND 
									  ca.source_entity = dis_col.source_entity  AND 
									  ca.address_line  = dis_col.address_line 

	WHERE NOT EXISTS (
    					SELECT 1
    					FROM bl_3nf.ce_stores st
    					WHERE st.source_system = dis_col.source_system AND 
    					  	  st.source_entity = dis_col.source_entity AND 
    					  	  st.store_src_id  = dis_col.store_src_id
					)); 
				
				GET DIAGNOSTICS inserted_rows = ROW_COUNT;
				CALL insert_into_log('nf_store', 'bl_3nf', 'ce_stores', inserted_rows, 'Function executed successfully');
    
EXCEPTION 
	WHEN OTHERS THEN
        CALL insert_into_log('nf_store', 'bl_3nf', 'ce_stores', inserted_rows, 'Error occurred: ' || SQLERRM);
		RAISE NOTICE 'Error code: %. Message text: %', SQLSTATE, SQLERRM;
	
END;
$$ LANGUAGE plpgsql;
--loading into table scd type2
CREATE OR REPLACE PROCEDURE nf_emp_scd() AS
$$ 
DECLARE
    inserted_rows INTEGER;
   
BEGIN 
	inserted_rows := 0; 
--a temporary table is created for further use
	CREATE TEMPORARY TABLE IF NOT EXISTS cte_emp AS
        					SELECT  COALESCE(employee_id, '-1') AS employee_id,
            						COALESCE(employee_first_name, 'n.a.') AS employee_first_name, 
            						COALESCE(employee_surname, 'n.a.') AS employee_surname,
            						COALESCE(employee_gender, 'n.a.') AS employee_gender,
            						'sa_set_card' AS source_system,
            						'src_sales_card' AS source_entity,
            						COALESCE(employee_id, '-1') AS employee_src_id,
            						ROW_NUMBER() OVER (PARTITION BY employee_id) AS rn
        					FROM sa_set_card.src_sales_card
        					UNION ALL
        					SELECT  COALESCE(employee_id, '-1')  AS employee_id,
            					    COALESCE(employee_first_name, 'n.a.') AS employee_first_name, 
            						COALESCE(employee_surname, 'n.a.') AS employee_surname,
            						COALESCE(employee_gender, 'n.a.') AS employee_gender,
            						'sa_set_cash' AS source_system,
            						'src_sales_cash' AS source_entity,
            						COALESCE(employee_id, '-1') AS employee_src_id,
            						ROW_NUMBER() OVER (PARTITION BY employee_id) AS rn
        					FROM sa_set_cash.src_sales_cash;
--rows are updated that have a difference in the specified columns, the 'false' flag is set and the end date is current_date	
UPDATE bl_3nf.ce_employees_scd AS ce
SET is_active = 'False',
    end_dt = current_date
WHERE 
	EXISTS (
		   	SELECT 1 
		   	FROM cte_emp
			WHERE ce.source_system = cte_emp.source_system     AND 
      			  ce.source_entity = cte_emp.source_entity     AND 
      			  ce.employee_src_id = cte_emp.employee_src_id  AND 
      			  (ce.employee_first_name <> cte_emp.employee_first_name OR 
      			  ce.employee_surname <> cte_emp.employee_surname        OR 
      			  ce.employee_gender <> cte_emp.employee_gender ));
--rows are inserted that are not yet present in the table according to the specified parameters      			 	 
INSERT INTO bl_3nf.ce_employees_scd (employee_id, employee_first_name, employee_surname, employee_gender,
                                     source_system, source_entity, employee_src_id, start_dt, end_dt, is_active, insert_dt)
(SELECT cte_emp.employee_src_id::bigint  AS employee_id,
       cte_emp.employee_first_name,
       cte_emp.employee_surname,
       cte_emp.employee_gender,
       cte_emp.source_system,
       cte_emp.source_entity,
       cte_emp.employee_src_id,
       current_date AS start_dt,
       '9999-12-31'::date AS end_dt,
       'True' AS is_active,
       CURRENT_DATE AS insert_dt
FROM cte_emp 
WHERE EXISTS (
					SELECT 1 
					FROM bl_3nf.ce_employees_scd scd
					WHERE scd.employee_id = cte_emp.employee_id::bigint AND 
					      scd.source_system = cte_emp.source_system     AND 
      			  		  scd.source_entity = cte_emp.source_entity     AND 
					      scd.employee_src_id = cte_emp.employee_src_id AND 
						  (scd.employee_first_name <> cte_emp.employee_first_name OR 
      			  		  scd.employee_surname <> cte_emp.employee_surname       OR 
      			          scd.employee_gender <> cte_emp.employee_gender) AND 
						  scd.is_active = 'False'
					));  

			GET DIAGNOSTICS inserted_rows = ROW_COUNT;
    		CALL insert_into_log('nf_emp_scd', 'bl_3nf', 'ce_employees_scd', inserted_rows, 'Function executed successfully');

EXCEPTION
        WHEN OTHERS THEN
            CALL insert_into_log('nf_emp_scd', 'bl_3nf', 'ce_employees_scd', inserted_rows, 'Error occurred: ' || SQLERRM);
			RAISE NOTICE 'Error code: %. Message text: %', SQLSTATE, SQLERRM;

END;
$$ LANGUAGE plpgsql;

/**cte was created for better readability; 
incremental loading is applied depending on the flag (N / Y), after the necessary insertion, the data is updated to the flag (Y), 
the next time only the previously unloaded rows are loaded
**/
CREATE OR REPLACE PROCEDURE nf_sales() AS  
$$
DECLARE 
	inserted_rows INTEGER;

BEGIN 
	inserted_rows :=0;

WITH new_sales AS  (
    					SELECT  product_name   AS product_src_id,
        						store_name     AS store_src_id,
        						channel_name   AS channel_src_id,
        						payment_name   AS payment_src_id,
        						supplier_name  AS supplier_src_id,
        						customer_card  AS customer_src_id,
        						employee_id    AS employee_src_id,
        						event_dt::date AS event_date,
        						quantity::integer,
        						cost::float,
        						sale_price::float,
        						discount::float,
        						'sa_set_card'    AS source_system,
            					'src_sales_card' AS source_entity
    					FROM sa_set_card.src_sales_card
   						WHERE loaded_flag = 'N'
    					UNION ALL
    					SELECT  product_name   AS product_src_id,
        						store_name     AS store_src_id,
        						channel_name   AS channel_src_id,
        						payment_name   AS payment_src_id,
        						supplier_name  AS supplier_src_id,
        						customer_card  AS customer_src_id,
        						employee_id    AS employee_src_id,
        						event_dt::date AS event_date,
        						quantity::integer,
       							cost::float,
        						sale_price::float, 
        						discount::float,
        						'sa_set_cash'    AS source_system,
            					'src_sales_cash' AS source_entity
    					FROM sa_set_cash.src_sales_cash
    					WHERE loaded_flag = 'N'
						) 
	INSERT INTO bl_3nf.ce_sales (product_id, store_id, channel_id, payment_id, supplier_id, customer_id, employee_id, event_dt, quantity, cost, 
	 							  sale_price, discount, insert_dt, update_dt)
	(SELECT pro.product_id, 
			st.store_id, 
			ch.channel_id, 
			pay.payment_id, 
			sup.supplier_id,
			cus.customer_id, 
			emp.employee_id, 
			asal.event_date, 
			asal.quantity, 
			asal.cost,    
			asal.sale_price, 
			asal.discount, 
			CURRENT_DATE, 
			CURRENT_DATE
	FROM new_sales asal 
	LEFT JOIN bl_3nf.ce_products pro  ON pro.product_src_id = asal.product_src_id 
	LEFT JOIN bl_3nf.ce_stores st     ON st.store_src_id = asal.store_src_id
	LEFT JOIN bl_3nf.ce_channels ch   ON ch.channel_src_id = asal.channel_src_id
	LEFT JOIN bl_3nf.ce_payments pay  ON pay.payment_src_id = asal.payment_src_id 
	LEFT JOIN bl_3nf.ce_suppliers sup ON sup.supplier_src_id = asal.supplier_src_id
	LEFT JOIN bl_3nf.ce_customers cus ON cus.customer_src_id = asal.customer_src_id
	LEFT JOIN bl_3nf.ce_employees_scd emp ON emp.employee_src_id = asal.employee_src_id AND emp.is_active = 'True'
											);

				GET DIAGNOSTICS inserted_rows = ROW_COUNT;
    			CALL insert_into_log('nf_sales', 'bl_3nf', 'ce_sales', inserted_rows, 'Function executed successfully');

	UPDATE sa_set_card.src_sales_card SET loaded_flag = 'Y' WHERE loaded_flag = 'N';
	UPDATE sa_set_cash.src_sales_cash SET loaded_flag = 'Y' WHERE loaded_flag = 'N';
   
EXCEPTION 
	WHEN OTHERS THEN
        CALL insert_into_log('nf_sales', 'bl_3nf', 'ce_sales', inserted_rows, 'Error occurred: ' || SQLERRM);
		RAISE NOTICE 'Error code: %. Message text: %', SQLSTATE, SQLERRM;

END;
$$ LANGUAGE plpgsql;