SET search_path TO BL_CL;

--dynamic sql is used to insert data, depending on parameteres 
CREATE OR REPLACE PROCEDURE dm_cust() AS 
$$
DECLARE
	inserted_rows INTEGER;
	din_query TEXT;

BEGIN 
	inserted_rows := 0;
	
	din_query =	'INSERT INTO bl_dm.dim_customers (customer_surr_id, customer_gender, customer_card, customer_birthdate_dt, source_system, 
													source_entity, customer_src_id, insert_dt, uptade_dt)
				(SELECT NEXTVAL(''bl_dm.seq_dim_customers''),
						customer_gender,
						customer_card,
						customer_birthdate_dt::date, 
						$1 AS source_system,
        				$2 AS source_entity,
						customer_id AS customer_src_id,
						CURRENT_DATE, 
						CURRENT_DATE 
				FROM bl_3nf.ce_customers nf_cus
				WHERE NOT EXISTS (
								SELECT 1 
								FROM bl_dm.dim_customers dm_cus
								WHERE dm_cus.customer_src_id = nf_cus.customer_id::varchar AND 
							  		  dm_cus.source_system = $1	
								) 
		  						AND customer_id != ''-1'')';
EXECUTE din_query USING 'bl_3nf', 'ce_customers';

				GET DIAGNOSTICS inserted_rows = ROW_COUNT;
    			CALL insert_into_log('dm_cust', 'bl_dm', 'dim_customers', inserted_rows, 'Function executed successfully');
    
EXCEPTION 
	WHEN OTHERS THEN
        CALL insert_into_log('dm_cust', 'bl_dm', 'dim_customers', inserted_rows, 'Error occurred: ' || SQLERRM);
		RAISE NOTICE 'Error code: %. Message text: %', SQLSTATE, SQLERRM;
	
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE dm_suppl() AS 
$$
DECLARE
    inserted_rows INTEGER;
   
BEGIN 
	inserted_rows := 0;
	
	INSERT INTO bl_dm.dim_suppliers(supplier_surr_id, supplier_name, supplier_phone, supplier_email, source_system, source_entity, 
									supplier_src_id, insert_dt, update_dt)
	(SELECT NEXTVAL('bl_dm.seq_dim_suppliers'),
			supplier_name,
			supplier_phone,
			supplier_email,
			'bl_3nf' AS source_system,
        	'ce_suppliers' AS source_entity,
        	supplier_id AS supplier_src_id,
        	CURRENT_DATE, 
			CURRENT_DATE 
	FROM bl_3nf.ce_suppliers nf_sup
	WHERE NOT EXISTS (
						SELECT 1 
						FROM bl_dm.dim_suppliers dm_sup
						WHERE dm_sup.supplier_src_id  = nf_sup.supplier_id::varchar AND 
							  dm_sup.source_system = 'bl_3nf'	
					) 
		  		AND supplier_id != '-1');
		  	
				GET DIAGNOSTICS inserted_rows = ROW_COUNT;
    			CALL insert_into_log('dm_suppl', 'bl_dm', 'dim_suppliers', inserted_rows, 'Function executed successfully');
   
EXCEPTION 
	WHEN OTHERS THEN
        CALL insert_into_log('dm_suppl', 'bl_dm', 'dim_suppliers', inserted_rows, 'Error occurred: ' || SQLERRM);
		RAISE NOTICE 'Error code: %. Message text: %', SQLSTATE, SQLERRM;
	
END;
$$ LANGUAGE plpgsql;
-- MERGE statement is used to merge the data in the one table with the second table based on a match in the value of the payment_name column. 
--If there is no record in the second table with the same payment_name, then a new record is inserted
CREATE OR REPLACE PROCEDURE dm_pay() AS 
$$
DECLARE
   	inserted_rows INTEGER;

BEGIN 
	inserted_rows := 0;

	MERGE INTO bl_dm.dim_payments AS tar_tab
USING (
    	SELECT  NEXTVAL('bl_dm.seq_dim_payments') AS payment_surr_id,
        		payment_name,
        		'bl_3nf' AS source_system,
        		'ce_payments' AS source_entity,
        		payment_id::VARCHAR AS payment_src_id,
        		CURRENT_DATE AS insert_dt,
        		CURRENT_DATE AS update_dt
    	FROM bl_3nf.ce_payments nf_pay
    	WHERE payment_id != '-1'
			) AS sour_tab
ON tar_tab.payment_name = sour_tab.payment_name
WHEN NOT MATCHED THEN
    INSERT (payment_surr_id, payment_name, source_system, source_entity, payment_src_id, insert_dt, update_dt)
    VALUES (sour_tab.payment_surr_id, 
    		sour_tab.payment_name, 
    		sour_tab.source_system, 
    		sour_tab.source_entity, 
    		sour_tab.payment_src_id, 
    		sour_tab.insert_dt, 
    		sour_tab.update_dt);		
			
				GET DIAGNOSTICS inserted_rows = ROW_COUNT;
    			CALL insert_into_log('dm_pay', 'bl_dm', 'dim_payments', inserted_rows, 'Function executed successfully');
 
EXCEPTION 
	WHEN OTHERS THEN
        CALL insert_into_log('dm_pay', 'bl_dm', 'dim_payments', inserted_rows, 'Error occurred: ' || SQLERRM);
		RAISE NOTICE 'Error code: %. Message text: %', SQLSTATE, SQLERRM;
	
END;
$$ LANGUAGE plpgsql;
--the dm_channels composite type is created, which contains two fields: channel_name and channel_type, 
--and this type is used to represent the data structure, which makes it easier to work with channel data in the dm_chan procedure
DO $$
BEGIN 
	IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'dm_channels') THEN 
		CREATE TYPE dm_channels AS (
				channel_name VARCHAR,
				channel_type VARCHAR
				);
	END IF;
END $$;

CREATE OR REPLACE PROCEDURE dm_chan() AS 
$$
DECLARE
   inserted_rows INTEGER;
   channels dm_channels;
  
BEGIN 
	inserted_rows := 0;
	
	INSERT INTO bl_dm.dim_channels(channel_surr_id, channel_name, channel_type, source_system, source_entity, channel_src_id, insert_dt, update_dt)
	(SELECT NEXTVAL ('bl_dm.seq_dim_channels'),
			nf_ch.channel_name,
			nf_ch.channel_type,
			'bl_3nf' AS source_system,
			'ce_channels' AS source_entity,
			 nf_ch.channel_id AS channel_src_id,
			 CURRENT_DATE, 
			 CURRENT_DATE 
	FROM bl_3nf.ce_channels nf_ch
	LEFT JOIN bl_dm.dim_channels dm_ch ON dm_ch.channel_src_id = nf_ch.channel_id::varchar AND 
                                          dm_ch.source_system = 'bl_3nf'
	WHERE NOT EXISTS (
						SELECT 1 
						FROM bl_dm.dim_channels dm_ch
						WHERE dm_ch.channel_src_id = nf_ch.channel_id::varchar AND 
							  dm_ch.source_system = 'bl_3nf'	
					) 
		  		AND channel_id != '-1');

				GET DIAGNOSTICS inserted_rows = ROW_COUNT;
    			CALL insert_into_log('dm_chan', 'bl_dm', 'dim_channels', inserted_rows, 'Function executed successfully');
   
EXCEPTION 
	WHEN OTHERS THEN
        CALL insert_into_log('dm_chan', 'bl_dm', 'dim_channels', inserted_rows, 'Error occurred: ' || SQLERRM);
		RAISE NOTICE 'Error code: %. Message text: %', SQLSTATE, SQLERRM;

END;
$$ LANGUAGE plpgsql;
--dimension dim_time is populated with generated data for 2020-2023
CREATE OR REPLACE PROCEDURE dm_time() AS 
$$
DECLARE
    inserted_rows INTEGER;
   
BEGIN 
	inserted_rows := 0;
	
	INSERT INTO bl_dm.dim_time_day (date_value, year_desc, quater_desc, month_desc, month_name, week_desc, day_desc, day_name, day_number_of_week,
									insert_dt, update_dt)
	(SELECT generated_date AS date_value,
			EXTRACT(YEAR FROM generated_date) AS year_desc,
    		EXTRACT(QUARTER FROM generated_date) AS quater_desc,
   			EXTRACT(MONTH FROM generated_date) AS month_desc,
			TO_CHAR(generated_date, 'Month') AS month_name,
			EXTRACT(WEEK FROM generated_date) AS week_desc,
			EXTRACT(DAY FROM generated_date) AS day_desc,
			TO_CHAR(generated_date, 'Day') AS day_name,
			EXTRACT(DOW FROM generated_date) AS day_number_of_week,
			CURRENT_DATE, 
			CURRENT_DATE
	FROM (
    		SELECT ('2020-01-01'::DATE + n) AS generated_date
    		FROM generate_series(0, ('2023-12-31'::DATE - '2020-01-01'::DATE)) AS n
		) AS in_date
	WHERE NOT EXISTS(
						SELECT 1 
						FROM bl_dm.dim_time_day dm_ti
						WHERE dm_ti.date_value = in_date.generated_date
					)); 
				
				GET DIAGNOSTICS inserted_rows = ROW_COUNT;
    			CALL insert_into_log('dm_time', 'bl_dm', 'dim_time_day', inserted_rows, 'Function executed successfully');
   
EXCEPTION 
	WHEN OTHERS THEN
        CALL insert_into_log('dm_time', 'bl_dm', 'dim_time_day', inserted_rows, 'Error occurred: ' || SQLERRM);
		RAISE NOTICE 'Error code: %. Message text: %', SQLSTATE, SQLERRM;

END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE dm_prod() AS 
$$
DECLARE
    inserted_rows INTEGER;
   
BEGIN 
	inserted_rows := 0;

	INSERT INTO bl_dm.dim_products(product_surr_id, product_name, product_categoty_id, product_category_name, age_limit_id, age_limit_value, 
								material_id, material_name, manufacturer_id, manufacturer_name, source_system, source_entity, 
								product_src_id, insert_dt, update_dt)
	(SELECT NEXTVAL('bl_dm.seq_dim_products'),
			nf_pr.product_name,
			nf_pr.product_category_id,
			nf_prc.product_category_name,
			nf_pr.age_limit_id,
			nf_age.age_limit_value,
			nf_pr.material_id,
			nf_mat.material_name,
			nf_pr.manufacturer_id,
			nf_man.manufacturer_name,
			'bl_3nf' AS source_system,
			'ce_products' AS source_entity,
			nf_pr.product_id AS product_src_id, 
			CURRENT_DATE, 
			CURRENT_DATE
	FROM bl_3nf.ce_products nf_pr
	LEFT JOIN bl_3nf.ce_product_category nf_prc ON nf_prc.product_category_id = nf_pr.product_category_id 
	LEFT JOIN bl_3nf.ce_age_limit nf_age ON nf_age.age_limit_id = nf_pr.age_limit_id 
	LEFT JOIN bl_3nf.ce_material nf_mat  ON nf_mat.material_id = nf_pr.material_id 
	LEFT JOIN bl_3nf.ce_manufacturer nf_man ON nf_man.manufacturer_id = nf_pr.manufacturer_id 
	WHERE NOT EXISTS (
						SELECT 1 
						FROM bl_dm.dim_products dm_pr
						WHERE dm_pr.product_src_id = nf_pr.product_id::varchar AND 
							  dm_pr.source_system = 'bl_3nf'	
					) 
		  		AND nf_pr.product_id != '-1'); 

				GET DIAGNOSTICS inserted_rows = ROW_COUNT;
    			CALL insert_into_log('dm_prod', 'bl_dm', 'dim_products', inserted_rows, 'Function executed successfully');
    
EXCEPTION 
	WHEN OTHERS THEN
        CALL insert_into_log('dm_prod', 'bl_dm', 'dim_products', inserted_rows, 'Error occurred: ' || SQLERRM);
		RAISE NOTICE 'Error code: %. Message text: %', SQLSTATE, SQLERRM;
	
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE dm_stores() AS 
$$
DECLARE
    inserted_rows INTEGER;
   
BEGIN 
	inserted_rows := 0;
		
	INSERT INTO bl_dm.dim_stores(store_surr_id, store_name, address_id, address_line, city_id, city_name, country_id, country_name, 
								source_system, source_entity, store_src_id, insert_dt, update_dt)
	(SELECT NEXTVAL ('bl_dm.seq_dim_stores'),
			nf_st.store_name,
			nf_st.address_id,
			nf_ad.address_line,
			nf_ad.city_id,
			nf_ci.city_name,
			nf_ci.country_id,
			nf_cou.country_name,
			'bl_3nf' AS source_system,
			'ce_store' AS source_entity,
			nf_st.store_id AS store_src_id,
			CURRENT_DATE, 
			CURRENT_DATE
	FROM bl_3nf.ce_stores nf_st
	LEFT JOIN bl_3nf.ce_address nf_ad ON nf_ad.address_id = nf_st.address_id 
	LEFT JOIN bl_3nf.ce_city nf_ci ON nf_ci.city_id = nf_ad.city_id 
	LEFT JOIN bl_3nf.ce_country nf_cou ON nf_cou.country_id = nf_ci.country_id 
	WHERE NOT EXISTS (
						SELECT 1 
						FROM bl_dm.dim_stores dm_st
						WHERE dm_st.store_src_id = nf_st.store_id::varchar AND 
							  dm_st.source_system = 'bl_3nf'	
					) 
		  		AND nf_st.store_id != '-1'); 

				GET DIAGNOSTICS inserted_rows = ROW_COUNT;
    			CALL insert_into_log('dm_stores', 'bl_dm', 'dim_stores', inserted_rows, 'Function executed successfully');
    
EXCEPTION 
	WHEN OTHERS THEN
        CALL insert_into_log('dm_stores', 'bl_dm', 'dim_stores', inserted_rows, 'Error occurred: ' || SQLERRM);
		RAISE NOTICE 'Error code: %. Message text: %', SQLSTATE, SQLERRM;

END;
$$ LANGUAGE plpgsql;
--loading data into scd type2
CREATE OR REPLACE PROCEDURE dm_emp_scd() AS 
$$ 
DECLARE
    inserted_rows INTEGER;
   
BEGIN 
	inserted_rows := 0; 
--flag 'false' and end date 'current_date' are set if there are records in which employee_src_id = employee_id 
--and not equal to at least one value from the first_name, surname, gender columns
	UPDATE bl_dm.dim_employees_scd AS dm_scd
	SET is_active = 'False',
    	end_dt = current_date
	WHERE 
		EXISTS (
		   		SELECT 1 
		   		FROM  bl_3nf.ce_employees_scd nf_scd
				WHERE dm_scd.employee_src_id = nf_scd.employee_id::varchar AND 
      			  	  (dm_scd.employee_first_name <> nf_scd.employee_first_name OR 
      			  	  dm_scd.employee_surname <> nf_scd.employee_surname        OR 
      			  	  dm_scd.employee_gender <> nf_scd.employee_gender)); 
--data is inserted if there are no rows at this level whose employee_src_id = employee_id and first_name, surname, gender are equal at the previous level			 
	INSERT INTO bl_dm.dim_employees_scd(employee_surr_id, employee_first_name, employee_surname, employee_gender, source_system, source_entity,
										employee_src_id, start_dt, end_dt, is_active, insert_dt)
	(SELECT nextval('bl_dm.seq_dim_employees_scd'),  
			nf_scd.employee_first_name,
			nf_scd.employee_surname,
			nf_scd.employee_gender,
			'bl_3nf' AS source_system,
			'ce_employees_scd' AS source_entity,
			nf_scd.employee_id AS employee_src_id,
			nf_scd.start_dt,
       		nf_scd.end_dt,
       		nf_scd.is_active,
       		CURRENT_DATE AS insert_dt
	FROM bl_3nf.ce_employees_scd nf_scd
	WHERE NOT EXISTS (
					SELECT 1 
					FROM bl_dm.dim_employees_scd dm_scd                                         
					WHERE dm_scd.employee_src_id = nf_scd.employee_id::varchar AND 
					      dm_scd.employee_first_name = nf_scd.employee_first_name AND 
      			  		  dm_scd.employee_surname = nf_scd.employee_surname       AND 
      			  		  dm_scd.employee_gender = nf_scd.employee_gender
					) AND nf_scd.employee_id != '-1' );  
					
			GET DIAGNOSTICS inserted_rows = ROW_COUNT;
    		CALL insert_into_log('dm_emp_scd', 'bl_dm', 'dim_employees_scd', inserted_rows, 'Function executed successfully');

EXCEPTION
        WHEN OTHERS THEN
            CALL insert_into_log('dm_emp_scd', 'bl_dm', 'dim_employees_scd', inserted_rows, 'Error occurred: ' || SQLERRM);
			RAISE NOTICE 'Error code: %. Message text: %', SQLSTATE, SQLERRM;

END;
$$ LANGUAGE plpgsql;
--a full load is used for the fact table, with the previous deletion of all information and a full insertion of a new one, 
--also partitioning by event_dt is used in this table
CREATE OR REPLACE PROCEDURE dm_fct_sales() AS
$$ 
DECLARE 
	inserted_rows INTEGER;

BEGIN 
	inserted_rows := 0;

DELETE FROM bl_dm.fct_sales_dd; 

INSERT INTO bl_dm.fct_sales_dd (event_dt, product_surr_id, customer_surr_id, supplier_surr_id, payment_surr_id, channel_surr_id,
								store_surr_id, employee_surr_id, quantity, "cost", sale_price, discount, calc_final_price, calc_revenue,
								insert_dt, update_dt)
SELECT  nf_sa.event_dt,
		dm_pr.product_surr_id,
		dm_cus.customer_surr_id,
		dm_sup.supplier_surr_id,
		dm_pay.payment_surr_id,
		dm_ch.channel_surr_id,
		dm_st.store_surr_id,
		dm_emp.employee_surr_id,
		nf_sa.quantity,
		nf_sa."cost",
		nf_sa.sale_price,
		nf_sa.discount,
		nf_sa.sale_price-nf_sa.discount AS calc_final_price,
		(nf_sa.sale_price-nf_sa.discount)*nf_sa.quantity AS calc_revenue,
		CURRENT_DATE,
		CURRENT_DATE
FROM bl_3nf.ce_sales nf_sa
LEFT JOIN bl_dm.dim_products dm_pr   ON dm_pr.product_src_id = nf_sa.product_id::varchar
LEFT JOIN bl_dm.dim_customers dm_cus ON dm_cus.customer_src_id = nf_sa.customer_id::varchar 
LEFT JOIN bl_dm.dim_suppliers dm_sup ON dm_sup.supplier_src_id = nf_sa.supplier_id::varchar 
LEFT JOIN bl_dm.dim_payments dm_pay  ON dm_pay.payment_src_id = nf_sa.payment_id::varchar 
LEFT JOIN bl_dm.dim_channels dm_ch   ON dm_ch.channel_src_id = nf_sa.channel_id::varchar 
LEFT JOIN bl_dm.dim_stores dm_st     ON dm_st.store_src_id = nf_sa.store_id::varchar  
LEFT JOIN bl_dm.dim_employees_scd dm_emp ON dm_emp.employee_src_id = nf_sa.employee_id::varchar AND dm_emp.is_active = 'True'; 


				GET DIAGNOSTICS inserted_rows = ROW_COUNT;
    			CALL insert_into_log('dm_fct_sales', 'bl_dm', 'fct_sales_dd', inserted_rows, 'Function executed successfully');
    
EXCEPTION 
	WHEN OTHERS THEN
        CALL insert_into_log('dm_fct_sales', 'bl_dm', 'fct_sales_dd', inserted_rows, 'Error occurred: ' || SQLERRM);
		RAISE NOTICE 'Error code: %. Message text: %', SQLSTATE, SQLERRM;

END;
$$ LANGUAGE plpgsql;