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

-- inserting the first test values ​​into the scd type2 table 3nf layer
CREATE OR REPLACE PROCEDURE nf_emp_scd_first() AS 
$$ 
DECLARE
    inserted_rows INTEGER;
   
BEGIN 
	inserted_rows := 0; 

	WITH cte_emp AS (
    				SELECT  tab.employee_src_id::bigint AS employee_id,
        					tab.employee_first_name,
        					tab.employee_surname,
        					tab.employee_gender,
        					tab.source_system,
        					tab.source_entity,
        					tab.employee_src_id,
       						'1900-01-01'::date AS start_dt,
        					'9999-12-31'::date AS end_dt,
        					'True' AS is_active,
        					CURRENT_DATE AS insert_dt,
        					ROW_NUMBER() OVER (PARTITION BY tab.employee_src_id ORDER BY tab.source_system) AS rn
   					FROM (
        					SELECT  COALESCE(employee_id, '-1') AS employee_id,
            						COALESCE(employee_first_name, 'n.a.') AS employee_first_name, 
            						COALESCE(employee_surname, 'n.a.')    AS employee_surname,
            						COALESCE(employee_gender, 'n.a.')     AS employee_gender,
            						'sa_set_card'    AS source_system,
            						'src_sales_card' AS source_entity,
            						COALESCE(employee_id, '-1') AS employee_src_id
        					FROM sa_set_card.src_sales_card
        					UNION ALL
        					SELECT  COALESCE(employee_id, '-1') AS employee_id,
            					    COALESCE(employee_first_name, 'n.a.') AS employee_first_name, 
            						COALESCE(employee_surname, 'n.a.')    AS employee_surname,
            						COALESCE(employee_gender, 'n.a.')     AS employee_gender,
            						'sa_set_cash'    AS source_system,
            						'src_sales_cash' AS source_entity,
            						COALESCE(employee_id, '-1') AS employee_src_id
        					FROM sa_set_cash.src_sales_cash
    					) tab 
    					)
	INSERT INTO bl_3nf.ce_employees_scd (employee_id, employee_first_name, employee_surname, employee_gender,
                                     source_system, source_entity, employee_src_id, start_dt, end_dt, is_active, insert_dt)
	(SELECT cte_emp.employee_id,
    		cte_emp.employee_first_name,
    		cte_emp.employee_surname,
    		cte_emp.employee_gender,
    		cte_emp.source_system,
   			cte_emp.source_entity,
    		cte_emp.employee_src_id,
    		cte_emp.start_dt,
    		cte_emp.end_dt,
    		cte_emp.is_active,
    		cte_emp.insert_dt
	FROM cte_emp
	WHERE rn = 1 AND NOT EXISTS (
    							SELECT 1 
    							FROM bl_3nf.ce_employees_scd scd
    							WHERE scd.source_system = cte_emp.source_system AND 
          							  scd.source_entity = cte_emp.source_entity AND 
          						      scd.employee_src_id = cte_emp.employee_src_id 
							));
			GET DIAGNOSTICS inserted_rows = ROW_COUNT;
    		CALL insert_into_log('nf_emp_scd_first', 'bl_3nf', 'ce_employees_scd', inserted_rows, 'Function executed successfully');

EXCEPTION
        WHEN OTHERS THEN
            CALL insert_into_log('nf_emp_scd_first', 'bl_3nf', 'ce_employees_scd', inserted_rows, 'Error occurred: ' || SQLERRM);
			RAISE NOTICE 'Error code: %. Message text: %', SQLSTATE, SQLERRM;
END;
$$ LANGUAGE plpgsql;

CALL bl_cl.nf_emp_scd_first()

-- inserting the first test values ​​into the scd type2 table dm layer
CREATE OR REPLACE PROCEDURE dm_emp_scd_first() AS 
$$ 
DECLARE
    inserted_rows INTEGER;
   
BEGIN 
	inserted_rows := 0; 

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
					WHERE   dm_scd.employee_src_id = nf_scd.employee_id::varchar 
					 ) AND nf_scd.employee_id != '-1' ); 
					
			GET DIAGNOSTICS inserted_rows = ROW_COUNT;
    		CALL insert_into_log('dm_emp_scd_first', 'bl_dm', 'dim_employees_scd', inserted_rows, 'Function executed successfully');

EXCEPTION
	WHEN OTHERS THEN
		CALL insert_into_log('dm_emp_scd_first', 'bl_dm', 'dim_employees_scd', inserted_rows, 'Error occurred: ' || SQLERRM);
		RAISE NOTICE 'Error code: %. Message text: %', SQLSTATE, SQLERRM;

END;
$$ LANGUAGE plpgsql;