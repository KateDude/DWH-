SET search_path TO BL_CL;
--master procedure is created that includes calls to all procedures for inserting data into the BL_3NF and BL_DM levels
CREATE OR REPLACE PROCEDURE master() AS 
$$ 
DECLARE 
	inserted_rows INTEGER;

BEGIN 
	inserted_rows :=0;

		CALL bl_cl.nf_pr_cat();
		PERFORM bl_cl.nf_age_lim();
		CALL bl_cl.nf_material();
		CALL bl_cl.nf_manufac();
		CALL bl_cl.nf_products();
		CALL bl_cl.nf_suppl();
		CALL bl_cl.nf_pay();
		CALL bl_cl.nf_channels();
		CALL bl_cl.nf_customers(); 
		CALL bl_cl.nf_country();
		CALL bl_cl.nf_city();
		CALL bl_cl.nf_address();
		CALL bl_cl.nf_store();
		CALL bl_cl.nf_emp_scd(); --bl_cl.nf_emp_scd_first(); 
		CALL bl_cl.nf_sales();    
		CALL bl_cl.dm_cust();
		CALL bl_cl.dm_suppl(); 
		CALL bl_cl.dm_pay();
		CALL bl_cl.dm_chan();
		CALL bl_cl.dm_time();
		CALL bl_cl.dm_prod();
		CALL bl_cl.dm_stores();
		CALL bl_cl.dm_emp_scd(); --bl_cl.dm_emp_scd_first(); 
		CALL bl_cl.dm_fct_sales();

				GET DIAGNOSTICS inserted_rows = ROW_COUNT;
				CALL insert_into_log('master', '3nf/bl_dm', 'for all tables', inserted_rows, 'Function executed successfully');
    
EXCEPTION 
	WHEN OTHERS THEN
        CALL insert_into_log('master', '3nf/bl_dm', 'for all tables', inserted_rows, 'Error occurred: ' || SQLERRM);
		RAISE NOTICE 'Error code: %. Message text: %', SQLSTATE, SQLERRM;

END;
$$ LANGUAGE plpgsql;

CALL master(); 

