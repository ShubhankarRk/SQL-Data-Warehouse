DROP PROCEDURE IF EXISTS bronze.load_bronze;


CREATE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Load data into bronze.crm_cust_info
    TRUNCATE TABLE bronze.crm_cust_info;
    EXECUTE FORMAT(
        'COPY bronze.crm_cust_info FROM %L WITH (FORMAT csv, HEADER true, DELIMITER '','')',
        '/path/to/cust_info.csv'
    );

    -- Load data into bronze.crm_prd_info
    TRUNCATE TABLE bronze.crm_prd_info;
    EXECUTE FORMAT(
        'COPY bronze.crm_prd_info FROM %L WITH (FORMAT csv, HEADER true, DELIMITER '','')',
        '/path/to/prd_info.csv'
    );

    -- Load data into bronze.crm_sales_details
    TRUNCATE TABLE bronze.crm_sales_detials;
    EXECUTE FORMAT(
        'COPY bronze.crm_sales_detials FROM %L WITH (FORMAT csv, HEADER true, DELIMITER '','')',
        '/path/to/sales_detials.csv'
    );

    -- Load data into bronze.erp_cust_az12
    TRUNCATE TABLE bronze.erp_cust_az12;
    EXECUTE FORMAT(
        'COPY bronze.erp_cust_az12 FROM %L WITH (FORMAT csv, HEADER true, DELIMITER '','')',
        '/path/to/erp_cust_az12.csv'
    );

    -- Load data into bronze.erp_loc_a101
    TRUNCATE TABLE bronze.erp_loc_a101;
    EXECUTE FORMAT(
        'COPY bronze.erp_loc_a101 FROM %L WITH (FORMAT csv, HEADER true, DELIMITER '','')',
        '/path/to/erp_loc_a101.csv'
    );

    -- Load data into bronze.erp_px_cat_g1v2
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    EXECUTE FORMAT(
        'COPY bronze.erp_px_cat_g1v2 FROM %L WITH (FORMAT csv, HEADER true, DELIMITER '','')',
        '/path/to/erp_px_cat_g1v2.csv'
    );
END;
$$;
