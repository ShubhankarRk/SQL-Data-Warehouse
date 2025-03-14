CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE 
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
BEGIN
    batch_start_time := NOW();
    RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading Silver Layer';
    RAISE NOTICE '================================================';

    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '------------------------------------------------';

    -- 🔹 Loading silver.crm_cust_info
    start_time := NOW();
    RAISE NOTICE '>> Truncating Table: silver.crm_cust_info';
    TRUNCATE TABLE silver.crm_cust_info;
    RAISE NOTICE '>> Inserting Data Into: silver.crm_cust_info';

    INSERT INTO silver.crm_cust_info (
        cst_id, 
        cst_key, 
        cst_firstname, 
        cst_lastname, 
        cst_marital_status, 
        cst_gndr,
        cst_create_date
    )
    SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname),
        TRIM(cst_lastname),
        CASE 
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'n/a'
        END,
        CASE 
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'n/a'
        END,
        cst_create_date
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
    ) t
    WHERE flag_last = 1; 

    end_time := NOW();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

    -- 🔹 Loading silver.crm_prd_info
    start_time := NOW();
    RAISE NOTICE '>> Truncating Table: silver.crm_prd_info';
    TRUNCATE TABLE silver.crm_prd_info;
    RAISE NOTICE '>> Inserting Data Into: silver.crm_prd_info';

    INSERT INTO silver.crm_prd_info (
        prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt
    )
    SELECT
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_'),
        SUBSTRING(prd_key, 7, LENGTH(prd_key)),
        prd_nm,
        COALESCE(prd_cost, 0),
        CASE 
            WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
            ELSE 'n/a'
        END,
        prd_start_dt::DATE,
        (LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL '1 day')::DATE
    FROM bronze.crm_prd_info;

    end_time := NOW();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

    -- 🔹 Loading silver.crm_sales_details
    start_time := NOW();
    RAISE NOTICE '>> Truncating Table: silver.crm_sales_details';
    TRUNCATE TABLE silver.crm_sales_details;
    RAISE NOTICE '>> Inserting Data Into: silver.crm_sales_details';

INSERT INTO silver.crm_sales_details (
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
)
SELECT 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE 
        WHEN LENGTH(sls_order_dt::TEXT) = 8 AND sls_order_dt::TEXT ~ '^[0-9]{8}$' 
            THEN TO_DATE(sls_order_dt::TEXT, 'YYYYMMDD')
        ELSE NULL 
    END AS sls_order_dt,
    CASE 
        WHEN LENGTH(sls_ship_dt::TEXT) = 8 AND sls_ship_dt::TEXT ~ '^[0-9]{8}$' 
            THEN TO_DATE(sls_ship_dt::TEXT, 'YYYYMMDD')
        ELSE NULL 
    END AS sls_ship_dt,
    CASE 
        WHEN LENGTH(sls_due_dt::TEXT) = 8 AND sls_due_dt::TEXT ~ '^[0-9]{8}$' 
            THEN TO_DATE(sls_due_dt::TEXT, 'YYYYMMDD')
        ELSE NULL 
    END AS sls_due_dt,
    CASE 
        WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
            THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales, 
    sls_quantity,
    CASE 
        WHEN sls_price IS NULL OR sls_price <= 0 
            THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price  
    END AS sls_price
FROM bronze.crm_sales_details;

    end_time := NOW();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

    -- 🔹 Loading silver.erp_cust_az12
    start_time := NOW();
    RAISE NOTICE '>> Truncating Table: silver.erp_cust_az12';
    TRUNCATE TABLE silver.erp_cust_az12;
    RAISE NOTICE '>> Inserting Data Into: silver.erp_cust_az12';

    INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
    SELECT
        CASE 
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
            ELSE cid
        END,
        CASE 
            WHEN bdate > NOW() THEN NULL
            ELSE bdate
        END,
        CASE 
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            ELSE 'n/a'
        END
    FROM bronze.erp_cust_az12;

    end_time := NOW();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

    -- 🔹 Loading silver.erp_loc_a101
    start_time := NOW();
    RAISE NOTICE '>> Truncating Table: silver.erp_loc_a101';
    TRUNCATE TABLE silver.erp_loc_a101;
    RAISE NOTICE '>> Inserting Data Into: silver.erp_loc_a101';

    INSERT INTO silver.erp_loc_a101 (cid, cntry)
    SELECT
        REPLACE(cid, '-', ''),
        CASE 
            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
            WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
            ELSE TRIM(cntry)
        END
    FROM bronze.erp_loc_a101;

    end_time := NOW();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

    -- 🔹 Loading silver.erp_px_cat_g1v2
    start_time := NOW();
    RAISE NOTICE '>> Truncating Table: silver.erp_px_cat_g1v2';
    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    RAISE NOTICE '>> Inserting Data Into: silver.erp_px_cat_g1v2';

    INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
    SELECT id, cat, subcat, maintenance FROM bronze.erp_px_cat_g1v2;

    batch_end_time := NOW();
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Loading Silver Layer is Completed';
    RAISE NOTICE '   - Total Load Duration: % seconds', EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));
    RAISE NOTICE '==========================================';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'ERROR OCCURRED: %', SQLERRM;
END $$;
