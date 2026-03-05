
USE DataWarehouse;

--CRM Tables
-- Transforming Bronze Table & Loading into Silver Tables
--Clean & load crm_cust_info to Silver

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY
		SET @batch_start_time=GETDATE()
		PRINT '>> Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info
		PRINT '>> Truncating Data Into: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info 
		(
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
			TRIM(cst_firstname) [cst_firstname],
			TRIM(cst_lastname) [cst_lastname],
			CASE 
					WHEN UPPER(TRIM(cst_marital_status))='S' THEN 'Single'
					WHEN UPPER(TRIM(cst_marital_status))='M' THEN 'Married'
					ELSE 'n/a'
				END [cst_marital_status],
				CASE 
					WHEN UPPER(TRIM(cst_gndr))='F' THEN 'Female'
					WHEN UPPER(TRIM(cst_gndr))='M' THEN 'Male'
					ELSE 'n/a'
				END [cst_gndr],
			cst_create_date
		FROM 
		(
		SELECT*,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) [FirstCorrect]
		FROM bronze.crm_cust_info
		) AS t
		WHERE FirstCorrect =1 AND cst_id IS NOT NULL;


		--Clean & load crm_prd_info to Silver

		PRINT '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Truncating Data Into: silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info 
		(
			prd_id       ,
			cat_id       ,
			prd_key      ,
			prd_nm       ,
			prd_cost     ,
			prd_line     ,
			prd_start_dt ,
			prd_end_dt    
		)

		SELECT
			prd_id,
			REPLACE(SUBSTRING(prd_key,1,5),'-','_') [cat_id],
			SUBSTRING(prd_key,7,LEN(prd_key)) [prd_key],
			prd_nm,
			ISNULL(prd_cost,0) [prd_cost],
			CASE UPPER(TRIM(prd_line)) 
				 WHEN 'M' THEN 'Mountain'
				 WHEN 'R' THEN 'Road'
				 WHEN 'S' THEN 'Other Sales'
				 ElSE 'n/a'
			END [prd_line],
			CAST(prd_start_dt AS DATE) [prd_start_dt],
			CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) [prd_end_dt]
		FROM bronze.crm_prd_info;
		   

		--Clean & load crm_sales_details to Silver
		--connecting bronze.sls.prd_key to silver.crm_sales_details


		PRINT '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Truncating Data Into: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details
		(
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_price,
			sls_quantity,
			sls_total_price
		)

		SELECT
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt)!=8 THEN NULL
				 ELSE CAST(CAST(sls_order_dt AS VARCHAR)AS DATE)
			END [sls_order_dt],
	
			CASE WHEN sls_order_dt =0 OR LEN(sls_order_dt) !=8 THEN NULL
				 ELSE CAST(CAST(sls_order_dt AS VARCHAR)AS DATE)
			END [sls_ship_dt],
	
			CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) !=8 THEN NULL
				 ELSE CAST(CAST(sls_due_dt AS VARCHAR)AS DATE)
			END [sls_due_date],
			CASE WHEN sls_sales IS NULL 
								OR sls_sales<=0
								OR sls_sales != sls_quantity*ABS(sls_price)
								THEN sls_quantity*ABS(sls_price)
				 ELSE sls_sales
			END [sls_sales],

			CASE WHEN sls_price IS NULL OR sls_price <=0 THEN sls_sales/NULLIF(sls_quantity,0)
				 ELSE sls_price
			END [sls_total_price],

			sls_quantity,
			sls_price
		FROM bronze.crm_sales_details



		--Clean & Load erp_cust_az12 to Silver

		PRINT '>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Truncating Data Into: silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12
		(
			cid,
			bdate,
			gen
		)

		SELECT
			CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
				 ELSE cid
			END [cid],
	
			CASE WHEN bdate>GETDATE() THEN NULL
				 ELSE bdate
			END [bdate],
			CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
				 WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
				 ELSE 'n/a' 
			END [gen]
		FROM bronze.erp_cust_az12

		--Clean & Load erp_loc_a101 to Silver


		PRINT '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Truncating Data Into: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101
		(
			cid,
			cntry
		)
		SELECT
			REPLACE(cid,'-','') [cid],
			CASE WHEN TRIM(cntry) ='DE' THEN 'Germany'
				 WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
				 WHEN TRIM(cntry)='' OR TRIM(cntry) IS NULL THEN 'n/a'
				 ELSE TRIM(cntry)
			END [cntry]
		FROM bronze.erp_loc_a101;

		--Clean & Load erp_px_cat_g1v2 to Silver

		PRINT '>> Truncating Table: Silver.erp_px_cat_g1v2';
		TRUNCATE TABLE Silver.erp_px_cat_g1v2;
		PRINT '>> Truncating Data Into: Silver.erp_px_cat_g1v2';
		INSERT INTO Silver.erp_px_cat_g1v2
		(
			id,
			cat,
			subcat,
			maintenance
		)
		SELECT
			id,
			cat,
			subcat,
			maintenance
		FROM bronze.erp_px_cat_g1v2;
		SET @batch_end_time=GETDATE()
		PRINT'Total Load Time:' + CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR) + 'sec';
	END TRY
	BEGIN CATCH
	PRINT 'Error Message'+ CAST(Error_Message() AS NVARCHAR);
	PRINT 'Error Message'+ CAST(Error_Number() AS NVARCHAR);
	PRINT 'Error Message'+ CAST(Error_State() AS NVARCHAR);
	END CATCH
END	;

EXEC silver.load_silver;


