USE DataWarehouse;

--GOLD LAYER
--Creating Dimension Customers

CREATE VIEW gold.dim_customers
AS
SELECT
	ROW_NUMBER() OVER(ORDER BY cst_id) [customer_key],
	ci.cst_id [customer_id],
	ci.cst_key [customer_number],
	ci.cst_firstname [first_name],
	ci.cst_lastname [last_name],
	la.cntry [country],
	ci.cst_marital_status marital_status,
		CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr 
		 ELSE COALESCE(ca.gen,'n/a')
	END [gender],
	ca.bdate [birthdate],
	ci.cst_create_date [create_date]
FROM silver.crm_cust_info [ci]
LEFT JOIN silver.erp_cust_az12 [ca]
	ON ci.cst_key=ca.cid
LEFT JOIN silver.erp_loc_a101 [la]
	ON ci.cst_key = la.cid


--Creating Dimension Products

CREATE VIEW gold.dim_products
AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt, pn.prd_key) [product_key],
	prd_id [product_id],
	prd_key [product_number],
	prd_nm [product_name],
	cat_id [category_id],
	pc.cat [category ],
	pc.subcat [subcategory],
	pc.maintenance,
	pn.prd_cost [cost],
	prd_line [product_line],
	prd_start_dt [start_date]
FROM Silver.crm_prd_info [pn]
LEFT JOIN Silver.erp_px_cat_g1v2 [pc]
	ON pn.cat_id=pc.id
WHERE prd_end_dt IS NULL --filter out all historical data


--Creating Fact Table

--crm_sales(prd_key) --> crm_prd_info(prd_key)
--crm_sales(cst_id) --> crm_cust_info(cst_id)

CREATE VIEW gold.fact_sales
AS
SELECT
	sd.sls_ord_num [order_number], 
	pr.product_key,
	cu.customer_key [customer_key],
	sd.sls_order_dt [order_date],
	sd.sls_ship_dt [ship_date],
	sd.sls_due_dt [due_date],
	sd.sls_sales [sales],
	sd.sls_quantity [quantity],
	sd.sls_price [price]
FROM silver.crm_sales_details [sd]
LEFT JOIN gold.dim_products [pr]
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers [cu]
ON sd.sls_cust_id = cu.customer_id;	




