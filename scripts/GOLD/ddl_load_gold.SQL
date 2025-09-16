/* ============================================================
   GOLD LAYER: STAR SCHEMA VIEWS (dim_customers, dim_products, fact_sales)
   ============================================================

   This script creates the **Gold Layer** of the Data Warehouse 
   using views that represent the final star schema model for BI and analytics.

   - dim_customers: Cleanses and standardizes customer information 
     (joins customer, demographic, and location data).
   - dim_products: Provides enriched product details 
     (joins product info with product categories and filters active products).
   - fact_sales: Fact table that links sales transactions 
     with customers and products for analytical reporting.

   ⚠️ WARNING:
   These views reference the Silver layer. 
   Ensure that all **Silver_Datawarehouse tables** are loaded 
   before running this script, otherwise the views will fail.

   Purpose:
   - Provide a **business-friendly schema** for reporting.
   - Enable star schema analytics (facts + dimensions).
   - Standardize keys with surrogate keys (ROW_NUMBER).
============================================================ */
CREATE VIEW Gold_Datawarehouse.dim_customers AS
SELECT 
ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,
 ci.cst_id AS customer_id,
 ci.cst_key AS customer_number,
 ci.cst_firstname AS first_name,
 ci.cst_lastname AS last_name,
 la.cntry AS country,
 ci.cst_marital_status AS marital_status,
 CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
	ELSE COALESCE(ca.gen, 'n/a')
END AS gender,
 ca.bdate AS birthdate,
 ci.cst_create_date AS create_date
 FROM Silver_Datawarehouse.crm_cust_info ci
 LEFT JOIN silver_Datawarehouse.erp_cust_az12 ca
 ON ci.cst_key = ca.cid
 LEFT JOIN silver_Datawarehouse.erp_loc_a101 la
 ON ci.cst_key = la.cid;
 
 
 CREATE VIEW Gold_Datawarehouse.dim_products AS
 SELECT 
 ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key ) AS product_key,
 pn.prd_id AS product_id,
 pn.prd_key AS product_number,
 pn.prd_nm AS product_name,
 pn.cat_id AS category_id,
 pc.cat AS category,
 pc.subcat AS subcategory,
 pc.maintenance AS maintenance,
 pn.prd_cost AS product_cost,
 pn.prd_line AS product_line,
 pn.prd_start_dt AS start_date
 FROM Silver_DataWarehouse.crm_Prd_info pn
 LEFT JOIN Silver_Datawarehouse.erp_cat_g1v2 pc
 ON pn.cat_id = pc.id
 WHERE prd_end_dt IS NULL;
 

CREATE VIEW Gold_Datawarehouse.fact_sales AS
SELECT 
sd.sls_ord_num AS order_key,
pr.product_key,
cu.customer_Key,
sd.sls_order_dt AS order_date,
sd.sls_ship_dt AS shipping_date,
sd.sls_due_dt AS due_date,
sd.sls_sales AS sales_amount,
sd.sls_quantity AS quantity,
sd.sls_price AS price
FROM Silver_Datawarehouse.crm_sales_details sd
LEFT JOIN Gold_Datawarehouse.dim_products pr
ON sd.sls_Prd_key = pr.product_number
LEFT JOIN Gold_Datawarehouse.dim_customers cu
ON sd.sls_cust_id = cu.customer_id;




