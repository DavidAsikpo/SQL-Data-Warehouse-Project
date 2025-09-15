/*
===============================================================================
 Script: create_silver_schema.sql
 Author: [Your Name]
 Description:
   This script creates the **Silver_Datawarehouse** schema tables for CRM and ERP.
   It defines the following tables:
     - crm_cust_info
     - crm_prd_info
     - crm_sales_details
     - erp_loc_a101
     - erp_cust_az12
     - erp_cat_g1v2

 WARNING:
   ⚠️ This script uses `DROP TABLE IF EXISTS` before each `CREATE TABLE`.
   Running it will REMOVE ALL existing data in these tables permanently.
   Use with caution, especially in production environments.

 Usage:
   Execute this script in MySQL to reset and recreate the Silver layer tables.
===============================================================================
*/
DROP TABLE IF EXISTS Silver_Datawarehouse.crm_cust_info;
CREATE TABLE Silver_Datawarehouse.crm_cust_info(
cst_id INT,
cst_key VARCHAR(50),
cst_firstname VARCHAR(50),
cst_lastname VARCHAR(50),
cst_marital_status VARCHAR(50),
cst_gndr VARCHAR(50),
cst_create_date DATE,
dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS Silver_Datawarehouse.crm_prd_info;
CREATE TABLE Silver_Datawarehouse.crm_prd_info(
prd_id INT,
cat_id VARCHAR(50),
prd_key VARCHAR(50),
prd_nm VARCHAR(50),
prd_cost VARCHAR(50),
prd_line VARCHAR(50),
prd_start_dt DATE,
prd_end_dt DATE,
dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS Silver_Datawarehouse.crm_sales_details;
CREATE TABLE Silver_Datawarehouse.crm_sales_details(
sls_ord_num VARCHAR(50),
sls_prd_key VARCHAR(50),
sls_cust_id INT,
sls_order_dt DATE,
sls_ship_dt DATE,
sls_due_dt DATE,
sls_sales INT,
sls_quantity INT,
sls_price INT,
dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS Silver_Datawarehouse.erp_loc_a101;
CREATE TABLE Silver_Datawarehouse.erp_loc_a101(
cid VARCHAR(50),
cntry VARCHAR(50),
dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS Silver_Datawarehouse.erp_cust_az12;
CREATE TABLE Silver_Datawarehouse.erp_cust_az12(
cid VARCHAR(50),
bdate DATE,
gen VARCHAR(50),
dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS Silver_Datawarehouse.erp_cat_g1v2;
CREATE TABLE Silver_Datawarehouse.erp_cat_g1v2(
id VARCHAR(50),
cat VARCHAR(50),
subcat VARCHAR(50),
maintenance VARCHAR(50),
dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);
