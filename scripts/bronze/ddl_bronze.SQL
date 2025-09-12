/*
_____________________________________________________________________________________________________________
DDL SCRIPT: CREATE BRONZE TABLE 
______________________________________________________________________________________________________________
Script Purpose:
    This script creates tables in the 'bronze' Database, dropping existing tables if they already exist
    run this script to re_run the structure of the DDL structure of 'bronze' Tables 
______________________________________________________________________________________________________________
*/




DROP TABLE IF EXISTS Bronze_Datawarehouse.crm_cust_info;
CREATE TABLE Bronze_Datawarehouse.crm_cust_info(
cst_id INT,
cst_key VARCHAR(50),
cst_firstname VARCHAR(50),
cst_lastname VARCHAR(50),
cst_marital_status VARCHAR(50),
cst_gndr VARCHAR(50),
cst_create_date DATETIME
);

DROP TABLE IF EXISTS Bronze_Datawarehouse.crm_prd_info;
CREATE TABLE Bronze_Datawarehouse.crm_prd_info(
prd_id INT,
prd_key VARCHAR(50),
prd_name VARCHAR(50),
prd_cost VARCHAR(50),
prd_line VARCHAR(50),
cst_start_dt DATETIME,
cst_end_dt DATETIME
);

DROP TABLE IF EXISTS Bronze_Datawarehouse.crm_sales_details;
CREATE TABLE Bronze_Datawarehouse.crm_sales_details(
sls_ord_num VARCHAR(50),
sls_prd_key VARCHAR(50),
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INT,
sls_price INT
);

DROP TABLE IF EXISTS Bronze_Datawarehouse.erp_loc_a101;
CREATE TABLE Bronze_Datawarehouse.erp_loc_a101(
cid VARCHAR(50),
cntry VARCHAR(50)
);

DROP TABLE IF EXISTS Bronze_Datawarehouse.erp_cust_az12;
CREATE TABLE Bronze_Datawarehouse.erp_cust_az12(
cid VARCHAR(50),
bdate DATETIME,
gen VARCHAR(50)
);

DROP TABLE IF EXISTS Bronze_Datawarehouse.erp_cat_g1v2;
CREATE TABLE Bronze_Datawarehouse.erp_cat_g1v2(
id VARCHAR(50),
cat DATETIME,
subcat VARCHAR(50),
maintenance VARCHAR(50)
);
