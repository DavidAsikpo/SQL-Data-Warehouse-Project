/*
===============================================================================
 Script: load_silver_procedure.sql
 Author: [Your Name]
 Description:
   This script defines a stored procedure `load_silver()` that performs the ETL 
   (Extract, Transform, Load) process from the Bronze layer to the Silver layer 
   in the data warehouse.

   Key Operations:
     - Extracts raw data from Bronze_Datawarehouse tables
     - Transforms data by:
         • Removing duplicates using ROW_NUMBER()
         • Standardizing gender, marital status, product lines, and country values
         • Cleaning unwanted spaces and special characters
         • Normalizing date formats and handling invalid values
         • Ensuring referential integrity between fact and dimension tables
     - Loads the cleaned and standardized data into Silver_Datawarehouse tables
     - Logs execution times for each table and the entire batch

 WARNING:
   ⚠️ This procedure TRUNCATES all Silver layer tables before reloading.
   Running it will PERMANENTLY DELETE existing Silver data and replace it 
   with the latest transformed dataset from Bronze.
   Do not execute in production without backups.

 Usage:
   CALL load_silver();
===============================================================================
*/
DELIMITER $$

CREATE PROCEDURE load_silver()
BEGIN
 DECLARE start_time DATETIME;
 DECLARE end_time DATETIME;
 DECLARE err_msg TEXT;
 DECLARE err_state CHAR(5);
 DECLARE err_no INT DEFAULT 0;
 DECLARE batch_start_time DATETIME;
 DECLARE batch_end_time DATETIME;
 
 DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN 
    GET DIAGNOSTICS CONDITION 1
    err_msg = MESSAGE_TEXT,
    err_no = MYSQL_ERRNO,
    err_state = returned_sqlstate;
    
    SELECT CONCAT('ERROR MESSAGE:', err_msg) AS MESSAGE;
    SELECT CONCAT('ERROR NO:', err_no) AS MESSAGE;
    SELECT CONCAT('ERROR STATE:', returned_sqlstate) AS MESSAGE;
END;

-- Sorting primary keys by most recent entry to remove duplicates
-- Removing unwated spaces from columsn
-- Data Standardization and Normalization

SET batch_start_time = NOW();
SELECT 'TRUNCATING TABLE: Silver_Datawarehouse.crm_cust_info';
SET start_time = NOW();
TRUNCATE TABLE Silver_Datawarehouse.crm_cust_info;
INSERT INTO Silver_Datawarehouse.crm_cust_info(
cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_marital_status,
cst_gndr,
cst_create_date)
SELECT cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname, 
TRIM(cst_lastname) AS cst_lastname,
CASE WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
     WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
     ELSE 'n/a' 
END cst_marital_status,

CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
     WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
     ELSE 'n/a' 
END cst_gndr,
CAST(cst_create_date AS DATE) AS cst_creat_date
FROM(
SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM Bronze_Datawarehouse.crm_cust_info
)t WHERE flag_last = 1 AND cst_id != 0;
SET end_time = NOW();
SELECT CONCAT('TIME DURATION TO LOAD Silver_Datawarehouse.crm_cust_info', TIMESTAMPDIFF(SECOND,start_time,end_time), 'Seconds');



SELECT 'TRUNCATING TABLE: Silver_Datawarehouse.crm_prd_info';
TRUNCATE TABLE Silver_Datawarehouse.crm_prd_info;
SET start_time = NOW();
INSERT INTO Silver_Datawarehouse.crm_prd_info(
prd_id ,
cat_id ,
prd_key ,
prd_nm ,
prd_cost,
prd_line ,
prd_start_dt,
prd_end_dt 

)
SELECT 
prd_id,
REPLACE(SUBSTRING(prd_key, 1,5), '-','_') AS cat_id,
SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_Key,
prd_nm,
IFNULL(NULLIF(prd_cost,''), 0 ) AS prd_cost,
CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
    WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
    WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
	WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
    ELSE 'n/a'
END AS prd_line,
CAST(prd_start_dt AS DATE) AS prd_start_dt,
CAST(DATE_SUB(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt),INTERVAL 1 DAY) AS DATE) AS prd_end_dt
FROM Bronze_Datawarehouse.crm_prd_info;
SET end_time = NOW();
SELECT CONCAT('TIME DURATION TO LOAD Silver_Datawarehouse.crm_prd_info', TIMESTAMPDIFF(SECOND,start_time,end_time), 'Seconds');





SELECT 'TRUNCATING TABLE: Silver_Datawarehouse.crm_sales_details';
TRUNCATE TABLE Silver_Datawarehouse.crm_sales_details;
SET start_time = NOW();
INSERT INTO Silver_Datawarehouse.crm_sales_details(
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
CASE WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt) != 8 THEN NULL
     ELSE STR_TO_DATE(CAST(sls_order_dt AS CHAR(8)), '%Y%m%d') 
END sls_order_dt,
CASE WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt) != 8 THEN NULL
     ELSE STR_TO_DATE(CAST(sls_ship_dt AS CHAR(8)), '%Y%m%d') 
END sls_ship_dt,
CASE WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt) != 8 THEN NULL
     ELSE STR_TO_DATE(CAST(sls_due_dt AS CHAR(8)), '%Y%m%d') 
END sls_due_dt,
CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
     THEN sls_quantity * ABS(sls_price)
	 ELSE sls_sales
END AS sls_sales,
sls_quantity,
CASE WHEN sls_price IS NULL OR sls_price <= 0 
     THEN sls_sales / NULLIF(sls_quantity, 0)
	 ELSE sls_price
END AS sls_price
FROM Bronze_Datawarehouse.crm_sales_details;
SET end_time = NOW();
SELECT CONCAT('TIME DURATION TO LOAD Silver_Datawarehouse.crm_sales_details', TIMESTAMPDIFF(SECOND,start_time,end_time), 'Seconds');





SELECT 'TRUNCATING TABLE: Silver_Datawarehouse.erp_cust_az12';
SET start_time = NOW();
TRUNCATE TABLE Silver_Datawarehouse.erp_cust_az12;
INSERT INTO Silver_Datawarehouse.erp_cust_az12
(cid,
bdate,
gen)

SELECT 
CASE WHEN cid LIKE 'NAS%'THEN SUBSTRING(cid, 4, LENGTH(cid))
	ELSE cid
END AS cid,
CASE WHEN bdate > NOW() THEN NULL
     ELSE bdate
END AS bdate,
CASE 
  WHEN UPPER(TRIM(REPLACE(gen, '\r', ''))) IN ('F', 'FEMALE') THEN 'Female'
  WHEN UPPER(TRIM(REPLACE(gen, '\r', ''))) IN ('M', 'MALE') THEN 'Male'
  ELSE 'n/a'
END AS gen
FROM Bronze_Datawarehouse.erp_cust_az12;
SET end_time = NOW();
SELECT CONCAT('TIME DURATION TO LOAD Silver_Datawarehouse.erp_cust_az12', TIMESTAMPDIFF(SECOND,start_time,end_time), 'Seconds');




SELECT 'TRUNCATING TABLE: Silver_Datawarehouse.erp_loc_a101';
TRUNCATE TABLE Silver_Datawarehouse.erp_loc_a101;
SET start_time = NOW();
INSERT INTO Silver_Datawarehouse.erp_loc_a101
(cid,
cntry)
SELECT 
  REPLACE(cid, '-', '') AS cid,
  CASE 
    WHEN UPPER(TRIM(REPLACE(cntry, '\r', ''))) = 'DE' 
         THEN 'Germany'
    WHEN UPPER(TRIM(REPLACE(cntry, '\r', ''))) IN ('US','USA') 
         THEN 'United States'
    WHEN TRIM(REPLACE(cntry, '\r', '')) = '' OR cntry IS NULL 
         THEN 'n/a'
    ELSE TRIM(REPLACE(cntry, '\r', ''))
  END AS cntry
FROM Bronze_Datawarehouse.erp_loc_a101;
SET end_time = NOW();
SELECT CONCAT('TIME DURATION TO LOAD Silver_Datawarehouse.erp_loc_a101', TIMESTAMPDIFF(SECOND,start_time,end_time), 'Seconds');





SELECT 'TRUNCATING TABLE: Silver_Datawarehouse.erp_cat_g1v2';
TRUNCATE TABLE Silver_Datawarehouse.erp_cat_g1v2;
SET start_time = NOW();
INSERT INTO Silver_Datawarehouse.erp_cat_g1v2
(
id,
cat,
subcat,
maintenance)
SELECT id,
cat,
subcat,
maintenance FROM Bronze_Datawarehouse.erp_cat_g1v2;
SET end_time = NOW();
SELECT CONCAT('TIME DURATION TO LOAD Silver_Datawarehouse.erp_loc_a101', TIMESTAMPDIFF(SECOND,start_time,end_time), 'Seconds');
SET batch_end_time = NOW();
SELECT CONCAT('TIME DURATION TO LOAD ALL TABLES', TIMESTAMPDIFF(SECOND,batch_start_time,batch_end_time), 'Seconds');


END $$

DELIMITER ;


