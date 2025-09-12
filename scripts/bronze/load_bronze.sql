/**********************************************************************************************
 Script: Bronze Layer Loader (MySQL)

 Description:
 This script truncates existing tables (if data already exists in them) 
 and reloads fresh data from CSV files into the Bronze Datawarehouse schema.  
 It uses **LOAD DATA LOCAL INFILE** for bulk inserts, which makes loading faster and 
 more efficient compared to row-by-row inserts.

 Features:
 1. Truncates each target table before loading to ensure clean data reloads.
 2. Uses LOCAL INFILE for bulk CSV imports.
 3. Captures and displays the load duration (in seconds) for each table.
 4. Handles errors gracefully:
    - Displays error number, SQLSTATE, and detailed error message if an exception occurs.
 5. Separates CRM and ERP tables clearly during the load process.

 Notes:
 - In this same **Bronze** folder, there is a **Python script** that performs 
   the exact same procedure. This was added as a fallback for Mac users, since 
   MySQL’s LOCAL INFILE can sometimes be restricted or tricky on macOS.  
   The Python script handles the truncation + CSV load logic, 
   also tracks load durations, and prints any errors encountered.

 Usage:
 Run this script in MySQL Workbench or CLI.  
 Make sure LOCAL INFILE is enabled in your MySQL configuration and that 
 file paths are correct for your environment.
**********************************************************************************************/

DELIMITER $$

BEGIN
    DECLARE start_time DATETIME;
    DECLARE end_time DATETIME;
    DECLARE err_number INT DEFAULT 0;
    DECLARE err_msg TEXT;
    DECLARE err_sqlstate CHAR(5);

    -- Error handler
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        GET DIAGNOSTICS CONDITION 1
            err_number = MYSQL_ERRNO,
            err_msg = MESSAGE_TEXT,
            err_sqlstate = RETURNED_SQLSTATE;
        
        SELECT CONCAT('Error Number: ', err_number) AS message;
        SELECT CONCAT('SQLSTATE: ', err_sqlstate) AS message;
        SELECT CONCAT('Error Message: ', err_msg) AS message;
    END;

    -- ---------------------------------------------------
    -- Loading CRM Tables
    -- ---------------------------------------------------

    SET start_time = NOW();
    TRUNCATE TABLE Bronze_Datawarehouse.crm_cust_info;
    LOAD DATA LOCAL INFILE '/Users/mac/Downloads/sql-data-warehouse-project 2/datasets/source_crm/cust_info.csv'
        INTO TABLE Bronze_Datawarehouse.crm_cust_info
        FIELDS TERMINATED BY ','
        LINES TERMINATED BY '\n'
        IGNORE 1 ROWS;
    SET end_time = NOW();
    SELECT CONCAT('crm_cust_info Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' Seconds') AS message;

    SET start_time = NOW();
    TRUNCATE TABLE Bronze_Datawarehouse.crm_prd_info;
    LOAD DATA LOCAL INFILE '/Users/mac/Downloads/sql-data-warehouse-project 2/datasets/source_crm/prd_info.csv'
        INTO TABLE Bronze_Datawarehouse.crm_prd_info
        FIELDS TERMINATED BY ','
        LINES TERMINATED BY '\n'
        IGNORE 1 ROWS;
    SET end_time = NOW();
    SELECT CONCAT('crm_prd_info Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' Seconds') AS message;

    SET start_time = NOW();
    TRUNCATE TABLE Bronze_Datawarehouse.crm_sales_details;
    LOAD DATA LOCAL INFILE '/Users/mac/Downloads/sql-data-warehouse-project 2/datasets/source_crm/sales_details.csv'
        INTO TABLE Bronze_Datawarehouse.crm_sales_details
        FIELDS TERMINATED BY ','
        LINES TERMINATED BY '\n'
        IGNORE 1 ROWS;
    SET end_time = NOW();
    SELECT CONCAT('crm_sales_details Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' Seconds') AS message;

    -- ---------------------------------------------------
    -- Loading ERP Tables
    -- ---------------------------------------------------

    SET start_time = NOW();
    TRUNCATE TABLE Bronze_Datawarehouse.erp_cat_g1v2;
    LOAD DATA LOCAL INFILE '/Users/mac/Downloads/sql-data-warehouse-project 2/datasets/source_crm/PX_CAT_GIV2erp_cust_az12.csv'
        INTO TABLE Bronze_Datawarehouse.erp_cat_g1v2
        FIELDS TERMINATED BY ','
        LINES TERMINATED BY '\n'
        IGNORE 1 ROWS;
    SET end_time = NOW();
    SELECT CONCAT('erp_cat_g1v2 Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' Seconds') AS message;

    SET start_time = NOW();
    TRUNCATE TABLE Bronze_Datawarehouse.erp_cust_az12;
    LOAD DATA LOCAL INFILE '/Users/mac/Downloads/sql-data-warehouse-project 2/datasets/source_erp/CUST_AZ12.csv'
        INTO TABLE Bronze_Datawarehouse.erp_cust_az12
        FIELDS TERMINATED BY ','
        LINES TERMINATED BY '\n'
        IGNORE 1 ROWS;
    SET end_time = NOW();
    SELECT CONCAT('erp_cust_az12 Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' Seconds') AS message;

    SET start_time = NOW();
    TRUNCATE TABLE Bronze_Datawarehouse.erp_loc_a101;
    LOAD DATA LOCAL INFILE '/Users/mac/Downloads/sql-data-warehouse-project 2/datasets/source_erp/LOC_A101.csv'
        INTO TABLE Bronze_Datawarehouse.erp_loc_a101
        FIELDS TERMINATED BY ','
        LINES TERMINATED BY '\n'
        IGNORE 1 ROWS;
    SET end_time = NOW();
    SELECT CONCAT('erp_loc_a101 Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' Seconds') AS message;

END$$

DELIMITER ;
