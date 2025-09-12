"""
**********************************************************************************************
 Script: Bronze Layer Loader (Python)

 Description:
 This script truncates existing tables (if data already exists in them) 
 and reloads fresh data from CSV files into the Bronze Datawarehouse schema.  
 It uses **LOAD DATA LOCAL INFILE** through Python’s MySQL connector, which allows 
 bulk inserts directly from CSV files.

 Features:
 1. Truncates each target table before loading to ensure clean reloads.
 2. Uses LOCAL INFILE for fast bulk CSV imports.
 3. Prints detailed messages showing which tables are being loaded.
 4. Captures and displays the load duration (in seconds) for each table.
 5. Captures the total runtime for the full load process.
 6. Handles errors gracefully:
    - If a table fails, it prints the MySQL error and continues loading the next table.

 Notes:
 - This script was added as a **fallback for Mac users** who may encounter 
   LOCAL INFILE restrictions when running SQL scripts directly in MySQL Workbench.
 - It performs the same Bronze Layer load procedure as the SQL script, 
   but with extra error handling and Python logging.

 Usage:
 Run this script with Python 3.  
 Make sure:
   - mysql-connector-python is installed (`pip install mysql-connector-python`).
   - LOCAL INFILE is enabled in your MySQL server settings.
   - File paths match your local environment.
**********************************************************************************************
"""
import mysql.connector as mysql
from time import time

# Map table names to CSV file paths
tables_and_files = {
    "crm_cust_info": "/Users/mac/Downloads/sql-data-warehouse-project 2/datasets/source_crm/cust_info.csv",
    "crm_prd_info": "/Users/mac/Downloads/sql-data-warehouse-project 2/datasets/source_crm/prd_info.csv",
    "crm_sales_details": "/Users/mac/Downloads/sql-data-warehouse-project 2/datasets/source_crm/sales_details.csv",
    "erp_cat_g1v2": "/Users/mac/Downloads/sql-data-warehouse-project 2/datasets/source_erp/PX_CAT_G1V2.csv",
    "erp_cust_az12": "/Users/mac/Downloads/sql-data-warehouse-project 2/datasets/source_erp/CUST_AZ12.csv",
    "erp_loc_a101": "/Users/mac/Downloads/sql-data-warehouse-project 2/datasets/source_erp/LOC_A101.csv"
}

# Define which tables belong to CRM vs ERP for grouping messages
crm_tables = ["crm_cust_info", "crm_prd_info", "crm_sales_details"]
erp_tables = ["erp_cat_g1v2", "erp_cust_az12", "erp_loc_a101"]

# Connect once
conn = mysql.connect(
    host="localhost",
    user="root",
    password="Davidasikpo2002!",
    database="Bronze_Datawarehouse",
    allow_local_infile=True
)

cursor = conn.cursor()

# Print Bronze Layer Header
print("\n" + "-" * 50)
print("LOADING BRONZE LAYER")
print("-" * 50 + "\n")


# Function to load a table with timing
def load_table(table, file_path):
    try:
        print(f"\n>> TRUNCATING TABLE: Bronze_Datawarehouse.{table}")
        cursor.execute(f"TRUNCATE TABLE {table};")

        print(f">> Inserting Data into: Bronze_Datawarehouse.{table}")
        start_time = time()
        query = f"""
        LOAD DATA LOCAL INFILE '{file_path}'
        INTO TABLE {table}
        FIELDS TERMINATED BY ','
        LINES TERMINATED BY '\\n'
        IGNORE 1 ROWS;
        """
        cursor.execute(query)
        conn.commit()
        end_time = time()
        duration = round(end_time - start_time, 2)
        print(f">> {table} loaded successfully! Load Duration: {duration} Seconds")
        print(">> ----------")
    except mysql.Error as err:
        print(f"ERROR OCCURRED WHILE LOADING TABLE: {table}")
        print(f"MySQL Error: {err}")
        print(">> ----------")


# Process CRM tables
print("-" * 50)
print("Loading CRM Tables")
print("-" * 50)
start_time_all = time()
for table in crm_tables:
    load_table(table, tables_and_files[table])

# Process ERP tables
print("\n" + "-" * 50)
print("Loading ERP Tables")
print("-" * 50)

for table in erp_tables:
    load_table(table, tables_and_files[table])
end_time_all = time()
duration1 = round(end_time_all - start_time_all, 2)

print(f"\n>> Duration of whole load process: {duration1} seconds")
# Close connection
cursor.close()
conn.close()

print("\nAll tables loaded successfully!")
