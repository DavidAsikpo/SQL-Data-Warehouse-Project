# 📊 Gold Layer Data Catalogue

## 📖 Overview  

The **Gold Layer** of the data warehouse represents the **curated, business-ready data**.  
Unlike the Bronze Layer (raw ingestion) and the Silver Layer (cleaned and standardized data), the Gold Layer provides **aggregated, enriched, and analytics-friendly tables** designed to support **reporting, dashboards, and advanced analytics**.  

- **Purpose**: To deliver **trusted, high-quality data** for decision-making across business units (sales, marketing, product, customer insights).  
- **Structure**: Tables are **modeled around business entities** (customers, products, sales, KPIs) and include **derived fields** (e.g., age, average order value, top products).  
- **Caution**: Since the Gold Layer depends on Silver, any **schema changes in Silver** must be reflected in Gold transformations to avoid data quality issues.  

This catalogue documents the **Gold Layer tables**, their columns, data types, and business meaning.  

---

## 🗂️ Gold Layer Tables  

### 1. `Gold_dim_customers`  
Dimension table storing **customer master data** with demographics and identifiers.  

| Column Name     | Data Type   | Description                                   |
|-----------------|-------------|-----------------------------------------------|
| Customer_Key    | Primary Key | Unique surrogate key for each customer.       |
| customer_id     | INT         | Source system identifier for the customer.    |
| customer_number | VARCHAR     | Business/customer reference number.           |
| first_name      | VARCHAR     | Customer’s first name.                        |
| last_name       | VARCHAR     | Customer’s last name.                         |
| country         | VARCHAR     | Country of the customer.                      |
| marital_status  | VARCHAR     | Marital status of the customer.               |
| gender          | VARCHAR     | Gender of the customer.                       |
| birthdate       | DATE        | Customer’s date of birth.                     |

---

### 2. `Gold_dim_products`  
Dimension table containing **product-related attributes** and categories.  

| Column Name     | Data Type   | Description                                   |
|-----------------|-------------|-----------------------------------------------|
| Product_key     | Primary Key | Unique surrogate key for each product.        |
| product_id      | INT         | Source system identifier for the product.     |
| product_number  | VARCHAR     | Business/product reference number.            |
| product_name    | VARCHAR     | Name of the product.                          |
| category_id     | VARCHAR     | Identifier for product category.              |
| category        | VARCHAR     | Product category (e.g., Electronics).         |
| subcategory     | VARCHAR     | Product subcategory.                          |
| maintenance     | VARCHAR     | Maintenance info (if applicable).             |
| cost            | DECIMAL     | Product cost.                                 |
| product_line    | VARCHAR     | Product line grouping.                        |
| start_date      | DATE        | Date product became active.                   |

---

### 3. `Gold_fact_sales`  
Fact table storing **transactional sales data**, linking customers and products.  

| Column Name     | Data Type   | Description                                   |
|-----------------|-------------|-----------------------------------------------|
| order_number    | VARCHAR     | Unique order number.                          |
| product_key     | Foreign Key | Links to `Gold_dim_products.Product_key`.     |
| customer_key    | Foreign Key | Links to `Gold_dim_customers.Customer_Key`.   |
| order_date      | DATE        | Date when the order was placed.               |
| shipping_date   | DATE        | Date when the order was shipped.              |
| due_date        | DATE        | Date when the order was due.                  |
| sales_amount    | DECIMAL     | Total sales amount for the order line.        |
| quantity        | INT         | Number of units sold.                         |
| price           | DECIMAL     | Unit price of the product.                    |

---


