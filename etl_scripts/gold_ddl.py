from config import get_connection

def main():
    # -------------------------------------------------------------
    # SQL Server connection (DataWarehouse)
    # -------------------------------------------------------------
    # conn = pyodbc.connect(
    #     'DRIVER={ODBC Driver 17 for SQL Server};'
    #     'SERVER=HUZAIFA\\SQLEXPRESS;'
    #     'DATABASE=DataWarehouse;'
    #     'Trusted_Connection=yes;'
    # )
    conn = get_connection()
    cursor = conn.cursor()

    print("=======================================")
    print(" Creating Gold Layer Views")
    print("=======================================")


    def run(sql):
        """Executes SQL script with GO separators."""
        for statement in sql.split("GO"):
            stmt = statement.strip()
            if stmt:
                cursor.execute(stmt)
        conn.commit()


    # -------------------------------------------------------------
    # GOLD VIEW DDL SCRIPT
    # -------------------------------------------------------------
    gold_views_sql = r"""/*
        ===============================================================================
        Gold Layer Views
        ===============================================================================
        */

        -- ================================
        -- gold.dim_customers
        -- ================================
        IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
            DROP VIEW gold.dim_customers;
        GO

        CREATE VIEW gold.dim_customers AS
        SELECT
            ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,
            ci.cst_id                          AS customer_id,
            ci.cst_key                         AS customer_number,
            ci.cst_firstname                   AS first_name,
            ci.cst_lastname                    AS last_name,
            la.cntry                           AS country,
            ci.cst_marital_status              AS marital_status,

            CASE 
                WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
                ELSE COALESCE(ca.gen, 'n/a')
            END                                AS gender,

            api.cst_phone_number               AS phone_number,
            ci.cst_create_date                 AS create_date
        FROM silver.crm_cust_info ci
        LEFT JOIN silver.erp_cust_az12 ca
            ON ci.cst_key = ca.cid
        LEFT JOIN silver.erp_loc_a101 la
            ON ci.cst_key = la.cid
        LEFT JOIN silver.api_cust_info api
            ON ci.cst_key = api.cst_key;
        GO


        -- ================================
        -- gold.dim_products
        -- ================================
        IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
            DROP VIEW gold.dim_products;
        GO

        CREATE VIEW gold.dim_products AS
        SELECT
            ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
            pn.prd_id               AS product_id,
            pn.prd_key              AS product_number,
            pn.prd_nm               AS product_name,
            pn.cat_id               AS category_id,
            pc.cat                  AS category,
            pc.subcat               AS subcategory,
            pc.maintenance          AS maintenance,
            api.prd_brand           AS brand,
            api.prd_manufacturer    AS manufacturer,
            pn.prd_cost             AS cost,
            pn.prd_line             AS product_line,
            pn.prd_start_dt         AS start_date
        FROM silver.crm_prd_info pn
        LEFT JOIN silver.erp_px_cat_g1v2 pc
            ON pn.cat_id = pc.id
        LEFT JOIN silver.api_prd_info api
            ON pn.cat_id = api.prd_id
        WHERE pn.prd_end_dt IS NULL;
        GO


        -- ================================
        -- gold.dim_date
        -- ================================
        IF OBJECT_ID('gold.dim_date', 'V') IS NOT NULL
            DROP VIEW gold.dim_date;
        GO

        CREATE VIEW gold.dim_date AS
        SELECT DISTINCT
            CONVERT(INT, FORMAT(d, 'yyyyMMdd')) AS date_key,
            d AS full_date,
            YEAR(d) AS year,
            MONTH(d) AS month,
            DAY(d) AS day,
            DATENAME(MONTH, d) AS month_name,
            DATEPART(QUARTER, d) AS quarter,
            DATENAME(WEEKDAY, d) AS weekday_name,
            DATEPART(WEEKDAY, d) AS weekday_number
        FROM (
            SELECT sls_order_dt AS d FROM silver.crm_sales_details
            UNION SELECT sls_ship_dt FROM silver.crm_sales_details
            UNION SELECT sls_due_dt FROM silver.crm_sales_details
        ) AS dt
        WHERE d IS NOT NULL;
        GO


        -- ================================
        -- gold.dim_location
        -- ================================
        IF OBJECT_ID('gold.dim_location', 'V') IS NOT NULL
            DROP VIEW gold.dim_location;
        GO

        CREATE VIEW gold.dim_location AS
        SELECT
            CONCAT('LOC', cs.cst_id) AS loc_key,
            la.cntry                 AS country,
            CASE 
                WHEN la.cntry = 'Australia'       THEN 'Sydney'
                WHEN la.cntry = 'Canada'          THEN 'Toronto'
                WHEN la.cntry = 'France'          THEN 'Paris'
                WHEN la.cntry = 'Germany'         THEN 'Berlin'
                WHEN la.cntry = 'United Kingdom'  THEN 'London'
                WHEN la.cntry = 'United States'   THEN 'New York'
                ELSE 'Unknown' 
            END AS city
        FROM silver.erp_loc_a101 la
        LEFT JOIN silver.crm_cust_info cs
            ON la.cid = cs.cst_key;
        GO


        -- ================================
        -- gold.fact_sales
        -- ================================
        IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
            DROP VIEW gold.fact_sales;
        GO

        CREATE VIEW gold.fact_sales AS
        SELECT
            sd.sls_ord_num      AS order_number,
            pr.product_key      AS product_key,
            cu.customer_key     AS customer_key,
            CONVERT(INT, FORMAT(sd.sls_order_dt, 'yyyyMMdd'))  AS date_key, 
            CONCAT('LOC', sd.sls_cust_id) AS loc_key,
            sd.sls_order_dt     AS order_date,
            sd.sls_ship_dt      AS shipping_date,
            sd.sls_due_dt       AS due_date,
            sd.sls_sales        AS sales_amount,
            sd.sls_quantity     AS quantity,
            sd.sls_price        AS price
        FROM silver.crm_sales_details sd
        LEFT JOIN gold.dim_products pr
            ON sd.sls_prd_key = pr.product_number
        LEFT JOIN gold.dim_customers cu
            ON sd.sls_cust_id = cu.customer_id;
        GO
    """

    # -------------------------------------------------------------
    # Execute SQL
    # -------------------------------------------------------------
    run(gold_views_sql)

    print("=======================================")
    print(" Gold Layer Views Created Successfully")
    print("=======================================")

    cursor.close()
    conn.close()

# Allows running this file directly
if __name__ == "__main__":
    main()  