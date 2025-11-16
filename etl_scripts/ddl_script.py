from config import get_connection

def main():
    conn = get_connection()
    cursor = conn.cursor()

    print("=======================================")
    print(" Creating Bronze + Silver Tables")
    print("=======================================")

    def run(sql):
        """Executes a SQL script that may contain multiple statements"""
        for statement in sql.split("GO"):
            stmt = statement.strip()
            if stmt:
                cursor.execute(stmt)
        conn.commit()


    # ==========================================================
    #  BRONZE TABLES
    # ==========================================================

    bronze_sql = r"""
    IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
        DROP TABLE bronze.crm_cust_info;
    CREATE TABLE bronze.crm_cust_info (
        cst_id              INT,
        cst_key             NVARCHAR(50),
        cst_firstname       NVARCHAR(50),
        cst_lastname        NVARCHAR(50),
        cst_marital_status  NVARCHAR(50),
        cst_gndr            NVARCHAR(50),
        cst_create_date     DATE
    );

    IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
        DROP TABLE bronze.crm_prd_info;
    CREATE TABLE bronze.crm_prd_info (
        prd_id       INT,
        prd_key      NVARCHAR(50),
        prd_nm       NVARCHAR(50),
        prd_cost     INT,
        prd_line     NVARCHAR(50),
        prd_start_dt DATETIME,
        prd_end_dt   DATETIME
    );

    IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
        DROP TABLE bronze.crm_sales_details;
    CREATE TABLE bronze.crm_sales_details (
        sls_ord_num  NVARCHAR(50),
        sls_prd_key  NVARCHAR(50),
        sls_cust_id  INT,
        sls_order_dt INT,
        sls_ship_dt  INT,
        sls_due_dt   INT,
        sls_sales    INT,
        sls_quantity INT,
        sls_price    INT
    );

    IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
        DROP TABLE bronze.erp_loc_a101;
    CREATE TABLE bronze.erp_loc_a101 (
        cid    NVARCHAR(50),
        cntry  NVARCHAR(50)
    );

    IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
        DROP TABLE bronze.erp_cust_az12;
    CREATE TABLE bronze.erp_cust_az12 (
        cid    NVARCHAR(50),
        bdate  DATE,
        gen    NVARCHAR(50)
    );

    IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
        DROP TABLE bronze.erp_px_cat_g1v2;
    CREATE TABLE bronze.erp_px_cat_g1v2 (
        id           NVARCHAR(50),
        cat          NVARCHAR(50),
        subcat       NVARCHAR(50),
        maintenance  NVARCHAR(50)
    );

    IF OBJECT_ID('bronze.api_cust_info', 'U') IS NOT NULL
        DROP TABLE bronze.api_cust_info;
    CREATE TABLE bronze.api_cust_info (
        id               NVARCHAR(50),
        cst_key          NVARCHAR(50),
        phone_number     NVARCHAR(50)
    );

    IF OBJECT_ID('bronze.api_prod_info', 'U') IS NOT NULL
        DROP TABLE bronze.api_prod_info;
    CREATE TABLE bronze.api_prod_info (
        id           NVARCHAR(50),
        cat          NVARCHAR(50),
        brand        NVARCHAR(50),
        manufacturer NVARCHAR(50)
    );
    """

    print("Creating Bronze Tables...")
    run(bronze_sql)
    print("Bronze Tables Created.\n")


    # ==========================================================
    #  SILVER TABLES
    # ==========================================================

    silver_sql = r"""
    IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
        DROP TABLE silver.crm_cust_info;
    CREATE TABLE silver.crm_cust_info (
        cst_id             INT,
        cst_key            NVARCHAR(50),
        cst_firstname      NVARCHAR(50),
        cst_lastname       NVARCHAR(50),
        cst_marital_status NVARCHAR(50),
        cst_gndr           NVARCHAR(50),
        cst_create_date    DATE,
        dwh_create_date    DATETIME2 DEFAULT GETDATE()
    );

    IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
        DROP TABLE silver.crm_prd_info;
    CREATE TABLE silver.crm_prd_info (
        prd_id          INT,
        cat_id          NVARCHAR(50),
        prd_key         NVARCHAR(50),
        prd_nm          NVARCHAR(50),
        prd_cost        INT,
        prd_line        NVARCHAR(50),
        prd_start_dt    DATE,
        prd_end_dt      DATE,
        dwh_create_date DATETIME2 DEFAULT GETDATE()
    );

    IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
        DROP TABLE silver.crm_sales_details;
    CREATE TABLE silver.crm_sales_details (
        sls_ord_num     NVARCHAR(50),
        sls_prd_key     NVARCHAR(50),
        sls_cust_id     INT,
        sls_order_dt    DATE,
        sls_ship_dt     DATE,
        sls_due_dt      DATE,
        sls_sales       INT,
        sls_quantity    INT,
        sls_price       INT,
        dwh_create_date DATETIME2 DEFAULT GETDATE()
    );

    IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
        DROP TABLE silver.erp_loc_a101;
    CREATE TABLE silver.erp_loc_a101 (
        cid             NVARCHAR(50),
        cntry           NVARCHAR(50),
        dwh_create_date DATETIME2 DEFAULT GETDATE()
    );

    IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
        DROP TABLE silver.erp_cust_az12;
    CREATE TABLE silver.erp_cust_az12 (
        cid             NVARCHAR(50),
        bdate           DATE,
        gen             NVARCHAR(50),
        dwh_create_date DATETIME2 DEFAULT GETDATE()
    );

    IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
        DROP TABLE silver.erp_px_cat_g1v2;
    CREATE TABLE silver.erp_px_cat_g1v2 (
        id              NVARCHAR(50),
        cat             NVARCHAR(50),
        subcat          NVARCHAR(50),
        maintenance     NVARCHAR(50),
        dwh_create_date DATETIME2 DEFAULT GETDATE()
    );

    IF OBJECT_ID('silver.api_cust_info', 'U') IS NOT NULL
        DROP TABLE silver.api_cust_info;
    CREATE TABLE silver.api_cust_info (
        cst_id           NVARCHAR(50),
        cst_key          NVARCHAR(50),
        cst_phone_number NVARCHAR(50)
    );

    IF OBJECT_ID('silver.api_prd_info', 'U') IS NOT NULL
        DROP TABLE silver.api_prd_info;
    CREATE TABLE silver.api_prd_info (
        prd_id          NVARCHAR(50),
        prd_cat         NVARCHAR(50),
        prd_brand       NVARCHAR(50),
        prd_manufacturer NVARCHAR(50)
    );
    """

    print("Creating Silver Tables...")
    run(silver_sql)
    print("Silver Tables Created.\n")

    print("=======================================")
    print(" All Bronze + Silver Tables Created Successfully")
    print("=======================================")

    cursor.close()
    conn.close()


# Allows running this file directly
if __name__ == "__main__":
    main()  