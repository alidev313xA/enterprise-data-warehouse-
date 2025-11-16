from config import get_connection

def main():
    conn = get_connection()
    cursor = conn.cursor()

    print("=======================================")
    print(" Loading Bronze CRM Tables")
    print("=======================================")

    def run(sql):
        cursor.execute(sql)
        conn.commit()

    # -------------------------------
    # Load CRM Customer Info
    # -------------------------------
    print("Truncating: bronze.crm_cust_info")
    run("TRUNCATE TABLE bronze.crm_cust_info;")

    print("Loading: bronze.crm_cust_info")
    run(r"""
    BULK INSERT bronze.crm_cust_info
    FROM 'C:\Users\irfan\Desktop\Ali''s Desktop\DWH Project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );
    """)

    # -------------------------------
    # Load CRM Product Info
    # -------------------------------
    print("Truncating: bronze.crm_prd_info")
    run("TRUNCATE TABLE bronze.crm_prd_info;")

    print("Loading: bronze.crm_prd_info")
    run(r"""
    BULK INSERT bronze.crm_prd_info
    FROM 'C:\Users\irfan\Desktop\Ali''s Desktop\DWH Project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );
    """)

    # -------------------------------
    # Load CRM Sales Details
    # -------------------------------
    print("Truncating: bronze.crm_sales_details")
    run("TRUNCATE TABLE bronze.crm_sales_details;")

    print("Loading: bronze.crm_sales_details")
    run(r"""
    BULK INSERT bronze.crm_sales_details
    FROM 'C:\Users\irfan\Desktop\Ali''s Desktop\DWH Project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );
    """)

    print("=======================================")
    print(" CRM Bronze Load Completed")
    print("=======================================")

    cursor.close()
    conn.close()

# Allows running this file directly
if __name__ == "__main__":
    main()