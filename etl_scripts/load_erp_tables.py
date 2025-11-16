from config import get_connection

def main():
    conn = get_connection()
    cursor = conn.cursor()

    print("=======================================")
    print(" Loading Bronze ERP Tables")
    print("=======================================")

    def run(sql):
        cursor.execute(sql)
        conn.commit()

    # Base folder path
    # C:\Users\irfan\Desktop\Ali's Desktop\DWH Project\sql-data-warehouse-project\datasets\source_erp\
    # Apostrophe escaped: Ali''s Desktop

    # -------------------------------
    # Load ERP Location Table
    # -------------------------------
    print("Truncating: bronze.erp_loc_a101")
    run("TRUNCATE TABLE bronze.erp_loc_a101;")

    print("Loading: bronze.erp_loc_a101")
    run(r"""
    BULK INSERT bronze.erp_loc_a101
    FROM 'C:\Users\irfan\Desktop\Ali''s Desktop\DWH Project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );
    """)

    # -------------------------------
    # Load ERP Customer Table
    # -------------------------------
    print("Truncating: bronze.erp_cust_az12")
    run("TRUNCATE TABLE bronze.erp_cust_az12;")

    print("Loading: bronze.erp_cust_az12")
    run(r"""
    BULK INSERT bronze.erp_cust_az12
    FROM 'C:\Users\irfan\Desktop\Ali''s Desktop\DWH Project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );
    """)

    # -------------------------------
    # Load ERP Product Category Table
    # -------------------------------
    print("Truncating: bronze.erp_px_cat_g1v2")
    run("TRUNCATE TABLE bronze.erp_px_cat_g1v2;")

    print("Loading: bronze.erp_px_cat_g1v2")
    run(r"""
    BULK INSERT bronze.erp_px_cat_g1v2
    FROM 'C:\Users\irfan\Desktop\Ali''s Desktop\DWH Project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );
    """)

    print("=======================================")
    print(" ERP Bronze Load Completed")
    print("=======================================")

    cursor.close()
    conn.close()

# Allows running this file directly
if __name__ == "__main__":
    main()

