import requests
from config import get_connection


def main(): 
    # 1) ---------------------- EXTRACT ----------------------
    CUSTOMER_API_URL = "http://127.0.0.1:5000/api/customers"
    PRODUCT_API_URL = "http://127.0.0.1:5000/api/products"

    print("📥 Extracting data from API...")
    response = requests.get(CUSTOMER_API_URL)
    customers = response.json()  # list of dicts

    response = requests.get(PRODUCT_API_URL)
    products = response.json()  # list of dicts


    # 2) ---------------------- getting the data ----------------------
    customers_data_to_insert = []
    for row in customers:
        customers_data_to_insert.append((
            str(row.get("id")),       # id
            str(row.get("key")),      # cst_key
            str(row.get("phone_number")),    # phone_number
        ))

    print(f"🔄 Transformation complete. Rows prepared: {len(customers_data_to_insert)}")

    product_data_to_insert = []
    for row in products:
        product_data_to_insert.append((
            str(row.get("id")),       # id
            str(row.get("cat")),      # cat
            str(row.get("brand")),    # brand
            str(row.get("manufacturer")),    # manufacturer
        ))

    print(f"🔄 Transformation complete. Rows prepared: {len(product_data_to_insert)}")


    # 3) ---------------------- LOAD ----------------------
    print("🗄 Connecting to SQL Server...")
    conn = get_connection()
    cursor = conn.cursor()
    cursor.fast_executemany = True

    # loading the customer data 
    print("📤 Loading data into SQL Server...")

    customer_insert_query = """
    INSERT INTO bronze.api_cust_info (id, cst_key, phone_number)
    VALUES (?, ?, ?)
    """
    cursor.execute("TRUNCATE TABLE bronze.api_cust_info"); 
    cursor.executemany(customer_insert_query, customers_data_to_insert)

    # loading product data 
    product_insert_query = """
    INSERT INTO bronze.api_prod_info (id, cat, brand, manufacturer)
    VALUES (?, ?, ?, ?)
    """
    cursor.execute("TRUNCATE TABLE bronze.api_prod_info"); 
    cursor.executemany(product_insert_query, product_data_to_insert)

    conn.commit()

    print("✅ ETL Successful! Data inserted into bronze.api_cust_info")

if __name__ == "__main__":
    main()