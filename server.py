from flask import Flask, jsonify
import pandas as pd
import json

app = Flask(__name__)

# --- Path to your customers CSV ---
CUSTOMER_CSV_PATH = r"datasets\customers.csv"
PRODUCT_CSV_PATH = r"datasets\product.csv"

# --- Convert CSV → JSON (simulate internal API DB) ---
customer_df = pd.read_csv(CUSTOMER_CSV_PATH, dtype={"id": str})
product_df = pd.read_csv(PRODUCT_CSV_PATH, dtype={"id": str})

customer_df.to_json("customers.json", orient="records")
product_df.to_json("product.json", orient="records")

# Creating end-point urls 

# for customers 
@app.route('/api/customers', methods=['GET'])
def get_customers():
    """Simulated API endpoint returning customer data."""
    with open("customers.json", "r") as f:
        data = json.load(f)
    return jsonify(data)

# for products
@app.route('/api/products', methods=['GET'])
def get_products():
    """Simulated API endpoint returning product data."""
    with open("product.json", "r") as f:
        data = json.load(f)
    return jsonify(data)

if __name__ == '__main__':
    print("APIs running:")
    app.run(port=5000, debug=True)
