# config.py
import pyodbc

# Centralized configuration
DATABASE_NAME = "DataWarehouse"        # Your Data Warehouse name from SQL Server
SERVER_NAME   = r"HUZAIFA\SQLEXPRESS"  # can also be moved to env variable

CONNECTION_STRING = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={SERVER_NAME};"
    f"DATABASE={DATABASE_NAME};"
    f"Trusted_Connection=yes;"
)

def get_connection():
    """Returns a SQL Server connection using centralized config."""
    return pyodbc.connect(CONNECTION_STRING)

