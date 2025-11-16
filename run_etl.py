from etl_scripts import ddl_script as a
from etl_scripts import load_api_tables as b 
from etl_scripts import load_crm_tables as c
from etl_scripts import load_erp_tables as d

from etl_scripts import load_silver_layer as e
from etl_scripts import gold_ddl as f

print("Creating Tables in Bronze Layer...")
a.main() 

print("Running ETL for API Tables...")
b.main() 

print("Running ETL for CRM Tables...")
c.main()

print("Running ETL for ERP Tables...")
d.main()

print("Tranforming the data in the bronze layer and Loading Silver Layer...")
e.main()

print("Creating the Gold Layer...") 
f.main()

print("ETL Process Completed Successfully!")