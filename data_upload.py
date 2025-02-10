import pandas as pd
from sqlalchemy import create_engine, text

DATABASE_URL = "mysql+pymysql://root:toor@127.0.0.1/shop" 

engine = create_engine(DATABASE_URL)

def create_table_and_insert_data(engine, table_name, sheet_name, excel_file):
    try:
        df = pd.read_excel(excel_file, sheet_name=sheet_name)

        columns = ', '.join([f"{col} VARCHAR(255)" for col in df.columns])  
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns});"

        with engine.connect() as connection:
            connection.execute(text(create_table_query))
            print(f"Table `{table_name}` created successfully (if it didn't exist).")

            placeholders = ', '.join(['%s'] * len(df.columns)) 
            insert_query = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({placeholders})"

            data_to_insert = [tuple(row) for row in df.values]

            connection.connection.cursor().executemany(insert_query, data_to_insert)
            print(f"Data from sheet `{sheet_name}` inserted into `{table_name}` successfully.")

    except Exception as e:
        print(f"Error: {e}")

excel_file = "GT_dGTL_data warehouse_07-01-2025.xlsx"  

table_name = "order_details"
sheet_name = "Order details"

try:
    with engine.connect() as connection:
        result = connection.execute(text("SELECT DATABASE();"))
        print("Connected to:", result.fetchone()[0])
        
    create_table_and_insert_data(engine, table_name, sheet_name, excel_file)

except Exception as e:
    print(f"Error connecting to MySQL: {e}")
