import pandas as pd
from sqlalchemy import create_engine
import os

# Database Configuration
DB_NAME = "mydatabase"
DB_USER = "myuser"
DB_PASSWORD = "mypassword"
DB_HOST = "localhost"
DB_PORT = "5433"

# CSV File Paths
CSV_FILES = ["customers.csv", "products.csv", "orders.csv"]

# Create a connection to PostgreSQL
def get_engine():
    return create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

def create_table_and_insert_data(engine, file_path, table_name):
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Table {table_name} created and data inserted from {file_path}.")
    else:
        print(f"Warning: {file_path} not found.")

def main():
    engine = get_engine()
    
    for csv_file in CSV_FILES:
        table_name = csv_file.split('.')[0]  # Extract table name from filename
        create_table_and_insert_data(engine, csv_file, table_name)
    
    print("All CSV files processed successfully.")

if __name__ == "__main__":
    main()
