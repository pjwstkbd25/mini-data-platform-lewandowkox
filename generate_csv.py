import pandas as pd
import random
from faker import Faker

fake = Faker()

# Generate Customers CSV
def generate_customers(file_path, num_customers=100):
    customers = pd.DataFrame({
        "customer_id": range(1, num_customers + 1),
        "name": [fake.name() for _ in range(num_customers)],
        "email": [fake.unique.email() for _ in range(num_customers)],
        "created_at": [fake.date_time_this_decade() for _ in range(num_customers)]
    })
    customers.to_csv(file_path, index=False)
    print(f"Generated {file_path}")

# Generate Products CSV
def generate_products(file_path, num_products=50):
    categories = ["Electronics", "Clothing", "Home & Kitchen", "Books", "Toys"]
    products = pd.DataFrame({
        "product_id": range(1, num_products + 1),
        "name": [fake.word().capitalize() + " Product" for _ in range(num_products)],
        "category": [random.choice(categories) for _ in range(num_products)],
        "price": [round(random.uniform(10, 1000), 2) for _ in range(num_products)],
        "stock_quantity": [random.randint(5, 100) for _ in range(num_products)],
        "created_at": [fake.date_time_this_decade() for _ in range(num_products)]
    })
    products.to_csv(file_path, index=False)
    print(f"Generated {file_path}")

# Generate Orders CSV
def generate_orders(file_path, num_orders=200, num_customers=100, num_products=50):
    orders = pd.DataFrame({
        "order_id": range(1, num_orders + 1),
        "customer_id": [random.randint(1, num_customers) for _ in range(num_orders)],
        "product_id": [random.randint(1, num_products) for _ in range(num_orders)],
        "quantity": [random.randint(1, 5) for _ in range(num_orders)],
        "total_price": [round(random.uniform(20, 5000), 2) for _ in range(num_orders)],
        "order_date": [fake.date_time_this_decade() for _ in range(num_orders)]
    })
    orders.to_csv(file_path, index=False)
    print(f"Generated {file_path}")

# Main execution
def main():
    generate_customers("customers.csv")
    generate_products("products.csv")
    generate_orders("orders.csv")
    print("All CSV files generated successfully.")

if __name__ == "__main__":
    main()