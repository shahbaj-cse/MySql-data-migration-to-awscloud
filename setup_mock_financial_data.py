from faker import Faker
import pymysql
import random

# Initialize Faker
fake = Faker()

# Connect to MySQL
conn = pymysql.connect(
    host="localhost",
    user="root",
    password="Sbssu@1010",
    database="retail_finance"
)
cursor = conn.cursor()

### Insert 1000 Customers
print("Inserting initial 1000 customers...")

for _ in range(1000):
    name = fake.name()
    email = fake.unique.email()  # Emails will be unique by default
    phone = fake.phone_number()[:20]
    address = fake.address()
    
    cursor.execute('INSERT INTO customers(name, email, phone, address) VALUES(%s, %s, %s, %s)',
                   (name, email, phone, address))

conn.commit()
print("1000 customers inserted.")

### Insert 100 Products ###
print("Inserting 100 products...")

product_categories = ['Electronics', 'Clothing', 'Books', 'Groceries', 'Furniture']
for _ in range(100):
    name = fake.word().capitalize()
    category = random.choice(product_categories)
    price = round(random.uniform(10, 1000), 2)
    stock_quantity = random.randint(5, 500)

    cursor.execute('INSERT INTO products(name, category, price, stock_quantity) VALUES(%s, %s, %s, %s)',
                   (name, category, price, stock_quantity))

conn.commit()
print("100 products inserted.")

### Fetch Data for Orders & Transactions ###
cursor.execute("SELECT customer_id FROM customers")
customer_ids = [row[0] for row in cursor.fetchall()]

cursor.execute("SELECT product_id, price FROM products")
products = cursor.fetchall()
product_price_map = {p[0]: p[1] for p in products}

if not customer_ids or not product_price_map:
    print("No customers or products found. Aborting!")
    exit()

### Insert 5000 Initial Orders and Transactions ###
print("Inserting 5000 initial orders and transactions...")

payment_methods = ['Credit Card', 'Debit Card', 'PayPal', 'UPI', 'Cash']
transaction_statuses = ['success', 'failed', 'pending']

for _ in range(5000):
    customer_id = random.choice(customer_ids)
    product_id = random.choice(list(product_price_map.keys()))
    unit_price = product_price_map[product_id]
    quantity = random.randint(1, 10)
    total_price = round(unit_price * quantity, 2)

    # Insert order
    cursor.execute('INSERT INTO orders(customer_id, product_id, quantity, total_price) VALUES (%s, %s, %s, %s)',
                   (customer_id, product_id, quantity, total_price))

    # Insert transaction
    payment_method = random.choice(payment_methods)
    status = random.choice(transaction_statuses)
    
    cursor.execute('INSERT INTO transactions(customer_id, amount, payment_method, status) VALUES (%s, %s, %s, %s)',
                   (customer_id, total_price, payment_method, status))

conn.commit()
print("5000 initial orders and transactions inserted.")

# Close connection
cursor.close()
conn.close()

print("First-day initialization completed successfully!")
