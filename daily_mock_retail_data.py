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

### Fetch Existing Customers ###
cursor.execute("SELECT email FROM customers")
existing_emails = set(row[0] for row in cursor.fetchall())

### Insert 100 New Customers (Avoid Duplicates) ###
print("Adding up to 100 new customers...")

new_customers = 0
for _ in range(100):
    email = fake.unique.email()
    
    if email not in existing_emails:
        name = fake.name()
        phone = fake.phone_number()[:20]
        address = fake.address()
        
        cursor.execute('INSERT INTO customers(name, email, phone, address) VALUES(%s, %s, %s, %s)',
                       (name, email, phone, address))
        existing_emails.add(email)
        new_customers += 1

conn.commit()
print(f"{new_customers} new customers added.")

### Fetch Updated Customer & Product Data ###
cursor.execute("SELECT customer_id FROM customers")
customer_ids = [row[0] for row in cursor.fetchall()]

cursor.execute("SELECT product_id, price FROM products")
products = cursor.fetchall()
product_price_map = {p[0]: p[1] for p in products}

if not customer_ids or not product_price_map:
    print("No customers or products found. Exiting!")
    exit()

### Insert 1000 New Orders & Transactions ###
print("Adding 1000 new orders and transactions...")

payment_methods = ['Credit Card', 'Debit Card', 'PayPal', 'UPI', 'Cash']
transaction_statuses = ['success', 'failed', 'pending']

for _ in range(1000):
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
print("1000 new orders and transactions added.")

# Close connection
cursor.close()
conn.close()

print("Daily mock data generation completed!")
