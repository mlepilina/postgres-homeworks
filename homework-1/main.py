"""Скрипт для заполнения данными таблиц в БД Postgres."""

import csv
import psycopg2

conn = psycopg2.connect(
    host="localhost",
    database="north",
    user="postgres",
    password="12345"
)


# create cursor
cur = conn.cursor()


# writing data from files to tables
with open(f'../homework-1/north_data/employees_data.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for dicty in reader:

        # execute query
        cur.execute(
            "INSERT INTO employees VALUES(%s, %s, %s, %s, %s, %s)",
            (
                dicty['employee_id'],
                dicty['first_name'],
                dicty['last_name'],
                dicty['title'],
                dicty['birth_date'],
                dicty['notes']
            ),
        )


with open(f'../homework-1/north_data/customers_data.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for dicty in reader:

        # execute query
        cur.execute(
            "INSERT INTO customers VALUES(%s, %s, %s)",
            (
                dicty['customer_id'],
                dicty['company_name'],
                dicty['contact_name']
            ),
        )


with open(f'../homework-1/north_data/orders_data.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for dicty in reader:

        # execute query
        cur.execute(
            "INSERT INTO orders VALUES(%s, %s, %s, %s, %s)",
            (
                dicty['order_id'],
                dicty['customer_id'],
                dicty['employee_id'],
                dicty['order_date'],
                dicty['ship_city']
            ),
        )


# execute query
cur.execute("SELECT * FROM employees")
# printing data
rows = cur.fetchall()
for row in rows:
    print(row)

# execute query
cur.execute("SELECT * FROM customers")
# printing data
rows = cur.fetchall()
for row in rows:
    print(row)

# execute query
cur.execute("SELECT * FROM orders")
# printing data
rows = cur.fetchall()
for row in rows:
    print(row)


# commit to database
conn.commit()


# close cursor and connection
cur.close()
conn.close()


