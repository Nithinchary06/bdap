-- EXPERIMENT 5 — HIVE DATABASE, TABLES AND SALES QUERIES
-- Aim: Create the database/tables and run six sales-analysis queries.
-- Execute this file in Hive CLI or Beeline, not at the normal Linux prompt.

CREATE DATABASE IF NOT EXISTS labdb;
SHOW DATABASES;
USE labdb;
SELECT current_database();

-- DROP DATABASE labdb;          -- uncomment only if empty and deletion is intended
-- DROP DATABASE labdb CASCADE;  -- DESTRUCTIVE: removes tables too

-- Create the sales order table.
CREATE TABLE IF NOT EXISTS sales_order (
    order_id INT, customer_id INT, product_id INT,
    order_date TIMESTAMP, order_amount DOUBLE,
    quantity INT, discount DOUBLE, tax DOUBLE, total_amount DOUBLE
);

-- Create the product table.
CREATE TABLE IF NOT EXISTS product (
    product_id INT, product_name STRING, category STRING,
    price DOUBLE, manufacturer STRING, date_added TIMESTAMP
);

-- Create customer table with a complex address field and partition columns.
CREATE TABLE IF NOT EXISTS customer (
    customer_id INT, customer_name STRING, email STRING, phone STRING,
    address STRUCT<street:STRING,city:STRING,state:STRING,zip:INT>,
    date_joined TIMESTAMP
)
PARTITIONED BY (country STRING, state STRING);

-- Load/insert your lab dataset before running the analysis queries.
-- 1. Total sales.
SELECT SUM(total_amount) AS total_sales FROM sales_order;

-- 2. Top 5 products by sales.
SELECT p.product_name, SUM(s.total_amount) AS sales
FROM sales_order s JOIN product p ON s.product_id = p.product_id
GROUP BY p.product_name ORDER BY sales DESC LIMIT 5;

-- 3. Customer-wise sales.
SELECT customer_id, SUM(total_amount) AS sales
FROM sales_order GROUP BY customer_id;

-- 4. Monthly sales trend.
SELECT YEAR(order_date) AS yr, MONTH(order_date) AS mon,
       SUM(total_amount) AS sales
FROM sales_order
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY yr, mon;

-- 5. Product-wise quantity sold.
SELECT p.product_name, SUM(s.quantity) AS qty_sold
FROM sales_order s JOIN product p ON s.product_id = p.product_id
GROUP BY p.product_name;

-- 6. Highest-priced product in each category.
SELECT category, product_name, price
FROM (
    SELECT category, product_name, price,
           ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC) AS rn
    FROM product
) ranked
WHERE rn = 1;

-- Verify loaded data.
SELECT * FROM product;
SELECT * FROM sales_order;
