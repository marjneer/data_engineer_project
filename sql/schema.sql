CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(100),
    mobile_number VARCHAR(20) UNIQUE,
    region VARCHAR(50)
);

CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    mobile_number VARCHAR(20),
    order_date_time DATETIME,
    sku_id VARCHAR(50),
    sku_count INT,
    total_amount DECIMAL(10,2),
    FOREIGN KEY (mobile_number) REFERENCES customers(mobile_number)
);

CREATE INDEX idx_orders_date ON orders(order_date_time);
CREATE INDEX idx_orders_mobile ON orders(mobile_number);
