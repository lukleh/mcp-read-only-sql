-- Insert sample data for PostgreSQL testing

-- Insert users
INSERT INTO users (username, email, full_name, is_active) VALUES
    ('john_doe', 'john@example.com', 'John Doe', true),
    ('jane_smith', 'jane@example.com', 'Jane Smith', true),
    ('bob_wilson', 'bob@example.com', 'Bob Wilson', true),
    ('alice_brown', 'alice@example.com', 'Alice Brown', true),
    ('charlie_davis', 'charlie@example.com', 'Charlie Davis', false),
    ('emma_jones', 'emma@example.com', 'Emma Jones', true),
    ('frank_miller', 'frank@example.com', 'Frank Miller', true),
    ('grace_taylor', 'grace@example.com', 'Grace Taylor', true),
    ('henry_anderson', 'henry@example.com', 'Henry Anderson', true),
    ('iris_martinez', 'iris@example.com', 'Iris Martinez', true);

-- Insert products
INSERT INTO products (name, description, price, stock_quantity, category) VALUES
    ('Laptop Pro 15', 'High-performance laptop with 15-inch display', 1299.99, 50, 'Electronics'),
    ('Wireless Mouse', 'Ergonomic wireless mouse with long battery life', 29.99, 200, 'Electronics'),
    ('USB-C Hub', '7-in-1 USB-C hub with multiple ports', 49.99, 150, 'Electronics'),
    ('Office Chair', 'Ergonomic office chair with lumbar support', 399.99, 30, 'Furniture'),
    ('Standing Desk', 'Height-adjustable standing desk', 599.99, 20, 'Furniture'),
    ('Monitor 27"', '4K UHD monitor with HDR support', 449.99, 75, 'Electronics'),
    ('Mechanical Keyboard', 'RGB mechanical keyboard with blue switches', 89.99, 100, 'Electronics'),
    ('Webcam HD', '1080p HD webcam with noise-canceling mic', 79.99, 120, 'Electronics'),
    ('Desk Lamp', 'LED desk lamp with adjustable brightness', 34.99, 80, 'Furniture'),
    ('Cable Management Kit', 'Complete cable management solution', 19.99, 250, 'Accessories'),
    ('Notebook Set', 'Set of 5 premium notebooks', 24.99, 300, 'Stationery'),
    ('Pen Set', 'Professional pen set with case', 39.99, 150, 'Stationery'),
    ('Coffee Maker', 'Programmable coffee maker with thermal carafe', 129.99, 40, 'Appliances'),
    ('Water Bottle', 'Insulated stainless steel water bottle', 19.99, 200, 'Accessories'),
    ('Backpack', 'Laptop backpack with multiple compartments', 79.99, 100, 'Accessories');

-- Insert orders
INSERT INTO orders (user_id, total_amount, status, shipping_address) VALUES
    (1, 1379.97, 'delivered', '123 Main St, New York, NY 10001'),
    (2, 129.97, 'shipped', '456 Oak Ave, Los Angeles, CA 90001'),
    (3, 649.98, 'processing', '789 Pine Rd, Chicago, IL 60601'),
    (1, 89.99, 'delivered', '123 Main St, New York, NY 10001'),
    (4, 1929.95, 'delivered', '321 Elm St, Houston, TX 77001'),
    (5, 54.98, 'cancelled', '654 Maple Dr, Phoenix, AZ 85001'),
    (6, 484.97, 'delivered', '987 Cedar Ln, Philadelphia, PA 19101'),
    (7, 1349.98, 'processing', '246 Birch Blvd, San Antonio, TX 78201'),
    (8, 199.97, 'shipped', '135 Spruce Way, San Diego, CA 92101'),
    (9, 34.99, 'delivered', '864 Willow Ct, Dallas, TX 75201'),
    (10, 159.96, 'processing', '579 Aspen Pl, San Jose, CA 95101'),
    (2, 599.99, 'delivered', '456 Oak Ave, Los Angeles, CA 90001'),
    (3, 449.99, 'shipped', '789 Pine Rd, Chicago, IL 60601'),
    (4, 79.98, 'delivered', '321 Elm St, Houston, TX 77001'),
    (5, 1299.99, 'pending', '654 Maple Dr, Phoenix, AZ 85001');

-- Insert order items
INSERT INTO order_items (order_id, product_id, quantity, unit_price, subtotal) VALUES
    (1, 1, 1, 1299.99, 1299.99),
    (1, 2, 1, 29.99, 29.99),
    (1, 3, 1, 49.99, 49.99),
    (2, 2, 1, 29.99, 29.99),
    (2, 3, 2, 49.99, 99.98),
    (3, 4, 1, 399.99, 399.99),
    (3, 14, 1, 249.99, 249.99),
    (4, 7, 1, 89.99, 89.99),
    (5, 1, 1, 1299.99, 1299.99),
    (5, 5, 1, 599.99, 599.99),
    (5, 2, 1, 29.99, 29.99),
    (6, 10, 1, 19.99, 19.99),
    (6, 9, 1, 34.99, 34.99),
    (7, 4, 1, 399.99, 399.99),
    (7, 9, 1, 34.99, 34.99),
    (7, 3, 1, 49.99, 49.99),
    (8, 1, 1, 1299.99, 1299.99),
    (8, 3, 1, 49.99, 49.99),
    (9, 11, 2, 24.99, 49.98),
    (9, 13, 1, 129.99, 129.99),
    (9, 14, 1, 19.99, 19.99),
    (10, 9, 1, 34.99, 34.99),
    (11, 12, 2, 39.99, 79.98),
    (11, 15, 1, 79.99, 79.99),
    (12, 5, 1, 599.99, 599.99),
    (13, 6, 1, 449.99, 449.99),
    (14, 12, 2, 39.99, 79.98),
    (15, 1, 1, 1299.99, 1299.99);

-- Create some views for easier querying
CREATE VIEW user_order_summary AS
SELECT
    u.username,
    u.email,
    COUNT(o.id) as total_orders,
    SUM(o.total_amount) as total_spent,
    MAX(o.order_date) as last_order_date
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
GROUP BY u.id, u.username, u.email;

CREATE VIEW product_sales_summary AS
SELECT
    p.name as product_name,
    p.category,
    COUNT(DISTINCT oi.order_id) as times_ordered,
    SUM(oi.quantity) as total_quantity_sold,
    SUM(oi.subtotal) as total_revenue
FROM products p
LEFT JOIN order_items oi ON p.id = oi.product_id
GROUP BY p.id, p.name, p.category;