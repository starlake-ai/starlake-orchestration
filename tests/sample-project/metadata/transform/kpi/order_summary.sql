SELECT
    o.customer_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    c.email,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(o.quantity) AS total_items,
    SUM(o.quantity * p.price) AS total_spent,
    MIN(o.order_date) AS first_order_date,
    MAX(o.order_date) AS last_order_date
FROM
    starbake.orders o
    JOIN starbake.customers c ON o.customer_id = c.id
    JOIN starbake.products p ON o.product_id = p.product_id
GROUP BY
    o.customer_id,
    c.first_name,
    c.last_name,
    c.email
ORDER BY
    total_spent DESC
