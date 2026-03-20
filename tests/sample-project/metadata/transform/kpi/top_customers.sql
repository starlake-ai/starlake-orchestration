SELECT
    customer_id,
    customer_name,
    email,
    total_orders,
    total_items,
    total_spent,
    first_order_date,
    last_order_date
FROM
    kpi.order_summary
WHERE
    total_spent > 0
ORDER BY
    total_spent DESC
LIMIT 5
