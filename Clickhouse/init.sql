CREATE DATABASE IF NOT EXISTS bi_analytics;

CREATE TABLE bi_analytics.orders
(
    order_id UInt64,
    customer_id UInt64,
    product_id UInt64,
    quantity UInt32,
    price Float64,
    status String,
    event_time DateTime,
    event_date Date
)
ENGINE = MergeTree
ORDER BY (event_date, order_id);

CREATE TABLE bi_analytics.daily_sales
(
    event_date Date,

    orders_count AggregateFunction(count, UInt8),

    customers_count AggregateFunction(uniq, UInt64),

    items_sold AggregateFunction(sum, UInt32),

    revenue AggregateFunction(sum, Float64)
)
ENGINE = AggregatingMergeTree
ORDER BY event_date;

CREATE MATERIALIZED VIEW bi_analytics.mv_daily_sales
TO bi_analytics.daily_sales
AS
SELECT
    event_date,

    countState() AS orders_count,

    uniqState(customer_id) AS customers_count,

    sumState(quantity) AS items_sold,

    sumState(quantity * price) AS revenue

FROM bi_analytics.orders
GROUP BY event_date;