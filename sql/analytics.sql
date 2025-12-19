-- ========================================
-- SAMPLE ANALYTICS QUERIES
-- Gold Layer in PostgreSQL
-- ========================================

-- Query 1: Top 5 Days by Revenue
SELECT 
    transaction_date,
    total_transactions,
    ROUND(total_amount::numeric, 2) as total_revenue,
    ROUND(avg_amount::numeric, 2) as avg_transaction_amount
FROM gold.daily_summary
ORDER BY total_amount DESC
LIMIT 5;

-- Query 2: City Performance Ranking
SELECT 
    city,
    total_transactions,
    ROUND(total_amount::numeric, 2) as total_revenue,
    ROUND(avg_amount::numeric, 2) as avg_transaction,
    unique_accounts
FROM gold.city_summary
ORDER BY total_amount DESC;

-- Query 3: Transaction Type Distribution
SELECT 
    transaction_type,
    total_transactions,
    ROUND(total_amount::numeric, 2) as total_amount,
    ROUND(avg_amount::numeric, 2) as avg_amount,
    ROUND((total_transactions::numeric / SUM(total_transactions) OVER ()) * 100, 2) as pct_of_total
FROM gold.transaction_type_summary
ORDER BY total_amount DESC;

-- Query 4: Peak Hours Analysis
SELECT 
    hour,
    total_transactions,
    ROUND(total_amount::numeric, 2) as total_amount,
    CASE 
        WHEN hour BETWEEN 9 AND 17 THEN 'Business Hours'
        WHEN hour BETWEEN 18 AND 22 THEN 'Evening'
        ELSE 'Off-Peak'
    END as time_category
FROM gold.hourly_pattern
ORDER BY total_transactions DESC;

-- Query 5: Business Insights - High Value Cities
SELECT 
    city,
    total_transactions,
    ROUND(avg_amount::numeric, 2) as avg_transaction,
    CASE 
        WHEN avg_amount > 5000 THEN 'High Value'
        WHEN avg_amount > 2000 THEN 'Medium Value'
        ELSE 'Standard'
    END as customer_segment
FROM gold.city_summary
WHERE city != 'Unknown'
ORDER BY avg_amount DESC;
