-- ============================================================================
-- USEFUL QUERIES FOR DEVELOPMENT AND TESTING
-- Database: etl_stocks
-- ============================================================================

-- ============================================================================
-- SECTION 1: DATA VERIFICATION QUERIES
-- ============================================================================

-- 1.1 Count records in all tables
SELECT 
    'stocks' as table_name, COUNT(*) as record_count FROM stocks
UNION ALL
SELECT 
    'daily_prices', COUNT(*) FROM daily_prices
UNION ALL
SELECT 
    'technical_indicators', COUNT(*) FROM technical_indicators
UNION ALL
SELECT 
    'market_overview', COUNT(*) FROM market_overview
UNION ALL
SELECT 
    'etl_execution_log', COUNT(*) FROM etl_execution_log
UNION ALL
SELECT 
    'data_quality_log', COUNT(*) FROM data_quality_log;

-- 1.2 Latest 10 prices for each stock
SELECT 
    s.symbol,
    s.company_name,
    dp.price_date,
    dp.open_price,
    dp.close_price,
    dp.volume,
    dp.daily_return
FROM stocks s
INNER JOIN daily_prices dp ON s.stock_id = dp.stock_id
WHERE s.symbol IN ('AAPL', 'MSFT', 'GOOGL')
ORDER BY s.symbol, dp.price_date DESC
LIMIT 30;

-- 1.3 Check for missing dates in price data
SELECT 
    s.symbol,
    MIN(dp.price_date) as first_date,
    MAX(dp.price_date) as last_date,
    COUNT(DISTINCT dp.price_date) as trading_days,
    MAX(dp.price_date) - MIN(dp.price_date) as date_range_days
FROM stocks s
LEFT JOIN daily_prices dp ON s.stock_id = dp.stock_id
GROUP BY s.stock_id, s.symbol
ORDER BY s.symbol;

-- 1.4 Find duplicate price records
SELECT 
    stock_id,
    price_date,
    COUNT(*) as duplicate_count
FROM daily_prices
GROUP BY stock_id, price_date
HAVING COUNT(*) > 1;

-- ============================================================================
-- SECTION 2: DATA QUALITY QUERIES
-- ============================================================================

-- 2.1 Check for NULL values in critical fields
SELECT 
    'daily_prices - open_price' as check_name,
    COUNT(*) as null_count
FROM daily_prices
WHERE open_price IS NULL
UNION ALL
SELECT 
    'daily_prices - close_price',
    COUNT(*)
FROM daily_prices
WHERE close_price IS NULL
UNION ALL
SELECT 
    'daily_prices - volume',
    COUNT(*)
FROM daily_prices
WHERE volume IS NULL;

-- 2.2 Check for negative or zero prices (should return 0)
SELECT 
    s.symbol,
    dp.price_date,
    dp.open_price,
    dp.high_price,
    dp.low_price,
    dp.close_price
FROM daily_prices dp
INNER JOIN stocks s ON dp.stock_id = s.stock_id
WHERE dp.open_price <= 0 
   OR dp.high_price <= 0 
   OR dp.low_price <= 0 
   OR dp.close_price <= 0;

-- 2.3 Check for invalid high/low relationships (should return 0)
SELECT 
    s.symbol,
    dp.price_date,
    dp.high_price,
    dp.low_price,
    dp.open_price,
    dp.close_price
FROM daily_prices dp
INNER JOIN stocks s ON dp.stock_id = s.stock_id
WHERE dp.high_price < dp.low_price
   OR dp.high_price < dp.open_price
   OR dp.high_price < dp.close_price
   OR dp.low_price > dp.open_price
   OR dp.low_price > dp.close_price;

-- 2.4 Data quality summary report
SELECT 
    'Total Stocks' as metric,
    COUNT(*)::TEXT as value
FROM stocks
UNION ALL
SELECT 
    'Active Stocks',
    COUNT(*)::TEXT
FROM stocks
WHERE is_active = TRUE
UNION ALL
SELECT 
    'Total Price Records',
    COUNT(*)::TEXT
FROM daily_prices
UNION ALL
SELECT 
    'Avg Records per Stock',
    ROUND(AVG(cnt), 2)::TEXT
FROM (
    SELECT COUNT(*) as cnt
    FROM daily_prices
    GROUP BY stock_id
) subq
UNION ALL
SELECT 
    'Price Records with NULLs',
    COUNT(*)::TEXT
FROM daily_prices
WHERE open_price IS NULL 
   OR close_price IS NULL 
   OR volume IS NULL;

-- ============================================================================
-- SECTION 3: ANALYTICAL QUERIES
-- ============================================================================

-- 3.1 Top 10 stocks by average daily volume (last 30 days)
SELECT 
    s.symbol,
    s.company_name,
    COUNT(dp.price_id) as trading_days,
    ROUND(AVG(dp.volume), 0) as avg_volume,
    ROUND(AVG(dp.close_price), 2) as avg_price
FROM stocks s
INNER JOIN daily_prices dp ON s.stock_id = dp.stock_id
WHERE dp.price_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY s.stock_id, s.symbol, s.company_name
ORDER BY avg_volume DESC
LIMIT 10;

-- 3.2 Calculate daily returns for last 5 days per stock
SELECT 
    s.symbol,
    dp.price_date,
    dp.close_price,
    LAG(dp.close_price) OVER (PARTITION BY s.stock_id ORDER BY dp.price_date) as prev_close,
    ROUND(
        ((dp.close_price - LAG(dp.close_price) OVER (PARTITION BY s.stock_id ORDER BY dp.price_date)) 
        / LAG(dp.close_price) OVER (PARTITION BY s.stock_id ORDER BY dp.price_date) * 100), 
        2
    ) as daily_return_pct
FROM stocks s
INNER JOIN daily_prices dp ON s.stock_id = dp.stock_id
WHERE s.symbol IN ('AAPL', 'MSFT', 'GOOGL')
  AND dp.price_date >= CURRENT_DATE - INTERVAL '5 days'
ORDER BY s.symbol, dp.price_date DESC;

-- 3.3 Moving averages comparison (10-day vs 50-day)
WITH price_data AS (
    SELECT 
        s.symbol,
        dp.price_date,
        dp.close_price,
        AVG(dp.close_price) OVER (
            PARTITION BY s.stock_id 
            ORDER BY dp.price_date 
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) as sma_10,
        AVG(dp.close_price) OVER (
            PARTITION BY s.stock_id 
            ORDER BY dp.price_date 
            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ) as sma_50
    FROM stocks s
    INNER JOIN daily_prices dp ON s.stock_id = dp.stock_id
    WHERE s.is_active = TRUE
)
SELECT 
    symbol,
    price_date,
    ROUND(close_price, 2) as close_price,
    ROUND(sma_10, 2) as sma_10,
    ROUND(sma_50, 2) as sma_50,
    CASE 
        WHEN sma_10 > sma_50 THEN 'Bullish'
        WHEN sma_10 < sma_50 THEN 'Bearish'
        ELSE 'Neutral'
    END as trend_signal
FROM price_data
WHERE price_date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY symbol, price_date DESC;

-- 3.4 Volatility analysis (standard deviation of returns)
SELECT 
    s.symbol,
    s.company_name,
    COUNT(dp.price_id) as trading_days,
    ROUND(AVG(dp.daily_return), 4) as avg_return,
    ROUND(STDDEV(dp.daily_return), 4) as volatility,
    ROUND(MIN(dp.daily_return), 4) as worst_day,
    ROUND(MAX(dp.daily_return), 4) as best_day
FROM stocks s
INNER JOIN daily_prices dp ON s.stock_id = dp.stock_id
WHERE dp.price_date >= CURRENT_DATE - INTERVAL '90 days'
  AND dp.daily_return IS NOT NULL
GROUP BY s.stock_id, s.symbol, s.company_name
ORDER BY volatility DESC;

-- 3.5 Year-to-date (YTD) performance
WITH first_price AS (
    SELECT 
        stock_id,
        close_price as ytd_open,
        price_date as ytd_start_date
    FROM daily_prices
    WHERE price_date = (
        SELECT MIN(price_date)
        FROM daily_prices dp2
        WHERE EXTRACT(YEAR FROM dp2.price_date) = EXTRACT(YEAR FROM CURRENT_DATE)
          AND dp2.stock_id = daily_prices.stock_id
    )
),
last_price AS (
    SELECT 
        stock_id,
        close_price as current_price,
        price_date as last_date
    FROM daily_prices
    WHERE price_date = (
        SELECT MAX(price_date)
        FROM daily_prices dp2
        WHERE dp2.stock_id = daily_prices.stock_id
    )
)
SELECT 
    s.symbol,
    s.company_name,
    fp.ytd_open,
    lp.current_price,
    ROUND(((lp.current_price - fp.ytd_open) / fp.ytd_open * 100), 2) as ytd_return_pct,
    fp.ytd_start_date,
    lp.last_date
FROM stocks s
INNER JOIN first_price fp ON s.stock_id = fp.stock_id
INNER JOIN last_price lp ON s.stock_id = lp.stock_id
ORDER BY ytd_return_pct DESC;

-- ============================================================================
-- SECTION 4: ETL MONITORING QUERIES
-- ============================================================================

-- 4.1 Latest ETL executions
SELECT 
    execution_id,
    job_name,
    transformation_name,
    start_time,
    end_time,
    duration_seconds,
    status,
    records_processed,
    records_inserted,
    records_updated,
    records_rejected
FROM etl_execution_log
ORDER BY start_time DESC
LIMIT 20;

-- 4.2 ETL success rate by job
SELECT 
    job_name,
    COUNT(*) as total_runs,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful_runs,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed_runs,
    ROUND(
        SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END)::NUMERIC / COUNT(*) * 100, 
        2
    ) as success_rate_pct,
    ROUND(AVG(duration_seconds), 2) as avg_duration_sec,
    MAX(start_time) as last_run
FROM etl_execution_log
GROUP BY job_name
ORDER BY last_run DESC;

-- 4.3 Failed ETL executions with error messages
SELECT 
    execution_id,
    job_name,
    transformation_name,
    start_time,
    duration_seconds,
    error_message
FROM etl_execution_log
WHERE status = 'FAILED'
ORDER BY start_time DESC
LIMIT 10;

-- 4.4 ETL performance over time (last 30 days)
SELECT 
    DATE(start_time) as execution_date,
    COUNT(*) as total_runs,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful_runs,
    SUM(records_processed) as total_records,
    ROUND(AVG(duration_seconds), 2) as avg_duration_sec
FROM etl_execution_log
WHERE start_time >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY DATE(start_time)
ORDER BY execution_date DESC;

-- 4.5 Data quality issues summary
SELECT 
    dql.table_name,
    dql.check_type,
    COUNT(*) as check_count,
    SUM(dql.records_checked) as total_records_checked,
    SUM(dql.records_failed) as total_records_failed,
    ROUND(AVG(dql.failure_percentage), 2) as avg_failure_pct,
    MAX(dql.check_timestamp) as last_check
FROM data_quality_log dql
GROUP BY dql.table_name, dql.check_type
ORDER BY total_records_failed DESC;

-- ============================================================================
-- SECTION 5: TECHNICAL INDICATORS QUERIES
-- ============================================================================

-- 5.1 Latest technical indicators for a specific stock
SELECT 
    s.symbol,
    ti.indicator_date,
    ti.indicator_type,
    ti.indicator_value,
    ti.time_period
FROM technical_indicators ti
INNER JOIN stocks s ON ti.stock_id = s.stock_id
WHERE s.symbol = 'AAPL'
  AND ti.indicator_date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY ti.indicator_date DESC, ti.indicator_type, ti.time_period;

-- 5.2 Compare SMA indicators across different periods
SELECT 
    s.symbol,
    ti.indicator_date,
    MAX(CASE WHEN ti.time_period = 10 THEN ti.indicator_value END) as sma_10,
    MAX(CASE WHEN ti.time_period = 20 THEN ti.indicator_value END) as sma_20,
    MAX(CASE WHEN ti.time_period = 50 THEN ti.indicator_value END) as sma_50,
    MAX(CASE WHEN ti.time_period = 200 THEN ti.indicator_value END) as sma_200
FROM technical_indicators ti
INNER JOIN stocks s ON ti.stock_id = s.stock_id
WHERE ti.indicator_type = 'SMA'
  AND s.symbol IN ('AAPL', 'MSFT')
  AND ti.indicator_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY s.symbol, ti.indicator_date
ORDER BY s.symbol, ti.indicator_date DESC;

-- 5.3 Golden Cross / Death Cross detection (SMA 50 crossing SMA 200)
WITH sma_data AS (
    SELECT 
        s.symbol,
        ti.indicator_date,
        MAX(CASE WHEN ti.time_period = 50 THEN ti.indicator_value END) as sma_50,
        MAX(CASE WHEN ti.time_period = 200 THEN ti.indicator_value END) as sma_200,
        LAG(MAX(CASE WHEN ti.time_period = 50 THEN ti.indicator_value END)) 
            OVER (PARTITION BY s.stock_id ORDER BY ti.indicator_date) as prev_sma_50,
        LAG(MAX(CASE WHEN ti.time_period = 200 THEN ti.indicator_value END)) 
            OVER (PARTITION BY s.stock_id ORDER BY ti.indicator_date) as prev_sma_200
    FROM technical_indicators ti
    INNER JOIN stocks s ON ti.stock_id = s.stock_id
    WHERE ti.indicator_type = 'SMA'
      AND ti.time_period IN (50, 200)
    GROUP BY s.stock_id, s.symbol, ti.indicator_date
)
SELECT 
    symbol,
    indicator_date,
    sma_50,
    sma_200,
    CASE 
        WHEN prev_sma_50 < prev_sma_200 AND sma_50 > sma_200 THEN 'Golden Cross (Bullish)'
        WHEN prev_sma_50 > prev_sma_200 AND sma_50 < sma_200 THEN 'Death Cross (Bearish)'
        ELSE 'No Cross'
    END as crossover_signal
FROM sma_data
WHERE sma_50 IS NOT NULL 
  AND sma_200 IS NOT NULL
  AND prev_sma_50 IS NOT NULL
  AND prev_sma_200 IS NOT NULL
  AND indicator_date >= CURRENT_DATE - INTERVAL '90 days'
ORDER BY indicator_date DESC;

-- 6.3 Check table sizes
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size,
    pg_total_relation_size(schemaname||'.'||tablename) AS size_bytes
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- 6.4 Check index usage
SELECT 
    schemaname,
    tablename,
    indexname,
    idx_scan as number_of_scans,
    idx_tup_read as tuples_read,
    idx_tup_fetch as tuples_fetched
FROM pg_stat_user_indexes
WHERE schemaname = 'public'
ORDER BY idx_scan DESC;

-- ============================================================================
-- END OF USEFUL QUERIES
-- ============================================================================