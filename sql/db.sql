-- =====================================================
-- ETL PROJECT - FINANCIAL DATA ANALYSIS
-- Database: etl_stocks
-- DBMS: PostgreSQL
-- Author: Gabriel Araújo (a27978@alunos.ipca.pt Github: @gabiquintao)
-- Date: October 2025
-- =====================================================

-- Drop existing tables
DROP TABLE IF EXISTS data_quality_log CASCADE;
DROP TABLE IF EXISTS etl_execution_log CASCADE;
DROP TABLE IF EXISTS market_overview CASCADE;
DROP TABLE IF EXISTS technical_indicators CASCADE;
DROP TABLE IF EXISTS daily_prices CASCADE;
DROP TABLE IF EXISTS stocks CASCADE;

-- ============================================================================
-- TABLE: stocks
-- Description: Master table for stock/company information
-- ============================================================================
CREATE TABLE stocks (
    stock_id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL UNIQUE,
    company_name VARCHAR(255),
    sector VARCHAR(100),
    industry VARCHAR(100),
    exchange VARCHAR(50),
    currency VARCHAR(3) DEFAULT 'USD',
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE stocks IS 'Master table containing information about stocks and companies';
COMMENT ON COLUMN stocks.symbol IS 'Stock ticker symbol (e.g., AAPL, MSFT)';
COMMENT ON COLUMN stocks.is_active IS 'Flag to indicate if stock is actively traded';

-- ============================================================================
-- TABLE: daily_prices
-- Description: Historical daily stock prices (OHLCV data)
-- ============================================================================
CREATE TABLE daily_prices (
    price_id BIGSERIAL PRIMARY KEY,
    stock_id INTEGER NOT NULL REFERENCES stocks(stock_id) ON DELETE CASCADE,
    price_date DATE NOT NULL,
    open_price NUMERIC(15, 4),
    high_price NUMERIC(15, 4),
    low_price NUMERIC(15, 4),
    close_price NUMERIC(15, 4) NOT NULL,
    adjusted_close NUMERIC(15, 4),
    volume BIGINT,
    daily_return NUMERIC(10, 6), -- Calculated: (close - prev_close) / prev_close
    price_change NUMERIC(15, 4), -- Calculated: close - open
    price_change_pct NUMERIC(10, 4), -- Calculated: (close - open) / open * 100
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(stock_id, price_date)
);

COMMENT ON TABLE daily_prices IS 'Historical daily OHLCV (Open, High, Low, Close, Volume) data';
COMMENT ON COLUMN daily_prices.adjusted_close IS 'Close price adjusted for splits and dividends';
COMMENT ON COLUMN daily_prices.daily_return IS 'Daily return percentage';

-- ============================================================================
-- TABLE: technical_indicators
-- Description: Technical analysis indicators (SMA, EMA, RSI, etc.)
-- ============================================================================
CREATE TABLE technical_indicators (
    indicator_id BIGSERIAL PRIMARY KEY,
    stock_id INTEGER NOT NULL REFERENCES stocks(stock_id) ON DELETE CASCADE,
    indicator_date DATE NOT NULL,
    indicator_type VARCHAR(20) NOT NULL, -- SMA, EMA, RSI, MACD, etc.
    indicator_value NUMERIC(15, 4) NOT NULL,
    time_period INTEGER, -- e.g., 10, 20, 50, 200 for moving averages
    series_type VARCHAR(20), -- close, open, high, low
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(stock_id, indicator_date, indicator_type, time_period)
);

COMMENT ON TABLE technical_indicators IS 'Technical analysis indicators calculated from price data';
COMMENT ON COLUMN technical_indicators.indicator_type IS 'Type of indicator: SMA, EMA, RSI, MACD, etc.';
COMMENT ON COLUMN technical_indicators.time_period IS 'Period used for calculation (e.g., 10-day SMA)';

-- ============================================================================
-- TABLE: market_overview
-- Description: General market information and company fundamentals
-- ============================================================================
CREATE TABLE market_overview (
    overview_id BIGSERIAL PRIMARY KEY,
    stock_id INTEGER NOT NULL REFERENCES stocks(stock_id) ON DELETE CASCADE,
    overview_date DATE NOT NULL,
    market_cap BIGINT,
    pe_ratio NUMERIC(10, 4),
    peg_ratio NUMERIC(10, 4),
    book_value NUMERIC(15, 4),
    dividend_per_share NUMERIC(10, 4),
    dividend_yield NUMERIC(10, 6),
    eps NUMERIC(10, 4), -- Earnings Per Share
    revenue_per_share NUMERIC(15, 4),
    profit_margin NUMERIC(10, 6),
    week_52_high NUMERIC(15, 4),
    week_52_low NUMERIC(15, 4),
    moving_avg_50 NUMERIC(15, 4),
    moving_avg_200 NUMERIC(15, 4),
    shares_outstanding BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(stock_id, overview_date)
);

COMMENT ON TABLE market_overview IS 'Market overview and fundamental analysis data';
COMMENT ON COLUMN market_overview.pe_ratio IS 'Price to Earnings ratio';
COMMENT ON COLUMN market_overview.eps IS 'Earnings Per Share';

-- ============================================================================
-- TABLE: etl_execution_log
-- Description: Log of ETL job executions for monitoring and debugging
-- ============================================================================
CREATE TABLE etl_execution_log (
    execution_id BIGSERIAL PRIMARY KEY,
    job_name VARCHAR(255) NOT NULL,
    transformation_name VARCHAR(255),
    execution_type VARCHAR(50), -- JOB, TRANSFORMATION
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    duration_seconds INTEGER,
    status VARCHAR(20) NOT NULL, -- RUNNING, SUCCESS, FAILED, WARNING
    records_read INTEGER DEFAULT 0,
    records_processed INTEGER DEFAULT 0,
    records_inserted INTEGER DEFAULT 0,
    records_updated INTEGER DEFAULT 0,
    records_rejected INTEGER DEFAULT 0,
    error_message TEXT,
    error_stacktrace TEXT,
    server_name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE etl_execution_log IS 'Audit log for ETL job and transformation executions';
COMMENT ON COLUMN etl_execution_log.status IS 'Execution status: RUNNING, SUCCESS, FAILED, WARNING';

-- ============================================================================
-- TABLE: data_quality_log
-- Description: Data quality checks and validation results
-- ============================================================================
CREATE TABLE data_quality_log (
    quality_id BIGSERIAL PRIMARY KEY,
    execution_id BIGINT REFERENCES etl_execution_log(execution_id),
    table_name VARCHAR(100) NOT NULL,
    check_type VARCHAR(50) NOT NULL, -- NULL_CHECK, DUPLICATE_CHECK, RANGE_CHECK, etc.
    check_description TEXT,
    records_checked INTEGER,
    records_passed INTEGER,
    records_failed INTEGER,
    failure_percentage NUMERIC(5, 2),
    error_details TEXT,
    check_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE data_quality_log IS 'Data quality validation results and statistics';
COMMENT ON COLUMN data_quality_log.check_type IS 'Type of quality check performed';

-- ============================================================================
-- INDEXES for Performance Optimization
-- ============================================================================

-- Indexes on stocks table
CREATE INDEX idx_stocks_symbol ON stocks(symbol);
CREATE INDEX idx_stocks_sector ON stocks(sector);
CREATE INDEX idx_stocks_active ON stocks(is_active);

-- Indexes on daily_prices table
CREATE INDEX idx_daily_prices_stock_id ON daily_prices(stock_id);
CREATE INDEX idx_daily_prices_date ON daily_prices(price_date);
CREATE INDEX idx_daily_prices_stock_date ON daily_prices(stock_id, price_date DESC);
CREATE INDEX idx_daily_prices_volume ON daily_prices(volume);

-- Indexes on technical_indicators table
CREATE INDEX idx_tech_indicators_stock_id ON technical_indicators(stock_id);
CREATE INDEX idx_tech_indicators_date ON technical_indicators(indicator_date);
CREATE INDEX idx_tech_indicators_type ON technical_indicators(indicator_type);
CREATE INDEX idx_tech_indicators_composite ON technical_indicators(stock_id, indicator_type, indicator_date DESC);

-- Indexes on market_overview table
CREATE INDEX idx_market_overview_stock_id ON market_overview(stock_id);
CREATE INDEX idx_market_overview_date ON market_overview(overview_date);

-- Indexes on etl_execution_log table
CREATE INDEX idx_etl_log_job_name ON etl_execution_log(job_name);
CREATE INDEX idx_etl_log_start_time ON etl_execution_log(start_time DESC);
CREATE INDEX idx_etl_log_status ON etl_execution_log(status);

-- Indexes on data_quality_log table
CREATE INDEX idx_quality_log_execution_id ON data_quality_log(execution_id);
CREATE INDEX idx_quality_log_table ON data_quality_log(table_name);
CREATE INDEX idx_quality_log_timestamp ON data_quality_log(check_timestamp DESC);

-- ============================================================================
-- TRIGGERS for automatic timestamp updates
-- ============================================================================

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger for stocks table
CREATE TRIGGER trigger_stocks_updated_at
    BEFORE UPDATE ON stocks
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Trigger for daily_prices table
CREATE TRIGGER trigger_daily_prices_updated_at
    BEFORE UPDATE ON daily_prices
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Trigger for market_overview table
CREATE TRIGGER trigger_market_overview_updated_at
    BEFORE UPDATE ON market_overview
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- VIEWS for easier data access and reporting
-- ============================================================================

-- View: Latest prices for all stocks
CREATE OR REPLACE VIEW vw_latest_prices AS
SELECT 
    s.symbol,
    s.company_name,
    s.sector,
    dp.price_date,
    dp.close_price,
    dp.volume,
    dp.daily_return,
    dp.price_change_pct
FROM stocks s
INNER JOIN daily_prices dp ON s.stock_id = dp.stock_id
INNER JOIN (
    SELECT stock_id, MAX(price_date) as max_date
    FROM daily_prices
    GROUP BY stock_id
) latest ON dp.stock_id = latest.stock_id AND dp.price_date = latest.max_date
WHERE s.is_active = TRUE
ORDER BY s.symbol;

COMMENT ON VIEW vw_latest_prices IS 'Latest available price for each active stock';

-- View: Stock performance summary
CREATE OR REPLACE VIEW vw_stock_performance AS
SELECT 
    s.symbol,
    s.company_name,
    COUNT(dp.price_id) as trading_days,
    MIN(dp.price_date) as first_date,
    MAX(dp.price_date) as last_date,
    AVG(dp.close_price) as avg_price,
    MIN(dp.low_price) as period_low,
    MAX(dp.high_price) as period_high,
    AVG(dp.volume) as avg_volume,
    AVG(dp.daily_return) as avg_daily_return
FROM stocks s
LEFT JOIN daily_prices dp ON s.stock_id = dp.stock_id
WHERE s.is_active = TRUE
GROUP BY s.stock_id, s.symbol, s.company_name
ORDER BY s.symbol;

COMMENT ON VIEW vw_stock_performance IS 'Summary statistics for each stock';

-- View: ETL execution summary
CREATE OR REPLACE VIEW vw_etl_execution_summary AS
SELECT 
    job_name,
    COUNT(*) as total_executions,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful_runs,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed_runs,
    AVG(duration_seconds) as avg_duration_seconds,
    MAX(start_time) as last_execution,
    SUM(records_processed) as total_records_processed
FROM etl_execution_log
GROUP BY job_name
ORDER BY last_execution DESC;

COMMENT ON VIEW vw_etl_execution_summary IS 'Summary of ETL job executions and performance';

-- ============================================================================
-- CONSTRAINTS and Business Rules
-- ============================================================================

-- Ensure prices are positive
ALTER TABLE daily_prices ADD CONSTRAINT chk_positive_prices 
    CHECK (open_price > 0 AND high_price > 0 AND low_price > 0 AND close_price > 0);

-- Ensure high >= low
ALTER TABLE daily_prices ADD CONSTRAINT chk_high_low 
    CHECK (high_price >= low_price);

-- Ensure high is the highest
ALTER TABLE daily_prices ADD CONSTRAINT chk_high_is_highest 
    CHECK (high_price >= open_price AND high_price >= close_price);

-- Ensure low is the lowest
ALTER TABLE daily_prices ADD CONSTRAINT chk_low_is_lowest 
    CHECK (low_price <= open_price AND low_price <= close_price);

-- Ensure volume is non-negative
ALTER TABLE daily_prices ADD CONSTRAINT chk_positive_volume 
    CHECK (volume >= 0);

-- Ensure execution duration is calculated correctly
ALTER TABLE etl_execution_log ADD CONSTRAINT chk_execution_duration
    CHECK (duration_seconds IS NULL OR duration_seconds >= 0);

-- ============================================================================
-- INITIAL DATA - Sample stocks to track
-- ============================================================================

INSERT INTO stocks (symbol, company_name, sector, industry, exchange) VALUES
('AAPL', 'Apple Inc.', 'Technology', 'Consumer Electronics', 'NASDAQ'),
('MSFT', 'Microsoft Corporation', 'Technology', 'Software', 'NASDAQ'),
('GOOGL', 'Alphabet Inc.', 'Technology', 'Internet Services', 'NASDAQ'),
('AMZN', 'Amazon.com Inc.', 'Consumer Cyclical', 'E-commerce', 'NASDAQ'),
('TSLA', 'Tesla Inc.', 'Consumer Cyclical', 'Automobiles', 'NASDAQ'),
('META', 'Meta Platforms Inc.', 'Technology', 'Social Media', 'NASDAQ'),
('NVDA', 'NVIDIA Corporation', 'Technology', 'Semiconductors', 'NASDAQ'),
('JPM', 'JPMorgan Chase & Co.', 'Financial', 'Banking', 'NYSE'),
('V', 'Visa Inc.', 'Financial', 'Credit Services', 'NYSE'),
('WMT', 'Walmart Inc.', 'Consumer Defensive', 'Discount Stores', 'NYSE'),
('IBM', 'International Business Machines', 'Technology', 'Information Technology', 'NYSE');

-- ============================================================================
-- UTILITY FUNCTIONS
-- ============================================================================

-- Function to get the latest price for a stock
CREATE OR REPLACE FUNCTION get_latest_price(p_symbol VARCHAR)
RETURNS TABLE (
    symbol VARCHAR,
    price_date DATE,
    close_price NUMERIC,
    daily_return NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        s.symbol,
        dp.price_date,
        dp.close_price,
        dp.daily_return
    FROM stocks s
    INNER JOIN daily_prices dp ON s.stock_id = dp.stock_id
    WHERE s.symbol = p_symbol
    ORDER BY dp.price_date DESC
    LIMIT 1;
END;
$$ LANGUAGE plpgsql;

-- Function to calculate moving average
CREATE OR REPLACE FUNCTION calculate_moving_average(
    p_stock_id INTEGER,
    p_end_date DATE,
    p_period INTEGER
)
RETURNS NUMERIC AS $$
DECLARE
    v_avg NUMERIC;
BEGIN
    SELECT AVG(close_price) INTO v_avg
    FROM (
        SELECT close_price
        FROM daily_prices
        WHERE stock_id = p_stock_id 
          AND price_date <= p_end_date
        ORDER BY price_date DESC
        LIMIT p_period
    ) subquery;
    
    RETURN v_avg;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- GRANTS
-- ============================================================================

-- Grant permissions to public for this educational project
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO postgres;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO postgres;

-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================

-- Check all tables were created
SELECT table_name, table_type
FROM information_schema.tables
WHERE table_schema = 'public'
ORDER BY table_name;

-- Check all indexes
SELECT indexname, tablename
FROM pg_indexes
WHERE schemaname = 'public'
ORDER BY tablename, indexname;

-- Verify initial data
SELECT * FROM stocks ORDER BY symbol;

-- ============================================================================
-- END OF SCRIPT
-- ============================================================================