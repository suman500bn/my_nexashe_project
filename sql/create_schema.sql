-- Economic Data Pipeline Database Schema
-- PostgreSQL Schema for storing processed economic data

-- Create database (run separately as superuser if needed)
-- CREATE DATABASE economic_data;

-- Connect to the economic_data database before running the following

-- Create schema for organizing tables
CREATE SCHEMA IF NOT EXISTS economic_data;

-- Set search path
SET search_path TO economic_data, public;

-- Countries reference table
CREATE TABLE IF NOT EXISTS countries (
    country_code VARCHAR(3) PRIMARY KEY,
    country_name VARCHAR(255) NOT NULL,
    region VARCHAR(100),
    income_level VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Inflation rates table
CREATE TABLE IF NOT EXISTS inflation_rates (
    id SERIAL PRIMARY KEY,
    country_code VARCHAR(3) NOT NULL,
    year INTEGER NOT NULL,
    inflation_rate DECIMAL(10, 6),
    inflation_indicator VARCHAR(255),
    inflation_category VARCHAR(20),
    data_source VARCHAR(50) DEFAULT 'World Bank',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (country_code) REFERENCES countries(country_code) ON DELETE CASCADE,
    UNIQUE(country_code, year, inflation_indicator)
);

-- Wage index table
CREATE TABLE IF NOT EXISTS wage_index (
    id SERIAL PRIMARY KEY,
    country_code VARCHAR(3) NOT NULL,
    year INTEGER NOT NULL,
    avg_annual_wage DECIMAL(12, 2),
    unit_labour_cost DECIMAL(10, 4),
    hourly_earnings_growth DECIMAL(8, 4),
    real_wage_index DECIMAL(10, 4),
    real_wage_adjusted DECIMAL(12, 2),
    data_source VARCHAR(50) DEFAULT 'OECD',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (country_code) REFERENCES countries(country_code) ON DELETE CASCADE,
    UNIQUE(country_code, year)
);

-- Commodity prices table
CREATE TABLE IF NOT EXISTS commodity_prices (
    id SERIAL PRIMARY KEY,
    year INTEGER NOT NULL,
    month INTEGER,
    commodity VARCHAR(255) NOT NULL,
    price DECIMAL(12, 4),
    avg_annual_price DECIMAL(12, 4),
    price_unit VARCHAR(50),
    data_source VARCHAR(50) DEFAULT 'IMF',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(year, month, commodity)
);

-- Economic indicators summary table (denormalized for analytics)
CREATE TABLE IF NOT EXISTS economic_indicators (
    id SERIAL PRIMARY KEY,
    country_code VARCHAR(3) NOT NULL,
    country_name VARCHAR(255),
    year INTEGER NOT NULL,
    inflation_rate DECIMAL(10, 6),
    inflation_category VARCHAR(20),
    avg_annual_wage DECIMAL(12, 2),
    unit_labour_cost DECIMAL(10, 4),
    real_wage_index DECIMAL(10, 4),
    real_wage_adjusted DECIMAL(12, 2),
    commodity_price_index DECIMAL(12, 4),
    economic_health_score INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (country_code) REFERENCES countries(country_code) ON DELETE CASCADE,
    UNIQUE(country_code, year)
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_inflation_rates_country_year 
    ON inflation_rates(country_code, year);

CREATE INDEX IF NOT EXISTS idx_inflation_rates_year 
    ON inflation_rates(year);

CREATE INDEX IF NOT EXISTS idx_wage_index_country_year 
    ON wage_index(country_code, year);

CREATE INDEX IF NOT EXISTS idx_wage_index_year 
    ON wage_index(year);

CREATE INDEX IF NOT EXISTS idx_commodity_prices_year 
    ON commodity_prices(year);

CREATE INDEX IF NOT EXISTS idx_commodity_prices_commodity 
    ON commodity_prices(commodity);

CREATE INDEX IF NOT EXISTS idx_economic_indicators_country_year 
    ON economic_indicators(country_code, year);

CREATE INDEX IF NOT EXISTS idx_economic_indicators_year 
    ON economic_indicators(year);

CREATE INDEX IF NOT EXISTS idx_economic_indicators_health_score 
    ON economic_indicators(economic_health_score);

-- Create triggers to update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Apply triggers to all tables
CREATE TRIGGER update_countries_updated_at 
    BEFORE UPDATE ON countries 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_inflation_rates_updated_at 
    BEFORE UPDATE ON inflation_rates 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_wage_index_updated_at 
    BEFORE UPDATE ON wage_index 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_commodity_prices_updated_at 
    BEFORE UPDATE ON commodity_prices 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_economic_indicators_updated_at 
    BEFORE UPDATE ON economic_indicators 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Create views for common queries
CREATE OR REPLACE VIEW latest_economic_data AS
SELECT 
    ei.country_code,
    ei.country_name,
    ei.year,
    ei.inflation_rate,
    ei.inflation_category,
    ei.avg_annual_wage,
    ei.real_wage_adjusted,
    ei.economic_health_score,
    RANK() OVER (PARTITION BY ei.country_code ORDER BY ei.year DESC) as year_rank
FROM economic_indicators ei
WHERE ei.year >= 2020;

CREATE OR REPLACE VIEW high_inflation_countries AS
SELECT 
    country_code,
    country_name,
    year,
    inflation_rate,
    inflation_category
FROM economic_indicators
WHERE inflation_rate > 0.10  -- Above 10%
ORDER BY inflation_rate DESC;

CREATE OR REPLACE VIEW wage_inflation_correlation AS
SELECT 
    country_code,
    country_name,
    year,
    inflation_rate,
    avg_annual_wage,
    real_wage_adjusted,
    CASE 
        WHEN real_wage_adjusted > avg_annual_wage THEN 'Wage Growth Outpacing Inflation'
        WHEN real_wage_adjusted < avg_annual_wage THEN 'Inflation Outpacing Wage Growth'
        ELSE 'Balanced'
    END as wage_inflation_relationship
FROM economic_indicators
WHERE inflation_rate IS NOT NULL 
    AND avg_annual_wage IS NOT NULL 
    AND real_wage_adjusted IS NOT NULL;

-- Grant permissions (adjust as needed for your environment)
-- GRANT ALL PRIVILEGES ON SCHEMA economic_data TO economic_user;
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA economic_data TO economic_user;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA economic_data TO economic_user;
