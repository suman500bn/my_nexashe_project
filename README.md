# Economic Data Pipeline

A comprehensive PySpark-based data pipeline for processing economic indicators from multiple international sources including World Bank, IMF, and OECD.

## 🌟 Features

### Data Sources
- **World Bank API**: Inflation indicators (GDP deflator, CPI)
- **IMF**: Commodity prices (oil, gold, metals, agricultural products)
- **OECD**: Wage statistics and labor market indicators

### Data Processing
- **Country Code Standardization**: Converts all country identifiers to ISO3 format
- **Percentage Normalization**: Standardizes percentage/decimal formatting
- **Data Cleaning**: Handles missing values, outliers, and data quality issues
- **PySpark Integration**: Distributed processing for large datasets

### Database Integration
- **PostgreSQL Schema**: Optimized database structure for economic data
- **Multiple Tables**: `inflation_rates`, `wage_index`, `commodity_prices`, `economic_indicators`
- **Data Validation**: Integrity checks and relationship validation
- **Performance Optimization**: Indexes and views for analytics

## 🏗️ Project Structure

```
nexashe_project/
├── config/
│   ├── __init__.py
│   └── settings.py              # Configuration management
├── data/
│   ├── raw/                     # Raw data from APIs
│   └── processed/               # Cleaned and merged data
├── src/
│   ├── data_fetchers/
│   │   ├── __init__.py
│   │   ├── world_bank_fetcher.py    # World Bank API client
│   │   ├── imf_fetcher.py           # IMF data downloader
│   │   └── oecd_fetcher.py          # OECD Stats API client
│   └── processors/
│       ├── __init__.py
│       ├── data_standardizer.py     # Country codes & percentage normalization
│       ├── spark_processor.py       # PySpark data processing
│       └── database_loader.py       # PostgreSQL data loading
├── sql/
│   └── create_schema.sql        # Database schema definition
├── .env.example                 # Environment variables template
├── .github/
│   └── copilot-instructions.md  # AI coding assistant instructions
├── requirements.txt             # Python dependencies
├── main.py                      # Main pipeline orchestrator
└── README.md                    # This file
```

## 🚀 Quick Start

### Prerequisites

1. **Python 3.8+** with pip
2. **PostgreSQL 12+** (optional for database features)
3. **Java 8+** (required for PySpark)

### Installation

1. **Clone and setup the project:**
   ```bash
   cd nexashe_project
   pip install -r requirements.txt
   ```

2. **Configure environment variables:**
   ```bash
   cp .env.example .env
   # Edit .env with your database credentials
   ```

3. **Setup PostgreSQL (optional):**
   ```bash
   # Create database
   createdb economic_data
   
   # Run schema creation
   psql economic_data < sql/create_schema.sql
   ```

### Running the Pipeline

**Complete pipeline execution:**
```bash
python main.py
```

**Individual components:**
```python
# Fetch World Bank data only
from src.data_fetchers.world_bank_fetcher import WorldBankFetcher
fetcher = WorldBankFetcher()
data = fetcher.fetch_inflation_data(start_year=2010, end_year=2023)

# Process with Spark
from src.processors.spark_processor import SparkDataProcessor
processor = SparkDataProcessor()
# ... process data
```

## 📊 Data Pipeline Workflow

### Step 1: Data Fetching
- **World Bank API**: Retrieves inflation indicators for all countries
- **IMF**: Downloads commodity price CSV data
- **OECD**: Fetches wage and labor statistics

### Step 2: Data Cleaning & Standardization
- **Country Codes**: Standardizes to ISO3 format (USA, GBR, DEU, etc.)
- **Percentages**: Normalizes to decimal format (0.05 for 5%)
- **Data Quality**: Removes invalid records and outliers

### Step 3: Data Processing with PySpark
- **Distributed Processing**: Handles large datasets efficiently
- **Data Merging**: Joins inflation, wage, and commodity data
- **Feature Engineering**: Creates derived economic indicators
- **Performance Optimization**: Adaptive query execution

### Step 4: Database Loading
- **Schema Creation**: Sets up optimized PostgreSQL tables
- **Data Loading**: Inserts processed data with relationships
- **Validation**: Ensures data integrity and completeness

## 🗄️ Database Schema

### Core Tables

**`countries`** - Reference table for country information
```sql
country_code VARCHAR(3) PRIMARY KEY  -- ISO3 codes (USA, GBR, etc.)
country_name VARCHAR(255)
region VARCHAR(100)
income_level VARCHAR(50)
```

**`inflation_rates`** - World Bank inflation data
```sql
country_code VARCHAR(3)
year INTEGER
inflation_rate DECIMAL(10,6)      -- Normalized to decimal format
inflation_indicator VARCHAR(255)  -- GDP deflator, CPI, etc.
inflation_category VARCHAR(20)    -- Low, Moderate, High, etc.
```

**`wage_index`** - OECD wage and labor data
```sql
country_code VARCHAR(3)
year INTEGER
avg_annual_wage DECIMAL(12,2)     -- USD
unit_labour_cost DECIMAL(10,4)    -- Index
real_wage_index DECIMAL(10,4)     -- Index
real_wage_adjusted DECIMAL(12,2)  -- Inflation-adjusted wages
```

**`commodity_prices`** - IMF commodity price data
```sql
year INTEGER
commodity VARCHAR(255)            -- Oil, Gold, Wheat, etc.
price DECIMAL(12,4)               -- USD per unit
avg_annual_price DECIMAL(12,4)    -- Annual average
```

**`economic_indicators`** - Consolidated analytics table
```sql
country_code VARCHAR(3)
year INTEGER
inflation_rate DECIMAL(10,6)
avg_annual_wage DECIMAL(12,2)
commodity_price_index DECIMAL(12,4)
economic_health_score INTEGER     -- 1-5 scoring system
```

### Analytics Views

- **`latest_economic_data`**: Most recent data for each country
- **`high_inflation_countries`**: Countries with inflation > 10%
- **`wage_inflation_correlation`**: Real wage vs inflation analysis

## 🔧 Configuration

### Environment Variables

```bash
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=economic_data
DB_USER=postgres
DB_PASSWORD=your_password

# Spark Configuration
SPARK_MASTER=local[*]
SPARK_APP_NAME=EconomicDataPipeline

# API Endpoints (usually don't need to change)
WORLD_BANK_API_URL=https://api.worldbank.org/v2
IMF_COMMODITY_URL=https://www.imf.org/-/media/Files/Research/CommodityPrices/Monthly/ExternalData.ashx
OECD_API_BASE=https://stats.oecd.org/restsdmx/sdmx.ashx/GetData
```

### Spark Configuration

The pipeline automatically configures Spark for optimal performance:
- **Adaptive Query Execution**: Automatically optimizes queries
- **Dynamic Coalescing**: Reduces partition overhead
- **Memory Management**: 4GB driver and executor memory
- **Local Execution**: Uses all available CPU cores

## 📈 Usage Examples

### Basic Data Analysis

```python
from src.processors.database_loader import DatabaseLoader

# Connect to database
config = {
    'host': 'localhost', 'port': '5432',
    'database': 'economic_data', 'user': 'postgres', 'password': 'password'
}
loader = DatabaseLoader(config)

# Get high inflation countries
high_inflation = loader.execute_query("""
    SELECT country_name, year, inflation_rate 
    FROM economic_data.high_inflation_countries 
    ORDER BY inflation_rate DESC LIMIT 10
""")

# Analyze wage-inflation relationship
wage_analysis = loader.execute_query("""
    SELECT country_name, 
           AVG(inflation_rate) as avg_inflation,
           AVG(real_wage_adjusted) as avg_real_wage
    FROM economic_data.wage_inflation_correlation 
    WHERE year >= 2010
    GROUP BY country_name
    ORDER BY avg_inflation DESC
""")
```

### Custom Data Processing

```python
from src.processors.spark_processor import SparkDataProcessor

# Initialize Spark processor
processor = SparkDataProcessor({
    'app_name': 'CustomAnalysis',
    'master': 'local[4]'  # Use 4 cores
})

# Load and process custom data
df = processor.spark.read.csv('custom_data.csv', header=True)
processed_df = df.filter(col('year') >= 2020)

# Perform analysis
result = processed_df.groupBy('country').agg(
    avg('inflation_rate').alias('avg_inflation'),
    max('commodity_price_index').alias('max_commodity_price')
)

result.show()
processor.close()
```

## 🔍 Data Quality & Validation

### Automated Checks
- **Orphaned Records**: Ensures all foreign key relationships are valid
- **Value Ranges**: Validates inflation rates are within reasonable bounds (-50% to 500%)
- **Completeness**: Checks for required data coverage
- **Consistency**: Verifies country code standardization

### Manual Validation Queries

```sql
-- Check data coverage by country and year
SELECT country_code, 
       COUNT(DISTINCT year) as years_covered,
       MIN(year) as earliest_year,
       MAX(year) as latest_year
FROM economic_data.economic_indicators
GROUP BY country_code
ORDER BY years_covered DESC;

-- Identify potential data quality issues
SELECT country_code, year, inflation_rate
FROM economic_data.inflation_rates
WHERE inflation_rate > 1.0 OR inflation_rate < -0.5  -- Outside normal range
ORDER BY ABS(inflation_rate) DESC;
```

## 🚀 Performance Optimization

### Spark Optimizations
- **Partition Management**: Automatic partition coalescing
- **Broadcast Joins**: For small lookup tables
- **Predicate Pushdown**: Filter optimization
- **Column Pruning**: Memory optimization

### Database Optimizations
- **Indexing Strategy**: Composite indexes on frequently queried columns
- **Partitioning**: Consider partitioning large tables by year
- **Materialized Views**: Pre-computed aggregations for analytics

## 🔧 Troubleshooting

### Common Issues

**1. Spark Memory Errors**
```bash
# Increase driver memory
export SPARK_DRIVER_MEMORY=8g
export SPARK_EXECUTOR_MEMORY=8g
```

**2. API Rate Limiting**
- World Bank: No rate limit, but be respectful
- IMF: Large file downloads may be slow
- OECD: May have rate limits, includes retry logic

**3. Database Connection Issues**
```python
# Test database connection
from src.processors.database_loader import DatabaseLoader
loader = DatabaseLoader(config)
result = loader.execute_query("SELECT 1")
print(result)
```

**4. Country Code Standardization**
```python
from src.processors.data_standardizer import CountryCodeStandardizer
standardizer = CountryCodeStandardizer()
print(standardizer.standardize_country_code("United States"))  # -> USA
```

## 📝 Contributing

1. **Code Style**: Follow PEP 8 guidelines
2. **Documentation**: Update docstrings and README for new features
3. **Testing**: Add unit tests for new functionality
4. **Logging**: Use the configured logging system

## 📄 License

This project is licensed under the MIT License. See LICENSE file for details.

## 🤝 Support

For questions or issues:
1. Check the troubleshooting section above
2. Review the logs in `pipeline.log`
3. Ensure all dependencies are properly installed
4. Verify database connection configuration

---

**Built with ❤️ using PySpark, PostgreSQL, and modern data engineering practices.**
