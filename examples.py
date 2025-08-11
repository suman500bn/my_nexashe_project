"""
Example usage scripts for the Economic Data Pipeline

This module contains example scripts showing how to use various components
of the economic data pipeline independently.
"""

import os
import sys
import pandas as pd
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent.parent / 'src'))

def example_world_bank_usage():
    """Example: Fetch World Bank data for specific countries"""
    print("=== World Bank API Example ===")
    
    from src.data_fetchers.world_bank_fetcher import WorldBankFetcher
    
    fetcher = WorldBankFetcher()
    
    # Fetch data for G7 countries
    g7_countries = ['USA', 'GBR', 'DEU', 'FRA', 'JPN', 'CAN', 'ITA']
    
    inflation_data = fetcher.fetch_inflation_data(
        countries=g7_countries,
        start_year=2015,
        end_year=2023,
        save_path="examples/data"
    )
    
    print(f"Fetched {len(inflation_data)} records")
    print("\nSample data:")
    print(inflation_data.head())
    
    # Show inflation trends
    if not inflation_data.empty:
        avg_inflation = inflation_data.groupby('year')['value'].mean()
        print(f"\nG7 Average Inflation by Year:")
        for year, rate in avg_inflation.items():
            print(f"  {year}: {rate:.2f}%")

def example_data_standardization():
    """Example: Standardize country codes"""
    print("\n=== Country Code Standardization Example ===")
    
    from src.processors.data_standardizer import CountryCodeStandardizer
    
    standardizer = CountryCodeStandardizer()
    
    # Test various country code formats
    test_codes = [
        'US', 'USA', 'United States', 'United States of America',
        'UK', 'GBR', 'Great Britain', 'United Kingdom',
        'Germany', 'Deutschland', 'DEU',
        'China', 'CHN', "People's Republic of China"
    ]
    
    print("Country Code Standardization:")
    for code in test_codes:
        standardized = standardizer.standardize_country_code(code)
        print(f"  {code:<30} -> {standardized}")

def example_percentage_normalization():
    """Example: Normalize percentage values"""
    print("\n=== Percentage Normalization Example ===")
    
    from src.processors.data_standardizer import PercentageNormalizer
    
    normalizer = PercentageNormalizer()
    
    # Test various percentage formats
    test_values = [5.2, 0.052, 150, 1.5, 25.5, 0.025]
    
    print("Percentage Normalization (auto-detect):")
    for value in test_values:
        normalized = normalizer.normalize_percentage(value, 'auto')
        print(f"  {value:<8} -> {normalized:.4f} ({normalized*100:.2f}%)")

def example_spark_processing():
    """Example: Basic Spark data processing"""
    print("\n=== Spark Processing Example ===")
    
    from src.processors.spark_processor import SparkDataProcessor
    
    processor = SparkDataProcessor({
        'app_name': 'ExampleApp',
        'master': 'local[2]'
    })
    
    # Create sample data
    sample_data = [
        ("USA", "United States", 2020, 0.012),
        ("GBR", "United Kingdom", 2020, 0.009),
        ("DEU", "Germany", 2020, 0.004),
        ("USA", "United States", 2021, 0.047),
        ("GBR", "United Kingdom", 2021, 0.026),
        ("DEU", "Germany", 2021, 0.032)
    ]
    
    # Create Spark DataFrame
    columns = ["country_code", "country_name", "year", "inflation_rate"]
    df = processor.spark.createDataFrame(sample_data, columns)
    
    print("Sample Spark DataFrame:")
    df.show()
    
    # Perform some aggregations
    print("Average inflation by country:")
    avg_inflation = df.groupBy("country_code", "country_name").agg(
        {"inflation_rate": "avg"}
    ).withColumnRenamed("avg(inflation_rate)", "avg_inflation_rate")
    
    avg_inflation.show()
    
    processor.close()

def example_database_queries():
    """Example: Database queries (requires database setup)"""
    print("\n=== Database Query Example ===")
    
    try:
        from src.processors.database_loader import DatabaseLoader
        from config.settings import Config
        
        config = Config()
        db_config = {
            'host': config.DB_HOST,
            'port': config.DB_PORT,
            'database': config.DB_NAME,
            'user': config.DB_USER,
            'password': config.DB_PASSWORD
        }
        
        if not config.DB_PASSWORD:
            print("⚠️ No database password configured. Skipping database example.")
            return
        
        loader = DatabaseLoader(db_config)
        
        # Test connection
        test_result = loader.execute_query("SELECT 1 as test")
        if not test_result.empty:
            print("✅ Database connection successful")
            
            # Get table counts (if tables exist)
            counts = loader.get_table_counts()
            print("Table record counts:")
            for table, count in counts.items():
                print(f"  {table}: {count:,}")
        
        loader.close()
        
    except Exception as e:
        print(f"Database example failed: {e}")
        print("This is expected if database is not configured")

def create_sample_analysis():
    """Create a sample analysis report"""
    print("\n=== Sample Analysis Example ===")
    
    # Create sample economic data
    countries = ['USA', 'GBR', 'DEU', 'FRA', 'JPN']
    years = [2019, 2020, 2021, 2022, 2023]
    
    data = []
    import random
    
    for country in countries:
        for year in years:
            # Simulate economic data
            base_inflation = {'USA': 0.02, 'GBR': 0.025, 'DEU': 0.015, 'FRA': 0.02, 'JPN': 0.005}
            inflation = base_inflation[country] + random.uniform(-0.01, 0.03)
            
            base_wage = {'USA': 50000, 'GBR': 45000, 'DEU': 48000, 'FRA': 42000, 'JPN': 35000}
            wage = base_wage[country] * (1 + random.uniform(-0.05, 0.1))
            
            data.append({
                'country': country,
                'year': year,
                'inflation_rate': inflation,
                'avg_wage': wage,
                'real_wage': wage / (1 + inflation)
            })
    
    df = pd.DataFrame(data)
    
    # Perform analysis
    print("Economic Indicators Analysis (Sample Data)")
    print("=" * 50)
    
    # Average inflation by country
    avg_inflation = df.groupby('country')['inflation_rate'].mean().sort_values(ascending=False)
    print("\nAverage Inflation Rate by Country (2019-2023):")
    for country, rate in avg_inflation.items():
        print(f"  {country}: {rate:.3f} ({rate*100:.1f}%)")
    
    # Real wage analysis
    print("\nReal Wage Trends:")
    for country in countries:
        country_data = df[df['country'] == country].sort_values('year')
        wage_change = ((country_data['real_wage'].iloc[-1] / country_data['real_wage'].iloc[0]) - 1) * 100
        print(f"  {country}: {wage_change:+.1f}% change in real wages")
    
    # Save sample analysis
    os.makedirs("examples/output", exist_ok=True)
    df.to_csv("examples/output/sample_analysis.csv", index=False)
    print(f"\nSample data saved to: examples/output/sample_analysis.csv")

def main():
    """Run all examples"""
    print("🚀 Economic Data Pipeline - Usage Examples")
    print("=" * 60)
    
    # Create examples directory
    os.makedirs("examples/data", exist_ok=True)
    os.makedirs("examples/output", exist_ok=True)
    
    try:
        example_world_bank_usage()
    except Exception as e:
        print(f"World Bank example failed: {e}")
    
    example_data_standardization()
    example_percentage_normalization()
    
    try:
        example_spark_processing()
    except Exception as e:
        print(f"Spark example failed: {e}")
    
    example_database_queries()
    create_sample_analysis()
    
    print("\n✅ Examples completed!")
    print("\nNext steps:")
    print("1. Review the generated sample files in examples/")
    print("2. Run the full pipeline: python main.py")
    print("3. Check the README.md for detailed documentation")

if __name__ == "__main__":
    main()
