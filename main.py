#!/usr/bin/env python3
"""
Economic Data Pipeline - Main Orchestrator

This script orchestrates the complete economic data pipeline:
1. Fetch data from World Bank API, IMF, and OECD
2. Clean and standardize data
3. Merge datasets using PySpark
4. Load into PostgreSQL database
"""

import os
import sys
import logging
from pathlib import Path
import pandas as pd
from pyspark.sql import SparkSession

# Add src to path for imports
sys.path.append(str(Path(__file__).parent / 'src'))

from config.settings import Config
from src.data_fetchers.world_bank_fetcher import WorldBankFetcher
from src.data_fetchers.imf_fetcher import IMFFetcher
from src.data_fetchers.oecd_fetcher import OECDFetcher
from src.processors.data_standardizer import CountryCodeStandardizer, PercentageNormalizer
from src.processors.spark_processor import SparkDataProcessor
from src.processors.database_loader import DatabaseLoader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class EconomicDataPipeline:
    """Main pipeline orchestrator"""
    
    def __init__(self):
        self.config = Config()
        self.standardizer = CountryCodeStandardizer()
        self.spark_processor = None
        self.db_loader = None
        
        # Create data directories
        os.makedirs(self.config.RAW_DATA_PATH, exist_ok=True)
        os.makedirs(self.config.PROCESSED_DATA_PATH, exist_ok=True)
    
    def step1_fetch_world_bank_data(self):
        """Step 1: Fetch inflation indicators from World Bank API"""
        logger.info("=== Step 1: Fetching World Bank inflation data ===")
        
        try:
            fetcher = WorldBankFetcher(self.config.WORLD_BANK_API_URL)
            
            # Fetch data for major economies and all available countries
            df = fetcher.fetch_inflation_data(
                countries=None,  # All countries
                start_year=2000,
                end_year=2023,
                save_path=self.config.RAW_DATA_PATH
            )
            
            logger.info(f"Fetched {len(df)} World Bank inflation records")
            return df
            
        except Exception as e:
            logger.error(f"Error in Step 1: {str(e)}")
            raise
    
    def step2_fetch_imf_data(self):
        """Step 2: Download IMF commodity prices CSV"""
        logger.info("=== Step 2: Fetching IMF commodity prices ===")
        
        try:
            fetcher = IMFFetcher()
            df = fetcher.fetch_commodity_prices(save_path=self.config.RAW_DATA_PATH)
            
            logger.info(f"Fetched {len(df)} IMF commodity price records")
            return df
            
        except Exception as e:
            logger.error(f"Error in Step 2: {str(e)}")
            raise
    
    def step3_fetch_oecd_data(self):
        """Step 3: Pull wage data from OECD Stats portal"""
        logger.info("=== Step 3: Fetching OECD wage data ===")
        
        try:
            fetcher = OECDFetcher()
            
            # Focus on major OECD countries
            major_oecd_countries = [
                'USA', 'GBR', 'DEU', 'FRA', 'JPN', 'CAN', 'AUS', 'ITA', 
                'ESP', 'NLD', 'BEL', 'AUT', 'CHE', 'SWE', 'NOR', 'DNK'
            ]
            
            df = fetcher.fetch_wage_data(
                countries=major_oecd_countries,
                start_year=2000,
                end_year=2023,
                save_path=self.config.RAW_DATA_PATH
            )
            
            logger.info(f"Fetched {len(df)} OECD wage records")
            return df
            
        except Exception as e:
            logger.error(f"Error in Step 3: {str(e)}")
            raise
    
    def step4_clean_and_standardize(self, inflation_df, commodity_df, wage_df):
        """Step 4: Clean and standardize all country codes"""
        logger.info("=== Step 4: Cleaning and standardizing data ===")
        
        try:
            # Standardize country codes
            if 'country_code' in inflation_df.columns:
                inflation_df = self.standardizer.standardize_dataframe(
                    inflation_df, 'country_code'
                )
            
            if 'country_code' in wage_df.columns:
                wage_df = self.standardizer.standardize_dataframe(
                    wage_df, 'country_code'
                )
            
            # Normalize percentage formatting
            if 'value' in inflation_df.columns:
                inflation_df['value'] = inflation_df['value'].apply(
                    lambda x: PercentageNormalizer.normalize_percentage(x, 'auto')
                )
            
            logger.info("Data cleaning and standardization completed")
            
            # Save standardized data
            inflation_df.to_csv(
                os.path.join(self.config.PROCESSED_DATA_PATH, 'standardized_inflation.csv'),
                index=False
            )
            commodity_df.to_csv(
                os.path.join(self.config.PROCESSED_DATA_PATH, 'standardized_commodities.csv'),
                index=False
            )
            wage_df.to_csv(
                os.path.join(self.config.PROCESSED_DATA_PATH, 'standardized_wages.csv'),
                index=False
            )
            
            return inflation_df, commodity_df, wage_df
            
        except Exception as e:
            logger.error(f"Error in Step 4: {str(e)}")
            raise
    
    def step5_merge_with_spark(self, inflation_df, commodity_df, wage_df):
        """Step 5: Merge datasets using PySpark"""
        logger.info("=== Step 5: Merging datasets with PySpark ===")
        
        try:
            # Initialize Spark processor
            spark_config = {
                'app_name': self.config.SPARK_APP_NAME,
                'master': self.config.SPARK_MASTER
            }
            self.spark_processor = SparkDataProcessor(spark_config)
            
            # Save pandas DataFrames as CSV for Spark to read
            inflation_path = os.path.join(self.config.PROCESSED_DATA_PATH, 'spark_inflation.csv')
            commodity_path = os.path.join(self.config.PROCESSED_DATA_PATH, 'spark_commodities.csv')
            wage_path = os.path.join(self.config.PROCESSED_DATA_PATH, 'spark_wages.csv')
            
            inflation_df.to_csv(inflation_path, index=False)
            commodity_df.to_csv(commodity_path, index=False)
            wage_df.to_csv(wage_path, index=False)
            
            # Load data into Spark
            spark_inflation = self.spark_processor.load_inflation_data(inflation_path)
            spark_commodity = self.spark_processor.load_commodity_data(commodity_path)
            spark_wage = self.spark_processor.load_wage_data(wage_path)
            
            # Merge datasets
            merged_df = self.spark_processor.merge_datasets(
                spark_inflation, spark_commodity, spark_wage
            )
            
            # Save merged data
            output_path = os.path.join(self.config.PROCESSED_DATA_PATH, 'merged_economic_data')
            self.spark_processor.save_processed_data(merged_df, output_path, format='parquet')
            
            # Also save as CSV for database loading
            csv_output_path = os.path.join(self.config.PROCESSED_DATA_PATH, 'merged_economic_data.csv')
            self.spark_processor.save_processed_data(merged_df, csv_output_path, format='csv')
            
            # Get summary statistics
            summary = self.spark_processor.get_data_summary(merged_df)
            logger.info(f"Merged dataset summary: {summary}")
            
            # Convert to pandas for database loading
            merged_pandas_df = merged_df.toPandas()
            
            logger.info(f"Merged {len(merged_pandas_df)} records using Spark")
            return merged_pandas_df
            
        except Exception as e:
            logger.error(f"Error in Step 5: {str(e)}")
            raise
    
    def step6_setup_database(self):
        """Step 6: Create PostgreSQL schema"""
        logger.info("=== Step 6: Setting up PostgreSQL database ===")
        
        try:
            # Database configuration
            db_config = {
                'host': self.config.DB_HOST,
                'port': self.config.DB_PORT,
                'database': self.config.DB_NAME,
                'user': self.config.DB_USER,
                'password': self.config.DB_PASSWORD
            }
            
            self.db_loader = DatabaseLoader(db_config)
            
            # Create schema
            schema_created = self.db_loader.create_schema()
            if schema_created:
                logger.info("Database schema created successfully")
            else:
                logger.warning("Schema creation had issues, but continuing...")
            
            return True
            
        except Exception as e:
            logger.error(f"Error in Step 6: {str(e)}")
            # Don't raise - database might not be available in demo environment
            logger.warning("Continuing without database connection")
            return False
    
    def step7_load_to_database(self, inflation_df, commodity_df, wage_df, merged_df):
        """Step 7: Load data into PostgreSQL tables"""
        logger.info("=== Step 7: Loading data into PostgreSQL ===")
        
        if not self.db_loader:
            logger.warning("No database connection available, skipping database load")
            return False
        
        try:
            # Load countries reference data
            countries_df = merged_df[['country_code', 'country_name']].drop_duplicates()
            self.db_loader.load_countries_data(countries_df)
            
            # Load inflation rates
            self.db_loader.load_inflation_data(inflation_df)
            
            # Load wage data
            self.db_loader.load_wage_data(wage_df)
            
            # Load commodity prices
            self.db_loader.load_commodity_data(commodity_df)
            
            # Load consolidated economic indicators
            self.db_loader.load_economic_indicators(merged_df)
            
            # Validate data integrity
            validation_results = self.db_loader.validate_data_integrity()
            logger.info(f"Data validation results: {validation_results}")
            
            # Get final table counts
            table_counts = self.db_loader.get_table_counts()
            logger.info(f"Final table counts: {table_counts}")
            
            logger.info("Data loading completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error in Step 7: {str(e)}")
            return False
    
    def run_pipeline(self):
        """Run the complete data pipeline"""
        logger.info("🚀 Starting Economic Data Pipeline")
        
        try:
            # Step 1-3: Data Fetching
            inflation_df = self.step1_fetch_world_bank_data()
            commodity_df = self.step2_fetch_imf_data()
            wage_df = self.step3_fetch_oecd_data()
            
            # Step 4: Data Cleaning
            inflation_df, commodity_df, wage_df = self.step4_clean_and_standardize(
                inflation_df, commodity_df, wage_df
            )
            
            # Step 5: Data Merging with Spark
            merged_df = self.step5_merge_with_spark(inflation_df, commodity_df, wage_df)
            
            # Step 6-7: Database Operations
            db_success = self.step6_setup_database()
            if db_success:
                self.step7_load_to_database(inflation_df, commodity_df, wage_df, merged_df)
            
            logger.info("✅ Pipeline completed successfully!")
            
            # Print summary
            self._print_pipeline_summary(inflation_df, commodity_df, wage_df, merged_df)
            
        except Exception as e:
            logger.error(f"❌ Pipeline failed: {str(e)}")
            raise
        finally:
            self._cleanup()
    
    def _print_pipeline_summary(self, inflation_df, commodity_df, wage_df, merged_df):
        """Print pipeline execution summary"""
        
        print("\n" + "="*60)
        print("📊 PIPELINE EXECUTION SUMMARY")
        print("="*60)
        print(f"Raw Data Records:")
        print(f"  • World Bank Inflation: {len(inflation_df):,}")
        print(f"  • IMF Commodities: {len(commodity_df):,}")
        print(f"  • OECD Wages: {len(wage_df):,}")
        print(f"\nProcessed Data:")
        print(f"  • Merged Economic Indicators: {len(merged_df):,}")
        print(f"\nData Storage:")
        print(f"  • Raw data saved to: {self.config.RAW_DATA_PATH}")
        print(f"  • Processed data saved to: {self.config.PROCESSED_DATA_PATH}")
        if self.db_loader:
            table_counts = self.db_loader.get_table_counts()
            print(f"  • Database records loaded: {sum(table_counts.values()):,}")
        print("="*60)
    
    def _cleanup(self):
        """Clean up resources"""
        if self.spark_processor:
            self.spark_processor.close()
        if self.db_loader:
            self.db_loader.close()

def main():
    """Main entry point"""
    try:
        pipeline = EconomicDataPipeline()
        pipeline.run_pipeline()
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
    except Exception as e:
        logger.error(f"Pipeline failed with error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
