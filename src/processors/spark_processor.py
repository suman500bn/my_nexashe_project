from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, regexp_replace, trim, upper, year, month
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import pandas as pd
import os
import logging
from typing import Dict, List, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SparkDataProcessor:
    """Process and merge economic data using PySpark"""
    
    def __init__(self, spark_config: Dict = None):
        self.spark_config = spark_config or {}
        self.spark = self._create_spark_session()
    
    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session"""
        
        builder = SparkSession.builder.appName(
            self.spark_config.get('app_name', 'EconomicDataPipeline')
        )
        
        # Set master
        master = self.spark_config.get('master', 'local[*]')
        builder = builder.master(master)
        
        # Configure memory and other settings
        builder = builder.config("spark.sql.adaptive.enabled", "true")
        builder = builder.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        builder = builder.config("spark.driver.memory", "4g")
        builder = builder.config("spark.executor.memory", "4g")
        
        return builder.getOrCreate()
    
    def load_inflation_data(self, file_path: str) -> DataFrame:
        """Load and clean World Bank inflation data"""
        
        logger.info(f"Loading inflation data from {file_path}")
        
        # Define schema
        schema = StructType([
            StructField("country_code", StringType(), True),
            StructField("country_name", StringType(), True),
            StructField("indicator_code", StringType(), True),
            StructField("indicator_name", StringType(), True),
            StructField("year", IntegerType(), True),
            StructField("value", DoubleType(), True)
        ])
        
        # Load data
        df = self.spark.read.csv(file_path, header=True, schema=schema)
        
        # Clean and standardize
        df = df.filter(col("value").isNotNull())
        df = df.filter(col("year").between(2000, 2030))
        
        # Standardize country codes (basic cleaning)
        df = df.withColumn("country_code", 
                          when(col("country_code") == "USA", "USA")
                          .when(col("country_code") == "GBR", "GBR")
                          .otherwise(col("country_code")))
        
        # Normalize percentage values (convert to decimal)
        df = df.withColumn("inflation_rate", 
                          when(col("value") > 1, col("value") / 100.0)
                          .otherwise(col("value")))
        
        # Select and rename columns for consistency
        df = df.select(
            col("country_code"),
            col("country_name"),
            col("year"),
            col("inflation_rate"),
            col("indicator_name").alias("inflation_indicator")
        )
        
        return df
    
    def load_commodity_data(self, file_path: str) -> DataFrame:
        """Load and clean IMF commodity data"""
        
        logger.info(f"Loading commodity data from {file_path}")
        
        # Define schema
        schema = StructType([
            StructField("year", IntegerType(), True),
            StructField("month", IntegerType(), True),
            StructField("commodity", StringType(), True),
            StructField("price", DoubleType(), True)
        ])
        
        # Load data
        df = self.spark.read.csv(file_path, header=True, schema=schema)
        
        # Clean data
        df = df.filter(col("price").isNotNull())
        df = df.filter(col("year").between(2000, 2030))
        df = df.filter(col("month").between(1, 12))
        
        # Clean commodity names
        df = df.withColumn("commodity", trim(col("commodity")))
        df = df.withColumn("commodity", regexp_replace(col("commodity"), r"[^\w\s()]", ""))
        
        # Calculate annual averages
        annual_df = df.groupBy("year", "commodity").agg(
            {"price": "avg"}
        ).withColumnRenamed("avg(price)", "avg_annual_price")
        
        return annual_df
    
    def load_wage_data(self, file_path: str) -> DataFrame:
        """Load and clean OECD wage data"""
        
        logger.info(f"Loading wage data from {file_path}")
        
        # Define schema
        schema = StructType([
            StructField("country_code", StringType(), True),
            StructField("year", IntegerType(), True),
            StructField("indicator", StringType(), True),
            StructField("value", DoubleType(), True)
        ])
        
        # Load data
        df = self.spark.read.csv(file_path, header=True, schema=schema)
        
        # Clean data
        df = df.filter(col("value").isNotNull())
        df = df.filter(col("year").between(2000, 2030))
        
        # Pivot wage indicators to columns
        wage_df = df.groupBy("country_code", "year").pivot("indicator").agg(
            {"value": "first"}
        )
        
        # Rename columns for clarity
        column_mappings = {
            "Average Annual Wages": "avg_annual_wage",
            "Unit Labour Costs": "unit_labour_cost",
            "Hourly Earnings Growth": "hourly_earnings_growth",
            "Real Wage Index": "real_wage_index"
        }
        
        for old_col, new_col in column_mappings.items():
            if old_col in wage_df.columns:
                wage_df = wage_df.withColumnRenamed(old_col, new_col)
        
        return wage_df
    
    def merge_datasets(self, inflation_df: DataFrame, 
                      commodity_df: DataFrame, 
                      wage_df: DataFrame) -> DataFrame:
        """Merge all economic datasets"""
        
        logger.info("Merging economic datasets")
        
        # Start with inflation data as base
        merged_df = inflation_df
        
        # Join with wage data on country and year
        merged_df = merged_df.join(
            wage_df, 
            on=["country_code", "year"], 
            how="left"
        )
        
        # For commodity data, we'll create a representative basket
        # Calculate weighted average of key commodities
        key_commodities = ["Crude Oil (petroleum)", "Gold", "Wheat", "Aluminum", "Copper"]
        
        commodity_filtered = commodity_df.filter(
            col("commodity").isin(key_commodities)
        )
        
        # Calculate composite commodity price index
        commodity_index = commodity_filtered.groupBy("year").agg(
            {"avg_annual_price": "avg"}
        ).withColumnRenamed("avg(avg_annual_price)", "commodity_price_index")
        
        # Join commodity index with main dataset
        merged_df = merged_df.join(
            commodity_index,
            on=["year"],
            how="left"
        )
        
        # Add derived features
        merged_df = self._add_derived_features(merged_df)
        
        return merged_df
    
    def _add_derived_features(self, df: DataFrame) -> DataFrame:
        """Add derived economic indicators"""
        
        # Real wage adjusted for inflation
        df = df.withColumn(
            "real_wage_adjusted",
            when((col("avg_annual_wage").isNotNull()) & (col("inflation_rate").isNotNull()),
                 col("avg_annual_wage") / (1 + col("inflation_rate")))
            .otherwise(None)
        )
        
        # Inflation category
        df = df.withColumn(
            "inflation_category",
            when(col("inflation_rate") < 0, "Deflation")
            .when(col("inflation_rate") <= 0.02, "Low")
            .when(col("inflation_rate") <= 0.05, "Moderate")
            .when(col("inflation_rate") <= 0.10, "High")
            .otherwise("Very High")
        )
        
        # Economic health score (simplified)
        df = df.withColumn(
            "economic_health_score",
            when((col("inflation_rate").between(0.01, 0.03)) & 
                 (col("real_wage_index") > 100), 5)
            .when(col("inflation_rate").between(0.01, 0.05), 4)
            .when(col("inflation_rate").between(-0.01, 0.01), 3)
            .when(col("inflation_rate") > 0.10, 1)
            .otherwise(2)
        )
        
        return df
    
    def save_processed_data(self, df: DataFrame, output_path: str, 
                           format: str = "parquet") -> None:
        """Save processed data to specified format"""
        
        logger.info(f"Saving processed data to {output_path}")
        
        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        if format.lower() == "parquet":
            df.write.mode("overwrite").parquet(output_path)
        elif format.lower() == "csv":
            df.coalesce(1).write.mode("overwrite").csv(output_path, header=True)
        elif format.lower() == "json":
            df.write.mode("overwrite").json(output_path)
        else:
            raise ValueError(f"Unsupported format: {format}")
        
        logger.info(f"Data saved successfully to {output_path}")
    
    def get_data_summary(self, df: DataFrame) -> Dict:
        """Get summary statistics of the processed data"""
        
        summary = {
            "total_records": df.count(),
            "countries": df.select("country_code").distinct().count(),
            "years": df.select("year").distinct().count(),
            "date_range": {
                "min_year": df.agg({"year": "min"}).collect()[0][0],
                "max_year": df.agg({"year": "max"}).collect()[0][0]
            }
        }
        
        # Additional statistics
        numeric_columns = ["inflation_rate", "avg_annual_wage", "commodity_price_index"]
        for col_name in numeric_columns:
            if col_name in df.columns:
                stats = df.select(col_name).describe().collect()
                summary[f"{col_name}_stats"] = {
                    row['summary']: row[col_name] for row in stats
                }
        
        return summary
    
    def close(self):
        """Close Spark session"""
        if self.spark:
            self.spark.stop()

if __name__ == "__main__":
    # Example usage
    processor = SparkDataProcessor()
    
    # This would typically be called from the main pipeline
    print("Spark Data Processor initialized successfully")
    processor.close()
