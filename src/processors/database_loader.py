import psycopg2
import pandas as pd
from sqlalchemy import create_engine, text
from pyspark.sql import DataFrame as SparkDataFrame
import logging
from typing import Dict, List, Optional
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseLoader:
    """Load processed data into PostgreSQL database"""
    
    def __init__(self, connection_config: Dict):
        self.config = connection_config
        self.engine = None
        self.connection = None
        self._setup_connection()
    
    def _setup_connection(self):
        """Setup database connection"""
        try:
            # SQLAlchemy engine for pandas operations
            connection_string = (
                f"postgresql://{self.config['user']}:{self.config['password']}"
                f"@{self.config['host']}:{self.config['port']}/{self.config['database']}"
            )
            self.engine = create_engine(connection_string)
            
            # Direct psycopg2 connection for raw SQL
            self.connection = psycopg2.connect(
                host=self.config['host'],
                port=self.config['port'],
                database=self.config['database'],
                user=self.config['user'],
                password=self.config['password']
            )
            
            logger.info("Database connection established successfully")
            
        except Exception as e:
            logger.error(f"Failed to connect to database: {str(e)}")
            raise
    
    def create_schema(self, schema_file: str = "sql/create_schema.sql"):
        """Create database schema from SQL file"""
        
        if not os.path.exists(schema_file):
            logger.error(f"Schema file not found: {schema_file}")
            return False
        
        try:
            with open(schema_file, 'r') as f:
                schema_sql = f.read()
            
            cursor = self.connection.cursor()
            cursor.execute(schema_sql)
            self.connection.commit()
            cursor.close()
            
            logger.info("Database schema created successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error creating schema: {str(e)}")
            self.connection.rollback()
            return False
    
    def load_countries_data(self, countries_df: pd.DataFrame) -> bool:
        """Load countries reference data"""
        
        try:
            # Prepare countries data
            countries_df = countries_df[['country_code', 'country_name']].drop_duplicates()
            
            # Load to database
            countries_df.to_sql(
                'countries',
                self.engine,
                schema='economic_data',
                if_exists='replace',
                index=False,
                method='multi'
            )
            
            logger.info(f"Loaded {len(countries_df)} countries to database")
            return True
            
        except Exception as e:
            logger.error(f"Error loading countries data: {str(e)}")
            return False
    
    def load_inflation_data(self, inflation_df: pd.DataFrame) -> bool:
        """Load inflation rates data"""
        
        try:
            # Prepare inflation data
            inflation_cols = [
                'country_code', 'year', 'inflation_rate', 
                'inflation_indicator', 'inflation_category'
            ]
            
            # Filter to available columns
            available_cols = [col for col in inflation_cols if col in inflation_df.columns]
            data_to_load = inflation_df[available_cols].dropna(subset=['country_code', 'year'])
            
            # Load to database
            data_to_load.to_sql(
                'inflation_rates',
                self.engine,
                schema='economic_data',
                if_exists='append',
                index=False,
                method='multi'
            )
            
            logger.info(f"Loaded {len(data_to_load)} inflation records to database")
            return True
            
        except Exception as e:
            logger.error(f"Error loading inflation data: {str(e)}")
            return False
    
    def load_wage_data(self, wage_df: pd.DataFrame) -> bool:
        """Load wage index data"""
        
        try:
            # Prepare wage data
            wage_cols = [
                'country_code', 'year', 'avg_annual_wage', 'unit_labour_cost',
                'hourly_earnings_growth', 'real_wage_index', 'real_wage_adjusted'
            ]
            
            # Filter to available columns
            available_cols = [col for col in wage_cols if col in wage_df.columns]
            data_to_load = wage_df[available_cols].dropna(subset=['country_code', 'year'])
            
            # Load to database
            data_to_load.to_sql(
                'wage_index',
                self.engine,
                schema='economic_data',
                if_exists='append',
                index=False,
                method='multi'
            )
            
            logger.info(f"Loaded {len(data_to_load)} wage records to database")
            return True
            
        except Exception as e:
            logger.error(f"Error loading wage data: {str(e)}")
            return False
    
    def load_commodity_data(self, commodity_df: pd.DataFrame) -> bool:
        """Load commodity prices data"""
        
        try:
            # Prepare commodity data
            commodity_cols = ['year', 'month', 'commodity', 'price', 'avg_annual_price']
            
            # Filter to available columns
            available_cols = [col for col in commodity_cols if col in commodity_df.columns]
            data_to_load = commodity_df[available_cols].dropna(subset=['year', 'commodity'])
            
            # Load to database
            data_to_load.to_sql(
                'commodity_prices',
                self.engine,
                schema='economic_data',
                if_exists='append',
                index=False,
                method='multi'
            )
            
            logger.info(f"Loaded {len(data_to_load)} commodity price records to database")
            return True
            
        except Exception as e:
            logger.error(f"Error loading commodity data: {str(e)}")
            return False
    
    def load_economic_indicators(self, indicators_df: pd.DataFrame) -> bool:
        """Load consolidated economic indicators data"""
        
        try:
            # Prepare economic indicators data
            indicator_cols = [
                'country_code', 'country_name', 'year', 'inflation_rate',
                'inflation_category', 'avg_annual_wage', 'unit_labour_cost',
                'real_wage_index', 'real_wage_adjusted', 'commodity_price_index',
                'economic_health_score'
            ]
            
            # Filter to available columns
            available_cols = [col for col in indicator_cols if col in indicators_df.columns]
            data_to_load = indicators_df[available_cols].dropna(subset=['country_code', 'year'])
            
            # Load to database
            data_to_load.to_sql(
                'economic_indicators',
                self.engine,
                schema='economic_data',
                if_exists='append',
                index=False,
                method='multi'
            )
            
            logger.info(f"Loaded {len(data_to_load)} economic indicator records to database")
            return True
            
        except Exception as e:
            logger.error(f"Error loading economic indicators: {str(e)}")
            return False
    
    def load_spark_dataframe(self, spark_df: SparkDataFrame, table_name: str, 
                            jdbc_url: str, connection_properties: Dict) -> bool:
        """Load Spark DataFrame directly to PostgreSQL using JDBC"""
        
        try:
            spark_df.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", f"economic_data.{table_name}") \
                .option("user", connection_properties["user"]) \
                .option("password", connection_properties["password"]) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
            
            logger.info(f"Loaded Spark DataFrame to {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error loading Spark DataFrame to {table_name}: {str(e)}")
            return False
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute SQL query and return results as DataFrame"""
        
        try:
            return pd.read_sql(query, self.engine)
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            return pd.DataFrame()
    
    def get_table_counts(self) -> Dict[str, int]:
        """Get record counts for all tables"""
        
        tables = [
            'countries', 'inflation_rates', 'wage_index', 
            'commodity_prices', 'economic_indicators'
        ]
        
        counts = {}
        for table in tables:
            try:
                query = f"SELECT COUNT(*) as count FROM economic_data.{table}"
                result = self.execute_query(query)
                counts[table] = result.iloc[0]['count'] if not result.empty else 0
            except Exception as e:
                logger.warning(f"Could not get count for {table}: {str(e)}")
                counts[table] = 0
        
        return counts
    
    def validate_data_integrity(self) -> Dict[str, bool]:
        """Validate data integrity across tables"""
        
        validation_results = {}
        
        try:
            # Check for orphaned records
            orphaned_inflation = self.execute_query("""
                SELECT COUNT(*) as count 
                FROM economic_data.inflation_rates ir
                LEFT JOIN economic_data.countries c ON ir.country_code = c.country_code
                WHERE c.country_code IS NULL
            """)
            validation_results['no_orphaned_inflation'] = orphaned_inflation.iloc[0]['count'] == 0
            
            orphaned_wage = self.execute_query("""
                SELECT COUNT(*) as count 
                FROM economic_data.wage_index wi
                LEFT JOIN economic_data.countries c ON wi.country_code = c.country_code
                WHERE c.country_code IS NULL
            """)
            validation_results['no_orphaned_wage'] = orphaned_wage.iloc[0]['count'] == 0
            
            # Check for reasonable value ranges
            inflation_range = self.execute_query("""
                SELECT MIN(inflation_rate) as min_rate, MAX(inflation_rate) as max_rate
                FROM economic_data.inflation_rates
                WHERE inflation_rate IS NOT NULL
            """)
            if not inflation_range.empty:
                min_rate, max_rate = inflation_range.iloc[0]['min_rate'], inflation_range.iloc[0]['max_rate']
                validation_results['reasonable_inflation_range'] = -0.5 <= min_rate <= max_rate <= 5.0
            
            # Check for data completeness
            recent_data = self.execute_query("""
                SELECT COUNT(*) as count
                FROM economic_data.economic_indicators
                WHERE year >= 2020
            """)
            validation_results['has_recent_data'] = recent_data.iloc[0]['count'] > 0
            
        except Exception as e:
            logger.error(f"Error during data validation: {str(e)}")
            validation_results['validation_error'] = str(e)
        
        return validation_results
    
    def close(self):
        """Close database connections"""
        try:
            if self.connection:
                self.connection.close()
            if self.engine:
                self.engine.dispose()
            logger.info("Database connections closed")
        except Exception as e:
            logger.error(f"Error closing connections: {str(e)}")

if __name__ == "__main__":
    # Example usage
    config = {
        'host': 'localhost',
        'port': '5432',
        'database': 'economic_data',
        'user': 'postgres',
        'password': 'your_password'
    }
    
    loader = DatabaseLoader(config)
    counts = loader.get_table_counts()
    print("Table counts:", counts)
    loader.close()
