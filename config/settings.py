import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Database Configuration
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = os.getenv('DB_PORT', '5432')
    DB_NAME = os.getenv('DB_NAME', 'economic_data')
    DB_USER = os.getenv('DB_USER', 'postgres')
    DB_PASSWORD = os.getenv('DB_PASSWORD', '')
    
    # Spark Configuration
    SPARK_MASTER = os.getenv('SPARK_MASTER', 'local[*]')
    SPARK_APP_NAME = os.getenv('SPARK_APP_NAME', 'EconomicDataPipeline')
    
    # Data Sources
    WORLD_BANK_API_URL = os.getenv('WORLD_BANK_API_URL', 'https://api.worldbank.org/v2')
    IMF_COMMODITY_URL = os.getenv('IMF_COMMODITY_URL', 'https://www.imf.org/-/media/Files/Research/CommodityPrices/Monthly/ExternalData.ashx')
    OECD_API_BASE = os.getenv('OECD_API_BASE', 'https://stats.oecd.org/restsdmx/sdmx.ashx/GetData')
    
    # Data Paths
    RAW_DATA_PATH = 'data/raw'
    PROCESSED_DATA_PATH = 'data/processed'
    
    @property
    def database_url(self):
        return f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
    
    @property
    def jdbc_url(self):
        return f"jdbc:postgresql://{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
