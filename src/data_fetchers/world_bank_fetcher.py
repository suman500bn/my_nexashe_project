import requests
import json
import pandas as pd
import os
from typing import Dict, List, Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WorldBankFetcher:
    """Fetch inflation indicators from World Bank API"""
    
    def __init__(self, base_url: str = "https://api.worldbank.org/v2"):
        self.base_url = base_url
        self.inflation_indicators = {
            'NY.GDP.DEFL.KD.ZG': 'GDP deflator (annual %)',
            'FP.CPI.TOTL.ZG': 'Inflation, consumer prices (annual %)',
            'NY.GDP.DEFL.ZS': 'GDP deflator: linked series (base year varies by country)'
        }
    
    def fetch_inflation_data(self, countries: List[str] = None, 
                           start_year: int = 2000, 
                           end_year: int = 2023,
                           save_path: str = "data/raw") -> pd.DataFrame:
        """
        Fetch inflation indicators for specified countries and years
        
        Args:
            countries: List of country ISO codes (default: all countries)
            start_year: Start year for data
            end_year: End year for data
            save_path: Path to save raw data
            
        Returns:
            pandas.DataFrame: Combined inflation data
        """
        
        if countries is None:
            countries = ['all']  # World Bank API accepts 'all' for all countries
        
        all_data = []
        
        for indicator_code, indicator_name in self.inflation_indicators.items():
            logger.info(f"Fetching {indicator_name} data...")
            
            try:
                # Construct API URL
                countries_str = ';'.join(countries)
                url = f"{self.base_url}/country/{countries_str}/indicator/{indicator_code}"
                
                params = {
                    'date': f"{start_year}:{end_year}",
                    'format': 'json',
                    'per_page': 10000  # Increase page size
                }
                
                response = requests.get(url, params=params)
                response.raise_for_status()
                
                data = response.json()
                
                # World Bank API returns metadata in first element, data in second
                if len(data) > 1 and data[1]:
                    for record in data[1]:
                        if record['value'] is not None:
                            all_data.append({
                                'country_code': record['countryiso3code'],
                                'country_name': record['country']['value'],
                                'indicator_code': indicator_code,
                                'indicator_name': indicator_name,
                                'year': int(record['date']),
                                'value': float(record['value'])
                            })
                
            except Exception as e:
                logger.error(f"Error fetching {indicator_name}: {str(e)}")
                continue
        
        # Convert to DataFrame
        df = pd.DataFrame(all_data)
        
        # Save raw data
        os.makedirs(save_path, exist_ok=True)
        output_file = os.path.join(save_path, 'world_bank_inflation.csv')
        df.to_csv(output_file, index=False)
        logger.info(f"Saved {len(df)} records to {output_file}")
        
        return df
    
    def get_country_list(self) -> pd.DataFrame:
        """Get list of all countries from World Bank API"""
        try:
            url = f"{self.base_url}/country"
            params = {'format': 'json', 'per_page': 500}
            
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            if len(data) > 1 and data[1]:
                countries = []
                for country in data[1]:
                    countries.append({
                        'country_code': country['id'],
                        'country_name': country['name'],
                        'iso3_code': country['iso2Code'],
                        'region': country['region']['value'] if country['region']['id'] != 'NA' else None,
                        'income_level': country['incomeLevel']['value'] if country['incomeLevel']['id'] != 'NA' else None
                    })
                
                return pd.DataFrame(countries)
        
        except Exception as e:
            logger.error(f"Error fetching country list: {str(e)}")
            return pd.DataFrame()

if __name__ == "__main__":
    fetcher = WorldBankFetcher()
    df = fetcher.fetch_inflation_data(start_year=2010, end_year=2023)
    print(f"Fetched {len(df)} inflation records")
    print(df.head())
