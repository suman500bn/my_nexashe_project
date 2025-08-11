import pandas as pd
import pycountry
import iso3166
import re
import logging
from typing import Dict, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CountryCodeStandardizer:
    """Standardize country codes across different datasets"""
    
    def __init__(self):
        # Create mapping dictionaries
        self.iso2_to_iso3 = {}
        self.name_to_iso3 = {}
        self.custom_mappings = self._get_custom_mappings()
        
        # Build standard mappings
        self._build_country_mappings()
    
    def _build_country_mappings(self):
        """Build country code mappings using pycountry"""
        
        try:
            for country in pycountry.countries:
                # ISO2 to ISO3 mapping
                if hasattr(country, 'alpha_2') and hasattr(country, 'alpha_3'):
                    self.iso2_to_iso3[country.alpha_2] = country.alpha_3
                
                # Name to ISO3 mapping
                if hasattr(country, 'name') and hasattr(country, 'alpha_3'):
                    self.name_to_iso3[country.name.upper()] = country.alpha_3
                
                # Common name variations
                if hasattr(country, 'common_name') and hasattr(country, 'alpha_3'):
                    self.name_to_iso3[country.common_name.upper()] = country.alpha_3
                    
        except Exception as e:
            logger.warning(f"Error building country mappings: {str(e)}")
    
    def _get_custom_mappings(self) -> Dict[str, str]:
        """Custom mappings for special cases and common variations"""
        
        return {
            # Common variations and special cases
            'UNITED STATES': 'USA',
            'US': 'USA',
            'UNITED STATES OF AMERICA': 'USA',
            'UNITED KINGDOM': 'GBR',
            'UK': 'GBR',
            'GREAT BRITAIN': 'GBR',
            'RUSSIA': 'RUS',
            'RUSSIAN FEDERATION': 'RUS',
            'SOUTH KOREA': 'KOR',
            'KOREA, REPUBLIC OF': 'KOR',
            'KOREA': 'KOR',
            'NORTH KOREA': 'PRK',
            'CHINA': 'CHN',
            'PEOPLE\'S REPUBLIC OF CHINA': 'CHN',
            'IRAN': 'IRN',
            'IRAN, ISLAMIC REPUBLIC OF': 'IRN',
            'VENEZUELA': 'VEN',
            'VENEZUELA, BOLIVARIAN REPUBLIC OF': 'VEN',
            'SYRIA': 'SYR',
            'SYRIAN ARAB REPUBLIC': 'SYR',
            'TANZANIA': 'TZA',
            'UNITED REPUBLIC OF TANZANIA': 'TZA',
            'VIETNAM': 'VNM',
            'VIET NAM': 'VNM',
            'BOLIVIA': 'BOL',
            'PLURINATIONAL STATE OF BOLIVIA': 'BOL',
            'MOLDOVA': 'MDA',
            'REPUBLIC OF MOLDOVA': 'MDA',
            'MACEDONIA': 'MKD',
            'NORTH MACEDONIA': 'MKD',
            'FORMER YUGOSLAV REPUBLIC OF MACEDONIA': 'MKD',
            
            # World Bank specific codes
            'WLD': 'WLD',  # World
            'EUU': 'EUU',  # European Union
            'HIC': 'HIC',  # High income countries
            'LIC': 'LIC',  # Low income countries
            'MIC': 'MIC',  # Middle income countries
            'UMC': 'UMC',  # Upper middle income countries
            'LMC': 'LMC',  # Lower middle income countries
            
            # OECD specific codes
            'OECD': 'OECD',
            'EA19': 'EA19',  # Euro area (19 countries)
            'EU28': 'EU28',  # European Union (28 countries)
            'G7': 'G7',
            'G20': 'G20'
        }
    
    def standardize_country_code(self, code: str) -> Optional[str]:
        """
        Standardize a country code to ISO3 format
        
        Args:
            code: Country code or name in various formats
            
        Returns:
            str: ISO3 country code or None if not found
        """
        
        if not code or pd.isna(code):
            return None
        
        code = str(code).strip().upper()
        
        # Check if already ISO3
        if len(code) == 3 and code.isalpha():
            try:
                pycountry.countries.get(alpha_3=code)
                return code
            except:
                pass
        
        # Check custom mappings first
        if code in self.custom_mappings:
            return self.custom_mappings[code]
        
        # Check ISO2 to ISO3 mapping
        if len(code) == 2 and code in self.iso2_to_iso3:
            return self.iso2_to_iso3[code]
        
        # Check name to ISO3 mapping
        if code in self.name_to_iso3:
            return self.name_to_iso3[code]
        
        # Try fuzzy matching for country names
        return self._fuzzy_country_match(code)
    
    def _fuzzy_country_match(self, name: str) -> Optional[str]:
        """Attempt fuzzy matching for country names"""
        
        # Clean the name
        clean_name = re.sub(r'[^\w\s]', '', name).strip()
        
        # Try partial matches
        for country_name, iso3_code in self.name_to_iso3.items():
            if clean_name in country_name or country_name in clean_name:
                return iso3_code
        
        # Try word matching
        name_words = set(clean_name.split())
        for country_name, iso3_code in self.name_to_iso3.items():
            country_words = set(country_name.split())
            if name_words.intersection(country_words):
                return iso3_code
        
        logger.warning(f"Could not standardize country code: {name}")
        return None
    
    def standardize_dataframe(self, df: pd.DataFrame, 
                             country_column: str = 'country_code') -> pd.DataFrame:
        """
        Standardize country codes in a DataFrame
        
        Args:
            df: Input DataFrame
            country_column: Name of the country code column
            
        Returns:
            pd.DataFrame: DataFrame with standardized country codes
        """
        
        if country_column not in df.columns:
            logger.error(f"Column '{country_column}' not found in DataFrame")
            return df
        
        df = df.copy()
        
        # Apply standardization
        df[f'{country_column}_standardized'] = df[country_column].apply(
            self.standardize_country_code
        )
        
        # Keep track of unmapped codes
        unmapped = df[df[f'{country_column}_standardized'].isna()][country_column].unique()
        if len(unmapped) > 0:
            logger.warning(f"Could not map {len(unmapped)} country codes: {list(unmapped)[:10]}")
        
        # Remove rows with unmapped countries
        initial_count = len(df)
        df = df.dropna(subset=[f'{country_column}_standardized'])
        final_count = len(df)
        
        if initial_count != final_count:
            logger.info(f"Removed {initial_count - final_count} rows with unmapped country codes")
        
        # Replace original column
        df[country_column] = df[f'{country_column}_standardized']
        df = df.drop(columns=[f'{country_column}_standardized'])
        
        return df

class PercentageNormalizer:
    """Normalize percentage and decimal formatting across datasets"""
    
    @staticmethod
    def normalize_percentage(value, current_format: str = 'auto') -> Optional[float]:
        """
        Normalize percentage values to decimal format (0-1)
        
        Args:
            value: The value to normalize
            current_format: 'percentage' (0-100), 'decimal' (0-1), or 'auto'
            
        Returns:
            float: Normalized value in decimal format
        """
        
        if pd.isna(value):
            return None
        
        try:
            val = float(value)
            
            if current_format == 'auto':
                # Auto-detect format based on value range
                if abs(val) > 10:  # Likely percentage format
                    return val / 100.0
                else:  # Likely decimal format
                    return val
            elif current_format == 'percentage':
                return val / 100.0
            else:  # decimal format
                return val
                
        except (ValueError, TypeError):
            return None
    
    @staticmethod
    def normalize_dataframe_percentages(df: pd.DataFrame, 
                                      percentage_columns: list,
                                      current_format: str = 'auto') -> pd.DataFrame:
        """
        Normalize percentage columns in a DataFrame
        
        Args:
            df: Input DataFrame
            percentage_columns: List of column names containing percentages
            current_format: Format of current percentage values
            
        Returns:
            pd.DataFrame: DataFrame with normalized percentages
        """
        
        df = df.copy()
        
        for col in percentage_columns:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: PercentageNormalizer.normalize_percentage(x, current_format)
                )
        
        return df

if __name__ == "__main__":
    # Test country standardization
    standardizer = CountryCodeStandardizer()
    
    test_codes = ['US', 'USA', 'United States', 'GB', 'GBR', 'China', 'CHN']
    for code in test_codes:
        result = standardizer.standardize_country_code(code)
        print(f"{code} -> {result}")
    
    # Test percentage normalization
    normalizer = PercentageNormalizer()
    test_values = [5.2, 0.052, 150, 1.5]
    for val in test_values:
        result = normalizer.normalize_percentage(val, 'auto')
        print(f"{val} -> {result}")
