import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import Config
from src.processors.database_loader import DatabaseLoader

class InflationAnalysis:
    """Detailed inflation analysis page"""
    
    def __init__(self):
        self.config = Config()
        self.db_loader = self._init_database()
    
    def _init_database(self):
        """Initialize database connection"""
        try:
            if self.config.DB_PASSWORD:
                db_config = {
                    'host': self.config.DB_HOST,
                    'port': self.config.DB_PORT,
                    'database': self.config.DB_NAME,
                    'user': self.config.DB_USER,
                    'password': self.config.DB_PASSWORD
                }
                return DatabaseLoader(db_config)
        except:
            pass
        return None
    
    @st.cache_data(ttl=300)
    def load_inflation_data(_self) -> pd.DataFrame:
        """Load inflation data from database or return sample data"""
        
        query = """
        SELECT 
            country_code,
            country_name,
            year,
            inflation_rate,
            inflation_category,
            inflation_indicator
        FROM economic_data.inflation_rates
        WHERE inflation_rate IS NOT NULL
        ORDER BY year DESC, country_code
        """
        
        if _self.db_loader:
            try:
                return _self.db_loader.execute_query(query)
            except:
                pass
        
        return _self._generate_sample_data()
    
    def _generate_sample_data(self) -> pd.DataFrame:
        """Generate sample inflation data"""
        import numpy as np
        
        countries = {
            'USA': 'United States', 'GBR': 'United Kingdom', 'DEU': 'Germany',
            'FRA': 'France', 'JPN': 'Japan', 'CAN': 'Canada', 'AUS': 'Australia',
            'ITA': 'Italy', 'ESP': 'Spain', 'NLD': 'Netherlands', 'CHE': 'Switzerland',
            'SWE': 'Sweden', 'NOR': 'Norway', 'DNK': 'Denmark', 'BEL': 'Belgium'
        }
        
        years = list(range(2000, 2024))
        indicators = ['CPI', 'GDP Deflator', 'Core CPI']
        
        np.random.seed(42)
        data = []
        
        for country_code, country_name in countries.items():
            base_inflation = np.random.uniform(0.005, 0.035)
            
            for year in years:
                for indicator in indicators:
                    # Create realistic inflation patterns
                    if year < 2008:
                        inflation = base_inflation + np.random.uniform(-0.01, 0.02)
                    elif year < 2010:  # Financial crisis
                        inflation = base_inflation + np.random.uniform(-0.03, 0.01)
                    elif year < 2020:  # Recovery period
                        inflation = base_inflation + np.random.uniform(-0.005, 0.015)
                    else:  # Recent high inflation
                        inflation = base_inflation + np.random.uniform(0, 0.08)
                    
                    # Adjust for different indicators
                    if indicator == 'Core CPI':
                        inflation *= 0.8  # Core typically lower
                    elif indicator == 'GDP Deflator':
                        inflation *= 1.1  # GDP deflator often higher
                    
                    category = self._categorize_inflation(inflation)
                    
                    data.append({
                        'country_code': country_code,
                        'country_name': country_name,
                        'year': year,
                        'inflation_rate': max(inflation, -0.05),  # Floor at -5%
                        'inflation_category': category,
                        'inflation_indicator': indicator
                    })
        
        return pd.DataFrame(data)
    
    def _categorize_inflation(self, rate: float) -> str:
        """Categorize inflation rate"""
        if rate < 0:
            return "Deflation"
        elif rate <= 0.02:
            return "Low"
        elif rate <= 0.05:
            return "Moderate"
        elif rate <= 0.10:
            return "High"
        else:
            return "Very High"
    
    def render_inflation_heatmap(self, data: pd.DataFrame, filters: dict):
        """Render inflation heatmap by country and year"""
        
        # Filter data
        filtered_data = data[
            (data['country_code'].isin(filters.get('countries', []))) &
            (data['year'].between(filters.get('year_range', (2000, 2023))[0], 
                                 filters.get('year_range', (2000, 2023))[1])) &
            (data['inflation_indicator'] == filters.get('indicator', 'CPI'))
        ]
        
        if filtered_data.empty:
            st.warning("No data available for selected filters")
            return
        
        # Create pivot table for heatmap
        heatmap_data = filtered_data.pivot_table(
            index='country_name',
            columns='year',
            values='inflation_rate',
            aggfunc='mean'
        )
        
        # Create heatmap
        fig = px.imshow(
            heatmap_data,
            title=f"Inflation Rates Heatmap ({filters.get('indicator', 'CPI')})",
            labels=dict(x="Year", y="Country", color="Inflation Rate"),
            color_continuous_scale='RdYlBu_r',
            aspect="auto"
        )
        
        # Format hover text
        fig.update_traces(
            hovertemplate="<b>%{y}</b><br>Year: %{x}<br>Inflation: %{z:.2%}<extra></extra>"
        )
        
        fig.update_layout(
            height=max(400, len(heatmap_data.index) * 25),
            xaxis_tickangle=-45
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    def render_inflation_distribution(self, data: pd.DataFrame, filters: dict):
        """Render inflation rate distribution analysis"""
        
        filtered_data = data[
            (data['country_code'].isin(filters.get('countries', []))) &
            (data['year'].between(filters.get('year_range', (2000, 2023))[0], 
                                 filters.get('year_range', (2000, 2023))[1]))
        ]
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Histogram of inflation rates
            fig_hist = px.histogram(
                filtered_data,
                x='inflation_rate',
                nbins=30,
                title="Distribution of Inflation Rates",
                labels={'inflation_rate': 'Inflation Rate', 'count': 'Frequency'},
                color_discrete_sequence=['skyblue']
            )
            
            fig_hist.update_xaxis(tickformat='.1%')
            fig_hist.update_layout(height=400)
            st.plotly_chart(fig_hist, use_container_width=True)
        
        with col2:
            # Box plot by category
            fig_box = px.box(
                filtered_data,
                x='inflation_category',
                y='inflation_rate',
                title="Inflation Rates by Category",
                labels={'inflation_rate': 'Inflation Rate', 'inflation_category': 'Category'}
            )
            
            fig_box.update_yaxis(tickformat='.1%')
            fig_box.update_layout(height=400)
            st.plotly_chart(fig_box, use_container_width=True)
    
    def render_inflation_volatility(self, data: pd.DataFrame, filters: dict):
        """Analyze inflation volatility by country"""
        
        filtered_data = data[
            (data['country_code'].isin(filters.get('countries', []))) &
            (data['year'].between(filters.get('year_range', (2000, 2023))[0], 
                                 filters.get('year_range', (2000, 2023))[1]))
        ]
        
        # Calculate volatility metrics
        volatility_stats = filtered_data.groupby(['country_code', 'country_name']).agg({
            'inflation_rate': ['mean', 'std', 'min', 'max']
        }).round(4)
        
        volatility_stats.columns = ['Mean', 'Std Dev', 'Min', 'Max']
        volatility_stats = volatility_stats.reset_index()
        
        # Create volatility chart
        fig = px.scatter(
            volatility_stats,
            x='Mean',
            y='Std Dev',
            size='Max',
            hover_name='country_name',
            title="Inflation Volatility Analysis (Mean vs Standard Deviation)",
            labels={
                'Mean': 'Average Inflation Rate',
                'Std Dev': 'Inflation Volatility (Std Dev)',
                'Max': 'Maximum Inflation'
            }
        )
        
        fig.update_xaxis(tickformat='.1%')
        fig.update_yaxis(tickformat='.1%')
        fig.update_layout(height=500)
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Display volatility table
        st.markdown("### 📊 Inflation Statistics by Country")
        
        # Format the dataframe for display
        display_stats = volatility_stats.copy()
        for col in ['Mean', 'Std Dev', 'Min', 'Max']:
            display_stats[col] = display_stats[col].apply(lambda x: f"{x:.2%}")
        
        st.dataframe(display_stats, use_container_width=True)
    
    def render_comparative_analysis(self, data: pd.DataFrame, filters: dict):
        """Render comparative analysis between countries"""
        
        if len(filters.get('countries', [])) < 2:
            st.warning("Please select at least 2 countries for comparative analysis")
            return
        
        filtered_data = data[
            (data['country_code'].isin(filters.get('countries', []))) &
            (data['year'].between(filters.get('year_range', (2000, 2023))[0], 
                                 filters.get('year_range', (2000, 2023))[1]))
        ]
        
        # Time series comparison
        fig_ts = px.line(
            filtered_data[filtered_data['inflation_indicator'] == 'CPI'].groupby(['year', 'country_code', 'country_name'])['inflation_rate'].mean().reset_index(),
            x='year',
            y='inflation_rate',
            color='country_name',
            title="Inflation Rate Trends Comparison (CPI)",
            labels={'inflation_rate': 'Inflation Rate', 'year': 'Year'}
        )
        
        fig_ts.update_yaxis(tickformat='.1%')
        fig_ts.update_layout(height=500)
        
        st.plotly_chart(fig_ts, use_container_width=True)
        
        # Statistical comparison
        st.markdown("### 📈 Statistical Comparison")
        
        comparison_stats = filtered_data.groupby('country_name').agg({
            'inflation_rate': ['count', 'mean', 'std', 'min', 'max']
        }).round(4)
        
        comparison_stats.columns = ['Data Points', 'Average', 'Volatility', 'Minimum', 'Maximum']
        comparison_stats = comparison_stats.reset_index()
        
        # Format percentages
        for col in ['Average', 'Volatility', 'Minimum', 'Maximum']:
            comparison_stats[f'{col}_formatted'] = comparison_stats[col].apply(lambda x: f"{x:.2%}")
        
        display_cols = ['country_name', 'Data Points'] + [f'{col}_formatted' for col in ['Average', 'Volatility', 'Minimum', 'Maximum']]
        display_stats = comparison_stats[display_cols].copy()
        display_stats.columns = ['Country', 'Data Points', 'Average', 'Volatility', 'Minimum', 'Maximum']
        
        st.dataframe(display_stats, use_container_width=True)
    
    def run(self, filters: dict):
        """Run the inflation analysis page"""
        
        st.markdown("# 📈 Detailed Inflation Analysis")
        st.markdown("Deep dive into inflation patterns, trends, and volatility across countries and time periods.")
        
        # Load data
        data = self.load_inflation_data()
        
        if data.empty:
            st.error("No inflation data available")
            return
        
        # Indicator selection
        available_indicators = sorted(data['inflation_indicator'].unique())
        selected_indicator = st.selectbox(
            "Select Inflation Indicator",
            options=available_indicators,
            index=0 if 'CPI' not in available_indicators else available_indicators.index('CPI')
        )
        
        filters['indicator'] = selected_indicator
        
        # Analysis sections
        st.markdown("## 🔥 Inflation Heatmap")
        self.render_inflation_heatmap(data, filters)
        
        st.divider()
        
        st.markdown("## 📊 Distribution Analysis")
        self.render_inflation_distribution(data, filters)
        
        st.divider()
        
        st.markdown("## 📉 Volatility Analysis")
        self.render_inflation_volatility(data, filters)
        
        st.divider()
        
        st.markdown("## 🔄 Comparative Analysis")
        self.render_comparative_analysis(data, filters)
