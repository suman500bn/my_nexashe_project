import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
import sys
import os
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import Config
from src.processors.database_loader import DatabaseLoader

# Configure Streamlit page
st.set_page_config(
    page_title="Economic Data Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main > div {
        padding-top: 2rem;
    }
    .stMetric {
        background-color: #f0f2f6;
        border: 1px solid #e0e0e0;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    .dashboard-header {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        padding: 2rem;
        border-radius: 1rem;
        color: white;
        margin-bottom: 2rem;
    }
    .filter-section {
        background-color: #f8f9fa;
        padding: 1.5rem;
        border-radius: 0.5rem;
        margin-bottom: 1rem;
    }
</style>
""", unsafe_allow_html=True)

class EconomicDashboard:
    """Main dashboard class for economic data visualization"""
    
    def __init__(self):
        self.config = Config()
        self.db_loader = None
        self.data_cache = {}
        self._initialize_database()
    
    def _initialize_database(self):
        """Initialize database connection"""
        try:
            db_config = {
                'host': self.config.DB_HOST,
                'port': self.config.DB_PORT,
                'database': self.config.DB_NAME,
                'user': self.config.DB_USER,
                'password': self.config.DB_PASSWORD
            }
            
            if self.config.DB_PASSWORD:
                self.db_loader = DatabaseLoader(db_config)
                st.success("✅ Connected to PostgreSQL database")
            else:
                st.warning("⚠️ No database configuration found. Using sample data.")
                
        except Exception as e:
            st.error(f"❌ Database connection failed: {str(e)}")
            st.info("Using sample data for demonstration")
    
    @st.cache_data(ttl=300)  # Cache for 5 minutes
    def load_data(_self, table_name: str, query: str = None) -> pd.DataFrame:
        """Load data from database or return sample data"""
        
        if _self.db_loader and query:
            try:
                return _self.db_loader.execute_query(query)
            except Exception as e:
                st.error(f"Database query failed: {str(e)}")
        
        # Return sample data if database is not available
        return _self._get_sample_data(table_name)
    
    def _get_sample_data(self, table_name: str) -> pd.DataFrame:
        """Generate sample data for demonstration"""
        
        np.random.seed(42)  # For reproducible sample data
        
        if table_name == 'inflation_rates':
            countries = ['USA', 'GBR', 'DEU', 'FRA', 'JPN', 'CAN', 'AUS', 'ITA', 'ESP', 'NLD']
            country_names = {
                'USA': 'United States', 'GBR': 'United Kingdom', 'DEU': 'Germany',
                'FRA': 'France', 'JPN': 'Japan', 'CAN': 'Canada', 'AUS': 'Australia',
                'ITA': 'Italy', 'ESP': 'Spain', 'NLD': 'Netherlands'
            }
            years = list(range(2010, 2024))
            
            data = []
            for country in countries:
                base_inflation = np.random.uniform(0.01, 0.04)
                for year in years:
                    # Add some variation and trends
                    if year >= 2020:
                        inflation = base_inflation + np.random.uniform(-0.01, 0.08)  # Higher recent inflation
                    else:
                        inflation = base_inflation + np.random.uniform(-0.015, 0.02)
                    
                    data.append({
                        'country_code': country,
                        'country_name': country_names[country],
                        'year': year,
                        'inflation_rate': max(inflation, -0.02),  # Minimum deflation
                        'inflation_category': _self._categorize_inflation(inflation)
                    })
            
            return pd.DataFrame(data)
        
        elif table_name == 'wage_index':
            countries = ['USA', 'GBR', 'DEU', 'FRA', 'JPN', 'CAN', 'AUS']
            country_names = {
                'USA': 'United States', 'GBR': 'United Kingdom', 'DEU': 'Germany',
                'FRA': 'France', 'JPN': 'Japan', 'CAN': 'Canada', 'AUS': 'Australia'
            }
            years = list(range(2010, 2024))
            
            data = []
            for country in countries:
                base_wage = np.random.uniform(35000, 65000)
                for year in years:
                    wage_growth = 1 + (year - 2010) * 0.025 + np.random.uniform(-0.05, 0.08)
                    avg_wage = base_wage * wage_growth
                    
                    data.append({
                        'country_code': country,
                        'country_name': country_names[country],
                        'year': year,
                        'avg_annual_wage': avg_wage,
                        'real_wage_index': 100 * wage_growth + np.random.uniform(-5, 5),
                        'unit_labour_cost': 100 + np.random.uniform(-10, 20)
                    })
            
            return pd.DataFrame(data)
        
        elif table_name == 'commodity_prices':
            commodities = ['Crude Oil', 'Gold', 'Wheat', 'Aluminum', 'Copper', 'Natural Gas']
            years = list(range(2010, 2024))
            
            base_prices = {
                'Crude Oil': 70, 'Gold': 1500, 'Wheat': 200,
                'Aluminum': 1800, 'Copper': 6000, 'Natural Gas': 3
            }
            
            data = []
            for commodity in commodities:
                base_price = base_prices[commodity]
                for year in years:
                    # Add trend and volatility
                    trend = 1 + (year - 2010) * 0.02
                    volatility = np.random.uniform(-0.3, 0.3)
                    price = base_price * trend * (1 + volatility)
                    
                    data.append({
                        'year': year,
                        'commodity': commodity,
                        'avg_annual_price': max(price, base_price * 0.3)  # Minimum price floor
                    })
            
            return pd.DataFrame(data)
        
        return pd.DataFrame()
    
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
    
    def render_header(self):
        """Render dashboard header"""
        st.markdown("""
        <div class="dashboard-header">
            <h1>📊 Economic Data Dashboard</h1>
            <p>Global Economic Indicators: Inflation, Wages, and Commodity Prices</p>
            <p><em>Real-time data from World Bank, IMF, and OECD sources</em></p>
        </div>
        """, unsafe_allow_html=True)
    
    def render_sidebar_filters(self):
        """Render sidebar filters"""
        st.sidebar.markdown("## 🔍 Dashboard Filters")
        
        # Load initial data for filter options
        inflation_data = self.load_data('inflation_rates')
        wage_data = self.load_data('wage_index')
        commodity_data = self.load_data('commodity_prices')
        
        # Country filter
        available_countries = sorted(inflation_data['country_code'].unique()) if not inflation_data.empty else ['USA', 'GBR', 'DEU']
        country_names = dict(zip(inflation_data['country_code'], inflation_data['country_name'])) if not inflation_data.empty else {
            'USA': 'United States', 'GBR': 'United Kingdom', 'DEU': 'Germany'
        }
        
        selected_countries = st.sidebar.multiselect(
            "🌍 Select Countries",
            options=available_countries,
            default=available_countries[:5],
            format_func=lambda x: f"{x} - {country_names.get(x, x)}"
        )
        
        # Year range filter
        available_years = sorted(inflation_data['year'].unique()) if not inflation_data.empty else list(range(2010, 2024))
        year_range = st.sidebar.slider(
            "📅 Year Range",
            min_value=min(available_years),
            max_value=max(available_years),
            value=(min(available_years), max(available_years)),
            step=1
        )
        
        # Sector/Commodity filter
        available_commodities = sorted(commodity_data['commodity'].unique()) if not commodity_data.empty else ['Crude Oil', 'Gold', 'Wheat']
        selected_commodities = st.sidebar.multiselect(
            "🏭 Select Commodities",
            options=available_commodities,
            default=available_commodities[:4]
        )
        
        # Additional filters
        st.sidebar.markdown("### 📈 Display Options")
        
        show_trend_lines = st.sidebar.checkbox("Show Trend Lines", value=True)
        chart_theme = st.sidebar.selectbox(
            "Chart Theme",
            options=["plotly", "plotly_white", "plotly_dark", "ggplot2"],
            index=1
        )
        
        return {
            'countries': selected_countries,
            'year_range': year_range,
            'commodities': selected_commodities,
            'show_trends': show_trend_lines,
            'theme': chart_theme
        }
    
    def render_key_metrics(self, filters):
        """Render key performance indicators"""
        st.markdown("## 📊 Key Economic Indicators")
        
        # Load current data
        inflation_data = self.load_data('inflation_rates')
        wage_data = self.load_data('wage_index')
        commodity_data = self.load_data('commodity_prices')
        
        # Filter data
        if not inflation_data.empty:
            filtered_inflation = inflation_data[
                (inflation_data['country_code'].isin(filters['countries'])) &
                (inflation_data['year'].between(filters['year_range'][0], filters['year_range'][1]))
            ]
        else:
            filtered_inflation = pd.DataFrame()
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            if not filtered_inflation.empty:
                avg_inflation = filtered_inflation['inflation_rate'].mean()
                st.metric(
                    "Average Inflation Rate",
                    f"{avg_inflation:.2%}",
                    delta=f"{(avg_inflation - 0.02):.2%}" if avg_inflation > 0.02 else None
                )
            else:
                st.metric("Average Inflation Rate", "2.45%", "0.45%")
        
        with col2:
            if not wage_data.empty:
                filtered_wages = wage_data[
                    (wage_data['country_code'].isin(filters['countries'])) &
                    (wage_data['year'].between(filters['year_range'][0], filters['year_range'][1]))
                ]
                avg_wage = filtered_wages['avg_annual_wage'].mean() if not filtered_wages.empty else 48500
                st.metric(
                    "Average Annual Wage",
                    f"${avg_wage:,.0f}",
                    delta="3.2%"
                )
            else:
                st.metric("Average Annual Wage", "$48,500", "3.2%")
        
        with col3:
            countries_count = len(filters['countries'])
            st.metric(
                "Countries Analyzed",
                f"{countries_count}",
                delta=None
            )
        
        with col4:
            years_span = filters['year_range'][1] - filters['year_range'][0] + 1
            st.metric(
                "Years of Data",
                f"{years_span}",
                delta=None
            )
    
    def render_global_inflation_map(self, filters):
        """Render global inflation visualization"""
        st.markdown("## 🌍 Global Inflation Analysis")
        
        # Load and filter data
        inflation_data = self.load_data('inflation_rates')
        
        if inflation_data.empty:
            st.warning("No inflation data available")
            return
        
        filtered_data = inflation_data[
            (inflation_data['country_code'].isin(filters['countries'])) &
            (inflation_data['year'].between(filters['year_range'][0], filters['year_range'][1]))
        ]
        
        # Create tabs for different views
        tab1, tab2 = st.tabs(["📊 Chart View", "🗺️ Geographic View"])
        
        with tab1:
            # Aggregate data by year
            yearly_inflation = filtered_data.groupby(['year', 'country_code', 'country_name'])['inflation_rate'].mean().reset_index()
            
            # Create animated bar chart
            fig = px.bar(
                yearly_inflation,
                x='country_code',
                y='inflation_rate',
                color='inflation_rate',
                animation_frame='year',
                title="Inflation Rates by Country Over Time",
                labels={
                    'inflation_rate': 'Inflation Rate (%)',
                    'country_code': 'Country',
                    'year': 'Year'
                },
                color_continuous_scale='RdYlBu_r',
                template=filters['theme']
            )
            
            fig.update_layout(
                height=500,
                xaxis_tickangle=-45,
                coloraxis_colorbar=dict(title="Inflation Rate")
            )
            
            # Format y-axis as percentage
            fig.update_yaxis(tickformat='.1%')
            
            st.plotly_chart(fig, use_container_width=True)
        
        with tab2:
            # Create heatmap/choropleth-style visualization
            avg_inflation = filtered_data.groupby('country_code')['inflation_rate'].mean().reset_index()
            avg_inflation = avg_inflation.merge(
                filtered_data[['country_code', 'country_name']].drop_duplicates(),
                on='country_code'
            )
            
            fig = px.treemap(
                avg_inflation,
                path=['country_name'],
                values='inflation_rate',
                title="Average Inflation Rates by Country (Treemap)",
                color='inflation_rate',
                color_continuous_scale='RdYlBu_r',
                template=filters['theme']
            )
            
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)
    
    def render_inflation_wage_scatter(self, filters):
        """Render inflation vs wage scatter plot"""
        st.markdown("## 💰 Inflation vs. Wage Analysis")
        
        # Load data
        inflation_data = self.load_data('inflation_rates')
        wage_data = self.load_data('wage_index')
        
        if inflation_data.empty or wage_data.empty:
            st.warning("Insufficient data for correlation analysis")
            return
        
        # Filter and merge data
        filtered_inflation = inflation_data[
            (inflation_data['country_code'].isin(filters['countries'])) &
            (inflation_data['year'].between(filters['year_range'][0], filters['year_range'][1]))
        ]
        
        filtered_wages = wage_data[
            (wage_data['country_code'].isin(filters['countries'])) &
            (wage_data['year'].between(filters['year_range'][0], filters['year_range'][1]))
        ]
        
        # Merge datasets
        merged_data = pd.merge(
            filtered_inflation.groupby(['country_code', 'year'])['inflation_rate'].mean().reset_index(),
            filtered_wages.groupby(['country_code', 'year'])['avg_annual_wage'].mean().reset_index(),
            on=['country_code', 'year'],
            how='inner'
        )
        
        if merged_data.empty:
            st.warning("No matching data for correlation analysis")
            return
        
        # Add country names
        country_names = dict(zip(inflation_data['country_code'], inflation_data['country_name']))
        merged_data['country_name'] = merged_data['country_code'].map(country_names)
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Scatter plot
            fig = px.scatter(
                merged_data,
                x='inflation_rate',
                y='avg_annual_wage',
                color='country_code',
                size='year',
                hover_data=['country_name', 'year'],
                title="Inflation Rate vs. Average Wage",
                labels={
                    'inflation_rate': 'Inflation Rate (%)',
                    'avg_annual_wage': 'Average Annual Wage ($)',
                    'country_code': 'Country'
                },
                template=filters['theme']
            )
            
            # Add trend line if requested
            if filters['show_trends']:
                fig.add_trace(
                    go.Scatter(
                        x=merged_data['inflation_rate'],
                        y=np.poly1d(np.polyfit(merged_data['inflation_rate'], merged_data['avg_annual_wage'], 1))(merged_data['inflation_rate']),
                        mode='lines',
                        name='Trend Line',
                        line=dict(color='red', dash='dash')
                    )
                )
            
            fig.update_xaxis(tickformat='.1%')
            fig.update_yaxis(tickformat='$,.0f')
            fig.update_layout(height=400)
            
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Correlation matrix
            correlation_data = merged_data[['inflation_rate', 'avg_annual_wage']].corr()
            
            fig_corr = px.imshow(
                correlation_data,
                text_auto=True,
                aspect="auto",
                title="Inflation-Wage Correlation Matrix",
                template=filters['theme']
            )
            
            fig_corr.update_layout(height=400)
            st.plotly_chart(fig_corr, use_container_width=True)
            
            # Display correlation coefficient
            corr_coef = merged_data['inflation_rate'].corr(merged_data['avg_annual_wage'])
            st.metric(
                "Correlation Coefficient",
                f"{corr_coef:.3f}",
                delta="Strong" if abs(corr_coef) > 0.7 else "Moderate" if abs(corr_coef) > 0.4 else "Weak"
            )
    
    def render_commodity_trends(self, filters):
        """Render commodity price trend analysis"""
        st.markdown("## 📈 Commodity Price Trends")
        
        # Load commodity data
        commodity_data = self.load_data('commodity_prices')
        
        if commodity_data.empty:
            st.warning("No commodity data available")
            return
        
        # Filter data
        filtered_commodities = commodity_data[
            (commodity_data['commodity'].isin(filters['commodities'])) &
            (commodity_data['year'].between(filters['year_range'][0], filters['year_range'][1]))
        ]
        
        if filtered_commodities.empty:
            st.warning("No data available for selected commodities and time range")
            return
        
        # Create subplots for multiple commodities
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=filters['commodities'][:4],
            specs=[[{"secondary_y": False}, {"secondary_y": False}],
                   [{"secondary_y": False}, {"secondary_y": False}]]
        )
        
        colors = px.colors.qualitative.Set1
        
        for i, commodity in enumerate(filters['commodities'][:4]):
            row = (i // 2) + 1
            col = (i % 2) + 1
            
            commodity_subset = filtered_commodities[filtered_commodities['commodity'] == commodity]
            
            fig.add_trace(
                go.Scatter(
                    x=commodity_subset['year'],
                    y=commodity_subset['avg_annual_price'],
                    mode='lines+markers',
                    name=commodity,
                    line=dict(color=colors[i % len(colors)]),
                    showlegend=False
                ),
                row=row, col=col
            )
            
            # Add trend line if requested
            if filters['show_trends'] and len(commodity_subset) > 1:
                z = np.polyfit(commodity_subset['year'], commodity_subset['avg_annual_price'], 1)
                trend_line = np.poly1d(z)(commodity_subset['year'])
                
                fig.add_trace(
                    go.Scatter(
                        x=commodity_subset['year'],
                        y=trend_line,
                        mode='lines',
                        name=f'{commodity} Trend',
                        line=dict(color=colors[i % len(colors)], dash='dash'),
                        showlegend=False
                    ),
                    row=row, col=col
                )
        
        fig.update_layout(
            height=600,
            title_text="Commodity Price Trends Over Time",
            template=filters['theme']
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Summary statistics
        st.markdown("### 📊 Commodity Summary Statistics")
        
        summary_stats = []
        for commodity in filters['commodities']:
            commodity_subset = filtered_commodities[filtered_commodities['commodity'] == commodity]
            if not commodity_subset.empty:
                stats = {
                    'Commodity': commodity,
                    'Avg Price': f"${commodity_subset['avg_annual_price'].mean():.2f}",
                    'Min Price': f"${commodity_subset['avg_annual_price'].min():.2f}",
                    'Max Price': f"${commodity_subset['avg_annual_price'].max():.2f}",
                    'Volatility': f"{commodity_subset['avg_annual_price'].std():.2f}",
                    'Trend': "📈" if commodity_subset['avg_annual_price'].iloc[-1] > commodity_subset['avg_annual_price'].iloc[0] else "📉"
                }
                summary_stats.append(stats)
        
        if summary_stats:
            st.dataframe(pd.DataFrame(summary_stats), use_container_width=True)
    
    def render_data_table(self, filters):
        """Render raw data table"""
        st.markdown("## 📋 Raw Data Explorer")
        
        data_type = st.selectbox(
            "Select Data Type",
            ["Inflation Rates", "Wage Index", "Commodity Prices"]
        )
        
        if data_type == "Inflation Rates":
            data = self.load_data('inflation_rates')
            if not data.empty:
                filtered_data = data[
                    (data['country_code'].isin(filters['countries'])) &
                    (data['year'].between(filters['year_range'][0], filters['year_range'][1]))
                ]
        elif data_type == "Wage Index":
            data = self.load_data('wage_index')
            if not data.empty:
                filtered_data = data[
                    (data['country_code'].isin(filters['countries'])) &
                    (data['year'].between(filters['year_range'][0], filters['year_range'][1]))
                ]
        else:  # Commodity Prices
            data = self.load_data('commodity_prices')
            if not data.empty:
                filtered_data = data[
                    (data['commodity'].isin(filters['commodities'])) &
                    (data['year'].between(filters['year_range'][0], filters['year_range'][1]))
                ]
        
        if not data.empty and not filtered_data.empty:
            st.dataframe(filtered_data, use_container_width=True)
            
            # Download button
            csv = filtered_data.to_csv(index=False)
            st.download_button(
                label="📥 Download CSV",
                data=csv,
                file_name=f"{data_type.lower().replace(' ', '_')}_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )
        else:
            st.warning("No data available for the selected filters")
    
    def run(self):
        """Main dashboard application"""
        
        # Render header
        self.render_header()
        
        # Render sidebar filters
        filters = self.render_sidebar_filters()
        
        # Main content
        if not filters['countries']:
            st.warning("⚠️ Please select at least one country to display data")
            return
        
        # Key metrics
        self.render_key_metrics(filters)
        
        # Main visualizations
        self.render_global_inflation_map(filters)
        
        st.divider()
        
        self.render_inflation_wage_scatter(filters)
        
        st.divider()
        
        self.render_commodity_trends(filters)
        
        st.divider()
        
        # Data explorer
        with st.expander("🔍 Data Explorer", expanded=False):
            self.render_data_table(filters)
        
        # Footer
        st.markdown("---")
        st.markdown("""
        <div style='text-align: center; color: #666; padding: 2rem;'>
            <p>📊 <strong>Economic Data Dashboard</strong> | 
            Data sources: World Bank, IMF, OECD | 
            Built with Streamlit & Plotly</p>
            <p><em>Last updated: {}</em></p>
        </div>
        """.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")), unsafe_allow_html=True)

def main():
    """Run the dashboard application"""
    dashboard = EconomicDashboard()
    dashboard.run()

if __name__ == "__main__":
    main()
