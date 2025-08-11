import streamlit as st
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import dashboard components
from dashboard.app import EconomicDashboard
from dashboard.pages.inflation_analysis import InflationAnalysis

def main():
    """Main multi-page dashboard application"""
    
    # Page configuration
    st.set_page_config(
        page_title="Economic Data Dashboard",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Initialize dashboard components
    main_dashboard = EconomicDashboard()
    inflation_analysis = InflationAnalysis()
    
    # Sidebar navigation
    st.sidebar.markdown("# 🏠 Navigation")
    page = st.sidebar.selectbox(
        "Choose a page:",
        ["📊 Main Dashboard", "📈 Inflation Analysis", "💰 Wage Analysis", "🏭 Commodity Analysis"]
    )
    
    # Common filters for all pages
    st.sidebar.markdown("---")
    filters = main_dashboard.render_sidebar_filters()
    
    # Page routing
    if page == "📊 Main Dashboard":
        main_dashboard.run()
    
    elif page == "📈 Inflation Analysis":
        inflation_analysis.run(filters)
    
    elif page == "💰 Wage Analysis":
        st.markdown("# 💰 Wage Analysis")
        st.info("🚧 This page is under construction. Coming soon!")
        st.markdown("### Planned Features:")
        st.markdown("""
        - Real wage trends analysis
        - Wage growth vs inflation comparison
        - Purchasing power analysis
        - Labor market indicators
        - Regional wage disparities
        """)
    
    elif page == "🏭 Commodity Analysis":
        st.markdown("# 🏭 Commodity Analysis")
        st.info("🚧 This page is under construction. Coming soon!")
        st.markdown("### Planned Features:")
        st.markdown("""
        - Commodity price volatility analysis
        - Seasonal patterns detection
        - Correlation analysis between commodities
        - Supply and demand indicators
        - Price forecasting models
        """)

if __name__ == "__main__":
    main()
