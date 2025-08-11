#!/bin/bash

# Dashboard Launcher Script
# Installs dashboard dependencies and launches the Streamlit app

echo "📊 Economic Data Dashboard Launcher"
echo "=================================="

# Check if virtual environment exists
if [ ! -d ".venv" ]; then
    echo "❌ Virtual environment not found. Please run setup.py first."
    exit 1
fi

# Install dashboard-specific dependencies
echo "📦 Installing dashboard dependencies..."
.venv/bin/python -m pip install -r dashboard_requirements.txt

# Set environment variables for proper Spark integration
export PYSPARK_PYTHON=".venv/bin/python"
export PYSPARK_DRIVER_PYTHON=".venv/bin/python"

# Launch Streamlit dashboard
echo "🚀 Launching Economic Data Dashboard..."
echo "📱 The dashboard will open in your web browser at http://localhost:8501"
echo "⏹️  Press Ctrl+C to stop the dashboard"
echo ""

.venv/bin/python -m streamlit run dashboard/main.py --server.port 8501 --server.address localhost
