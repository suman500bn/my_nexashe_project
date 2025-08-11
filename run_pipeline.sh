#!/bin/bash

# Economic Data Pipeline Launcher
# Sets proper environment variables and runs the main pipeline

echo "🚀 Starting Economic Data Pipeline"
echo "=================================="

# Set up Spark Python environment to use the virtual environment
export PYSPARK_PYTHON="/Users/sumkalap/eclipse-workspace/nexashe_project/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="/Users/sumkalap/eclipse-workspace/nexashe_project/.venv/bin/python"

# Optional: Set Java options for better performance
export JAVA_OPTS="-Xms2g -Xmx4g"

# Run the pipeline
echo "📊 Launching pipeline with proper environment..."
/Users/sumkalap/eclipse-workspace/nexashe_project/.venv/bin/python main.py

echo "✅ Pipeline execution completed!"
echo "📂 Check the data/ directory for results"
echo "📋 Review pipeline.log for detailed execution logs"
