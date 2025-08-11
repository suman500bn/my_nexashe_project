#!/usr/bin/env python3
"""
Setup script for Economic Data Pipeline

This script helps set up the development environment and verify dependencies.
"""

import os
import sys
import subprocess
import shutil
from pathlib import Path

def check_python_version():
    """Check if Python version is compatible"""
    if sys.version_info < (3, 8):
        print("❌ Python 3.8 or higher is required")
        print(f"Current version: {sys.version}")
        return False
    else:
        print(f"✅ Python version: {sys.version.split()[0]}")
        return True

def check_java():
    """Check if Java is installed for PySpark"""
    try:
        result = subprocess.run(['java', '-version'], 
                              capture_output=True, text=True)
        if result.returncode == 0:
            java_version = result.stderr.split('\n')[0]
            print(f"✅ Java found: {java_version}")
            return True
    except FileNotFoundError:
        pass
    
    print("❌ Java not found. PySpark requires Java 8 or higher")
    print("Please install Java: https://adoptium.net/")
    return False

def install_dependencies():
    """Install Python dependencies"""
    print("\n📦 Installing Python dependencies...")
    
    try:
        subprocess.check_call([
            sys.executable, '-m', 'pip', 'install', '-r', 'requirements.txt'
        ])
        print("✅ Dependencies installed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to install dependencies: {e}")
        return False

def create_env_file():
    """Create .env file from template"""
    env_example = Path('.env.example')
    env_file = Path('.env')
    
    if env_example.exists() and not env_file.exists():
        shutil.copy(env_example, env_file)
        print("✅ Created .env file from template")
        print("📝 Please edit .env with your database credentials")
    elif env_file.exists():
        print("✅ .env file already exists")
    else:
        print("❌ .env.example not found")

def verify_directory_structure():
    """Verify required directories exist"""
    required_dirs = [
        'data/raw',
        'data/processed',
        'src/data_fetchers',
        'src/processors',
        'config',
        'sql'
    ]
    
    all_exist = True
    for dir_path in required_dirs:
        if os.path.exists(dir_path):
            print(f"✅ {dir_path}")
        else:
            print(f"❌ {dir_path} - missing")
            all_exist = False
    
    return all_exist

def test_imports():
    """Test critical imports"""
    test_modules = [
        ('pyspark', 'PySpark'),
        ('pandas', 'Pandas'),
        ('requests', 'Requests'),
        ('sqlalchemy', 'SQLAlchemy')
    ]
    
    print("\n🧪 Testing imports...")
    all_imports_ok = True
    
    for module, name in test_modules:
        try:
            __import__(module)
            print(f"✅ {name}")
        except ImportError as e:
            print(f"❌ {name}: {e}")
            all_imports_ok = False
    
    return all_imports_ok

def test_spark():
    """Test Spark initialization"""
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
            .appName("SetupTest") \
            .master("local[1]") \
            .getOrCreate()
        
        # Create a simple DataFrame to test
        data = [("test", 1)]
        df = spark.createDataFrame(data, ["col1", "col2"])
        count = df.count()
        
        spark.stop()
        
        if count == 1:
            print("✅ Spark test successful")
            return True
        else:
            print("❌ Spark test failed")
            return False
            
    except Exception as e:
        print(f"❌ Spark test failed: {e}")
        return False

def check_postgresql():
    """Check PostgreSQL availability (optional)"""
    try:
        import psycopg2
        print("✅ psycopg2 (PostgreSQL driver) available")
        
        # Try to connect if credentials are provided
        from config.settings import Config
        config = Config()
        
        if config.DB_PASSWORD:  # Only try if password is set
            try:
                import psycopg2
                conn = psycopg2.connect(
                    host=config.DB_HOST,
                    port=config.DB_PORT,
                    database=config.DB_NAME,
                    user=config.DB_USER,
                    password=config.DB_PASSWORD
                )
                conn.close()
                print("✅ PostgreSQL connection successful")
                return True
            except Exception as e:
                print(f"⚠️  PostgreSQL connection failed: {e}")
                print("   (This is optional - pipeline can run without database)")
        else:
            print("⚠️  No database password configured (optional)")
        
    except ImportError:
        print("❌ psycopg2 not available")
    
    return False

def main():
    """Main setup function"""
    print("🚀 Economic Data Pipeline Setup")
    print("=" * 50)
    
    # Check requirements
    checks = [
        ("Python Version", check_python_version),
        ("Java Installation", check_java),
        ("Directory Structure", verify_directory_structure),
    ]
    
    all_checks_passed = True
    for name, check_func in checks:
        print(f"\n📋 Checking {name}...")
        if not check_func():
            all_checks_passed = False
    
    if not all_checks_passed:
        print("\n❌ Some requirements are not met. Please fix the issues above.")
        return False
    
    # Install dependencies
    if not install_dependencies():
        return False
    
    # Create environment file
    create_env_file()
    
    # Test imports
    if not test_imports():
        print("\n❌ Some imports failed. Check your Python environment.")
        return False
    
    # Test Spark
    print("\n🧪 Testing Spark...")
    if not test_spark():
        return False
    
    # Test PostgreSQL (optional)
    print("\n🗄️  Checking PostgreSQL...")
    check_postgresql()
    
    print("\n🎉 Setup completed successfully!")
    print("\nNext steps:")
    print("1. Edit .env file with your database credentials (optional)")
    print("2. Run the pipeline: python main.py")
    print("3. Check the README.md for detailed usage instructions")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
