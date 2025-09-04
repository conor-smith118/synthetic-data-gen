#!/usr/bin/env python3
"""
Test script to verify volume setup for synthetic data generation.
This script helps ensure the Databricks volume is accessible and writable.
"""

import os
import tempfile
from datetime import datetime

def test_volume_access():
    """Test if we can access and write to the Databricks volume."""
    volume_path = "/Volumes/conor_smith/synthetic_data_app/synthetic_data_volume"
    
    print("🧪 Testing Databricks Volume Access")
    print(f"Volume path: {volume_path}")
    print("-" * 50)
    
    # Test 1: Check if volume path exists or can be created
    try:
        if os.path.exists(volume_path):
            print("✅ Volume path exists")
        else:
            print("⚠️  Volume path doesn't exist, attempting to create...")
            os.makedirs(volume_path, exist_ok=True)
            print("✅ Volume path created successfully")
    except Exception as e:
        print(f"❌ Cannot access/create volume path: {str(e)}")
        return False
    
    # Test 2: Test write permissions
    try:
        test_filename = f"test_file_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        test_filepath = os.path.join(volume_path, test_filename)
        
        with open(test_filepath, 'w') as f:
            f.write("This is a test file for volume access verification.")
        
        print("✅ Write test successful")
        
        # Test 3: Test read permissions
        with open(test_filepath, 'r') as f:
            content = f.read()
        
        print("✅ Read test successful")
        
        # Test 4: Clean up test file
        os.remove(test_filepath)
        print("✅ File cleanup successful")
        
        return True
        
    except Exception as e:
        print(f"❌ Volume access test failed: {str(e)}")
        return False

def suggest_alternatives():
    """Suggest alternative setups if volume access fails."""
    print("\n🔧 Alternative Setup Options:")
    print("-" * 30)
    print("1. Local Development:")
    print("   - Files will be saved to './generated_documents' directory")
    print("   - Manual copy to volume required")
    
    print("\n2. Databricks Environment:")
    print("   - Ensure you have access to the volume:")
    print("     /Volumes/conor_smith/synthetic_data_app/synthetic_data_volume")
    print("   - Check volume permissions in Databricks workspace")
    
    print("\n3. Manual Volume Creation:")
    print("   - In Databricks SQL/Notebooks:")
    print("     CREATE VOLUME IF NOT EXISTS conor_smith.synthetic_data_app.synthetic_data_volume;")

def main():
    print("📁 Synthetic Data Generator - Volume Setup Test")
    print("=" * 60)
    
    volume_accessible = test_volume_access()
    
    if volume_accessible:
        print("\n🎉 Volume setup successful!")
        print("Your synthetic data generator should work with automatic volume saving.")
    else:
        print("\n⚠️  Volume setup incomplete.")
        suggest_alternatives()
    
    # Also test local fallback
    print("\n📂 Testing Local Directory Fallback:")
    print("-" * 40)
    
    local_dir = "./generated_documents"
    try:
        os.makedirs(local_dir, exist_ok=True)
        test_file = os.path.join(local_dir, "test_local.txt")
        
        with open(test_file, 'w') as f:
            f.write("Local directory test")
        
        with open(test_file, 'r') as f:
            f.read()
        
        os.remove(test_file)
        print("✅ Local directory backup working")
        
    except Exception as e:
        print(f"❌ Local directory test failed: {str(e)}")

if __name__ == "__main__":
    main()
