#!/usr/bin/env python3
"""Test Azure Storage connectivity directly."""

from azure.storage.blob import BlobServiceClient
import os

# Azure credentials
storage_account = "agentstge"
container = "raw-data"

# Using connection string approach (more reliable)
connection_string = f"DefaultEndpointsProtocol=https;AccountName={storage_account};AccountKey=YOUR_KEY;EndpointSuffix=core.windows.net"

print("🔍 TESTING AZURE STORAGE CONNECTIVITY")
print("=" * 60)
print(f"Storage Account: {storage_account}")
print(f"Container: {container}")

try:
    # Create BlobServiceClient
    service_client = BlobServiceClient(
        account_url=f"https://{storage_account}.blob.core.windows.net/",
        credential=None  # Using anonymous access for public containers
    )
    
    # Get container client
    container_client = service_client.get_container_client(container)
    
    # List blobs
    print("\n📁 Files in container:")
    blob_count = 0
    for blob in container_client.list_blobs():
        if blob_count < 10:  # Show first 10
            print(f"   - {blob.name} ({blob.size} bytes)")
        blob_count += 1
    
    print(f"\n✅ Total files found: {blob_count}")
    
    # Create a test file
    test_data = "product_id,product_name,category,price\nTEST001,Test Product,Test,99.99"
    blob_name = f"test/azure_connectivity_test_{os.getpid()}.csv"
    
    try:
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(test_data, overwrite=True)
        print(f"\n✅ Successfully created test file: {blob_name}")
    except Exception as e:
        print(f"\n⚠️ Could not create test file (expected if read-only): {e}")
    
except Exception as e:
    print(f"\n❌ Error: {e}")
    print("\nThis might be due to:")
    print("1. Storage account requires authentication")
    print("2. Network connectivity issues")
    print("3. Container doesn't exist or is private")