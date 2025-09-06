"""
Create a large example datasets.csv file with 100 datasets to demonstrate scalability.
"""

import csv
import random
from typing import List

def create_large_datasets_csv(output_path: str = "datasets_100_example.csv"):
    """Create a CSV file with 100 example datasets."""
    
    # Sample data for generating realistic dataset names and descriptions
    business_areas = [
        "customer", "product", "order", "inventory", "financial", "user", "transaction",
        "supplier", "warehouse", "shipping", "payment", "billing", "analytics", "reporting",
        "audit", "compliance", "security", "notification", "preference", "subscription"
    ]
    
    data_types = [
        "master", "transaction", "reference", "log", "archive", "staging", "dimension",
        "fact", "summary", "aggregate", "raw", "processed", "validated", "cleansed"
    ]
    
    environments = ["prod", "staging", "dev", "test", "backup"]
    
    # Generate 100 datasets
    datasets = []
    
    for i in range(1, 101):
        # Generate dataset name
        business_area = random.choice(business_areas)
        data_type = random.choice(data_types)
        env = random.choice(environments)
        
        dataset_name = f"{business_area}_{data_type}_{env}_{i:03d}"
        description = f"{business_area.title()} {data_type} data comparison for {env} environment"
        
        # Generate table and S3 key
        sql_table = f"{business_area}_{data_type}_{env}"
        s3_key = f"data/{business_area}/{data_type}_{env}.parquet"
        
        # Randomly assign some overrides (about 20% of datasets)
        overrides = {}
        if random.random() < 0.2:
            if random.random() < 0.5:
                overrides['chunk_size_override'] = str(random.choice([1000000, 2000000, 5000000]))
            if random.random() < 0.5:
                overrides['max_parallelism_override'] = str(random.choice([4, 8, 16, 32]))
            if random.random() < 0.3:
                overrides['sample_size_override'] = str(random.choice([50000, 100000, 200000]))
            if random.random() < 0.4:
                overrides['enable_full_comparison'] = 'false'
        
        # Create dataset row
        dataset = {
            'name': dataset_name,
            'description': description,
            'sql_server_table': sql_table,
            's3_parquet_key': s3_key,
            'chunk_size_override': overrides.get('chunk_size_override', ''),
            'max_parallelism_override': overrides.get('max_parallelism_override', ''),
            'enable_metadata_comparison': 'true',
            'enable_fingerprinting': 'true',
            'enable_sampling': 'true',
            'enable_full_comparison': overrides.get('enable_full_comparison', 'true'),
            'sample_size_override': overrides.get('sample_size_override', ''),
            'notes': f"Generated dataset #{i} - {env} environment"
        }
        
        datasets.append(dataset)
    
    # Write CSV file
    fieldnames = [
        'name', 'description', 'sql_server_table', 's3_parquet_key',
        'chunk_size_override', 'max_parallelism_override',
        'enable_metadata_comparison', 'enable_fingerprinting',
        'enable_sampling', 'enable_full_comparison', 'sample_size_override', 'notes'
    ]
    
    with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(datasets)
    
    print(f"Created {len(datasets)} datasets in {output_path}")
    
    # Show statistics
    with_overrides = sum(1 for d in datasets if any(d[f] for f in ['chunk_size_override', 'max_parallelism_override', 'sample_size_override']))
    without_full_comparison = sum(1 for d in datasets if d['enable_full_comparison'] == 'false')
    
    print(f"Statistics:")
    print(f"  Total datasets: {len(datasets)}")
    print(f"  Datasets with overrides: {with_overrides}")
    print(f"  Datasets without full comparison: {without_full_comparison}")
    print(f"  Average description length: {sum(len(d['description']) for d in datasets) / len(datasets):.1f} characters")

def create_enterprise_datasets_csv(output_path: str = "datasets_enterprise.csv"):
    """Create a CSV file with enterprise-style dataset names."""
    
    # Enterprise dataset patterns
    enterprise_datasets = [
        # Customer domain
        ("CUST_MASTER", "Customer master data", "customers", "data/customer/master.parquet"),
        ("CUST_PROFILES", "Customer profiles", "customer_profiles", "data/customer/profiles.parquet"),
        ("CUST_PREFERENCES", "Customer preferences", "customer_preferences", "data/customer/preferences.parquet"),
        ("CUST_ADDRESSES", "Customer addresses", "customer_addresses", "data/customer/addresses.parquet"),
        ("CUST_CONTACTS", "Customer contacts", "customer_contacts", "data/customer/contacts.parquet"),
        
        # Product domain
        ("PROD_CATALOG", "Product catalog", "products", "data/product/catalog.parquet"),
        ("PROD_CATEGORIES", "Product categories", "product_categories", "data/product/categories.parquet"),
        ("PROD_ATTRIBUTES", "Product attributes", "product_attributes", "data/product/attributes.parquet"),
        ("PROD_PRICES", "Product pricing", "product_prices", "data/product/prices.parquet"),
        ("PROD_INVENTORY", "Product inventory", "product_inventory", "data/product/inventory.parquet"),
        
        # Order domain
        ("ORD_HEADERS", "Order headers", "order_headers", "data/order/headers.parquet"),
        ("ORD_LINES", "Order lines", "order_lines", "data/order/lines.parquet"),
        ("ORD_PAYMENTS", "Order payments", "order_payments", "data/order/payments.parquet"),
        ("ORD_SHIPPING", "Order shipping", "order_shipping", "data/order/shipping.parquet"),
        ("ORD_STATUS", "Order status", "order_status", "data/order/status.parquet"),
        
        # Financial domain
        ("FIN_TRANSACTIONS", "Financial transactions", "financial_transactions", "data/financial/transactions.parquet"),
        ("FIN_ACCOUNTS", "Financial accounts", "financial_accounts", "data/financial/accounts.parquet"),
        ("FIN_BALANCES", "Account balances", "account_balances", "data/financial/balances.parquet"),
        ("FIN_RECONCILIATION", "Financial reconciliation", "financial_reconciliation", "data/financial/reconciliation.parquet"),
        ("FIN_AUDIT", "Financial audit trail", "financial_audit", "data/financial/audit.parquet"),
        
        # Analytics domain
        ("ANALYTICS_EVENTS", "Analytics events", "analytics_events", "data/analytics/events.parquet"),
        ("ANALYTICS_METRICS", "Analytics metrics", "analytics_metrics", "data/analytics/metrics.parquet"),
        ("ANALYTICS_REPORTS", "Analytics reports", "analytics_reports", "data/analytics/reports.parquet"),
        ("ANALYTICS_DASHBOARDS", "Analytics dashboards", "analytics_dashboards", "data/analytics/dashboards.parquet"),
        ("ANALYTICS_KPIS", "Key performance indicators", "analytics_kpis", "data/analytics/kpis.parquet"),
    ]
    
    datasets = []
    
    for name, description, table, s3_key in enterprise_datasets:
        # Add some overrides for large datasets
        overrides = {}
        if "TRANSACTION" in name or "EVENTS" in name:
            overrides['chunk_size_override'] = '2000000'
            overrides['max_parallelism_override'] = '16'
            overrides['sample_size_override'] = '200000'
        elif "MASTER" in name or "CATALOG" in name:
            overrides['enable_full_comparison'] = 'false'  # Metadata only for reference data
        
        dataset = {
            'name': name,
            'description': description,
            'sql_server_table': table,
            's3_parquet_key': s3_key,
            'chunk_size_override': overrides.get('chunk_size_override', ''),
            'max_parallelism_override': overrides.get('max_parallelism_override', ''),
            'enable_metadata_comparison': 'true',
            'enable_fingerprinting': 'true',
            'enable_sampling': 'true',
            'enable_full_comparison': overrides.get('enable_full_comparison', 'true'),
            'sample_size_override': overrides.get('sample_size_override', ''),
            'notes': f"Enterprise {name.split('_')[0].lower()} domain dataset"
        }
        
        datasets.append(dataset)
    
    # Write CSV file
    fieldnames = [
        'name', 'description', 'sql_server_table', 's3_parquet_key',
        'chunk_size_override', 'max_parallelism_override',
        'enable_metadata_comparison', 'enable_fingerprinting',
        'enable_sampling', 'enable_full_comparison', 'sample_size_override', 'notes'
    ]
    
    with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(datasets)
    
    print(f"Created {len(datasets)} enterprise datasets in {output_path}")

if __name__ == "__main__":
    print("Creating large datasets examples...")
    
    # Create 100 random datasets
    create_large_datasets_csv("datasets_100_example.csv")
    
    # Create enterprise-style datasets
    create_enterprise_datasets_csv("datasets_enterprise.csv")
    
    print("\nExample files created:")
    print("  - datasets_100_example.csv (100 random datasets)")
    print("  - datasets_enterprise.csv (25 enterprise datasets)")
    print("\nYou can use these as templates for your own large-scale configurations.")
