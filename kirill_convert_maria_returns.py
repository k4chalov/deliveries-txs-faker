import pandas as pd
import os
import sys
import uuid
import json
from pathlib import Path
from datetime import datetime, timezone
import random

# Set UTF-8 encoding for Windows console
if sys.platform == "win32":
    import codecs
    sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())

def read_all_csv_files():
    """Read all CSV files from maria_script folder organized by type"""
    maria_script_path = Path("maria_script")
    
    # Initialize dictionaries to store data by type
    data = {
        'line_items': {},
        'orders': {},
        'customers': {},
        'products': {},
        'variants': {},
        'refunds': {}
    }
    
    # File suffixes (A, B, C, D)
    suffixes = ['A', 'B', 'C', 'D']
    
    for suffix in suffixes:
        print(f"Reading files with suffix {suffix}...")
        
        # Read each type of CSV file
        try:
            data['line_items'][suffix] = pd.read_csv(maria_script_path / f"line_items_{suffix}.csv")
            data['orders'][suffix] = pd.read_csv(maria_script_path / f"orders_{suffix}.csv")
            data['customers'][suffix] = pd.read_csv(maria_script_path / f"customers_{suffix}.csv")
            data['products'][suffix] = pd.read_csv(maria_script_path / f"products_{suffix}.csv")
            data['variants'][suffix] = pd.read_csv(maria_script_path / f"variants_{suffix}.csv")
            data['refunds'][suffix] = pd.read_csv(maria_script_path / f"refunds_{suffix}.csv")
            print(f"  Successfully loaded all {suffix} files")
        except Exception as e:
            print(f"  Error loading {suffix} files: {e}")
    
    return data

def create_mappings(data):
    """Create mapping dictionaries for quick lookups"""
    mappings = {
        'order_to_customer': {},
        'variant_to_product': {},
        'line_item_to_order': {},
        'line_item_to_variant': {},
        'refund_to_line_item': {},
        'refund_to_order': {}
    }
    
    suffixes = ['A', 'B', 'C', 'D']
    
    for suffix in suffixes:
        # Map orders to customers
        orders_df = data['orders'][suffix]
        for _, row in orders_df.iterrows():
            mappings['order_to_customer'][row['order_id']] = row['customer_id']
        
        # Map variants to products
        variants_df = data['variants'][suffix]
        for _, row in variants_df.iterrows():
            mappings['variant_to_product'][row['variant_id']] = row['product_id']
        
        # Map line items to orders and variants
        line_items_df = data['line_items'][suffix]
        for _, row in line_items_df.iterrows():
            mappings['line_item_to_order'][row['line_item_id']] = row['order_id']
            mappings['line_item_to_variant'][row['line_item_id']] = row['variant_id']
        
        # Map refunds to line items and orders
        refunds_df = data['refunds'][suffix]
        for _, row in refunds_df.iterrows():
            mappings['refund_to_line_item'][row['refund_id']] = row['line_item_id']
            mappings['refund_to_order'][row['refund_id']] = row['order_id']
    
    return mappings

def convert_order_id_to_uuid(order_id):
    """Convert order ID like 'A_O1' or 'D_O123' to UUID format like '00000000-0000-0000-0000-a00000000001'"""
    # Extract the letter (A, B, C, D) and make it lowercase
    letter = order_id[0].lower()
    
    # Extract all digits from the order ID
    digits = ''.join(filter(str.isdigit, order_id))
    
    # Ensure we don't exceed the 12-character limit for the last part
    # Format: letter + up to 11 digits (padded with zeros if needed)
    if len(digits) > 11:
        # If more than 11 digits, take the last 11
        digits = digits[-11:]
    
    # Create the UUID suffix: letter + digits padded to 12 characters total
    uuid_suffix = letter + digits.zfill(11)
    
    # Create the full UUID
    uuid_result = f"00000000-0000-0000-0000-{uuid_suffix}"
    
    return uuid_result

def load_ordered_variants_lookup():
    """Load the ordered variants CSV and create a lookup dictionary"""
    ordered_variants_file = "kirill_convert_maria_ordered_variants.csv"
    
    if not os.path.exists(ordered_variants_file):
        raise FileNotFoundError(f"Required file {ordered_variants_file} not found. Please run kirill_convert_maria.py first.")
    
    print(f"Loading ordered variants lookup from {ordered_variants_file}...")
    df = pd.read_csv(ordered_variants_file)
    
    # Create lookup dictionary using composite key
    # Key: (order_external_id, line_item_external_id, variant_external_id, product_external_id)
    # Value: id (UUID)
    lookup = {}
    for _, row in df.iterrows():
        key = (
            row['order_external_id'],
            row['line_item_external_id'], 
            row['variant_external_id'],
            row['product_external_id']
        )
        lookup[key] = row['id']
    
    print(f"  Loaded {len(lookup)} ordered variant records for lookup")
    return lookup

def find_ordered_variant_id(lookup_dict, order_id, line_item_id, variant_id, product_id):
    """Find the ordered variant ID from the lookup dictionary"""
    key = (order_id, line_item_id, variant_id, product_id)
    
    if key in lookup_dict:
        return lookup_dict[key]
    else:
        print(f"    Warning: No ordered variant found for {key}")
        return None

def convert_date_to_datetime_with_timezone(date_str):
    """Convert date string to datetime with timezone for PostgreSQL compatibility"""
    try:
        # Parse the date (assuming format like '2024-02-06')
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        
        # Add random time component (between 00:00:00 and 23:59:59)
        hour = random.randint(0, 23)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        
        # Create datetime with time component
        datetime_obj = date_obj.replace(hour=hour, minute=minute, second=second)
        
        # Add UTC timezone
        datetime_with_tz = datetime_obj.replace(tzinfo=timezone.utc)
        
        # Return in ISO format with timezone
        return datetime_with_tz.isoformat()
        
    except ValueError:
        # If parsing fails, return current datetime with timezone
        return datetime.now(timezone.utc).isoformat()

def create_variant_attributes_json(variant):
    """Create JSONB-compatible variant attributes"""
    return json.dumps({
        "color": variant['color'],
        "size": variant['size']
    })

def create_taxes_json():
    """Create JSONB-compatible taxes structure (placeholder)"""
    return json.dumps({
        "vat_rate": 0.20,
        "tax_amount": 0.0
    })

def export_all_returned_variants_to_csv(data, mappings, output_file="kirill_convert_maria_returned_variants.csv"):
    """Export all refunded line items with related data to CSV file"""
    print(f"Exporting all returned variants to {output_file}...")
    
    # Load the ordered variants lookup
    ordered_variants_lookup = load_ordered_variants_lookup()
    
    # Define the CSV headers based on the table schema
    headers = [
        'id', 'ordered_variant_id', 'parent_order_external_id', 'refund_external_id',
        'refund_date_created', 'refund_amount', 'refund_reason', 'refunded_by',
        'refunded_payment', 'order_status', 'order_total_amount', 'order_currency',
        'order_created_at', 'order_updated_at', 'customer_email', 'customer_phone_number',
        'shipping_first_name', 'shipping_last_name', 'shipping_address_1', 'shipping_address_2',
        'shipping_city', 'shipping_state', 'shipping_postcode', 'shipping_country_code',
        'product_external_id', 'product_title', 'product_description', 'variant_external_id',
        'variant_title', 'variant_sku', 'variant_price', 'variant_attributes',
        'variant_image_id', 'variant_image_src', 'returned_line_item_external_id',
        'returned_quantity', 'returned_unit_price', 'returned_subtotal', 'returned_subtotal_tax',
        'returned_total', 'returned_total_tax', 'returned_currency', 'tax_class', 'taxes',
        'profile_id', 'group_order_id', 'created_at', 'updated_at', 'category_id', 'category'
    ]
    
    # List to store all rows
    all_rows = []
    
    # Process each dataset (A, B, C, D)
    suffixes = ['A', 'B', 'C', 'D']
    
    for suffix in suffixes:
        print(f"  Processing dataset {suffix}...")
        
        refunds_df = data['refunds'][suffix]
        line_items_df = data['line_items'][suffix]
        orders_df = data['orders'][suffix]
        customers_df = data['customers'][suffix]
        products_df = data['products'][suffix]
        variants_df = data['variants'][suffix]
        
        # Create lookup dictionaries for faster access
        # Handle potential duplicates by dropping them
        line_items_dict = line_items_df.drop_duplicates(subset=['line_item_id']).set_index('line_item_id').to_dict('index')
        orders_dict = orders_df.drop_duplicates(subset=['order_id']).set_index('order_id').to_dict('index')
        customers_dict = customers_df.drop_duplicates(subset=['customer_id']).set_index('customer_id').to_dict('index')
        products_dict = products_df.drop_duplicates(subset=['product_id']).set_index('product_id').to_dict('index')
        variants_dict = variants_df.drop_duplicates(subset=['variant_id']).set_index('variant_id').to_dict('index')
        
        # Process each refund
        for _, refund in refunds_df.iterrows():
            # Get related data
            line_item_id = refund['line_item_id']
            order_id = refund['order_id']
            
            # Check if line item exists (some refunds might reference non-existent line items)
            if line_item_id not in line_items_dict:
                print(f"    Warning: Line item {line_item_id} not found, skipping refund {refund['refund_id']}")
                continue
                
            # Check if order exists
            if order_id not in orders_dict:
                print(f"    Warning: Order {order_id} not found, skipping refund {refund['refund_id']}")
                continue
            
            line_item = line_items_dict[line_item_id]
            order = orders_dict[order_id]
            customer_id = order['customer_id']
            customer = customers_dict[customer_id]
            variant_id = line_item['variant_id']
            variant = variants_dict[variant_id]
            product_id = variant['product_id']
            product = products_dict[product_id]
            
            # Calculate returned amounts
            returned_quantity = refund['quantity_refunded']
            returned_unit_price = variant['price']
            returned_subtotal = refund['refund_amount']
            returned_subtotal_tax = returned_subtotal * 0.20  # Assuming 20% VAT
            returned_total = returned_subtotal
            returned_total_tax = returned_subtotal_tax
            
            # Parse customer name (assuming format: "First Last" or "Title First Last")
            name_parts = customer['name'].split()
            if len(name_parts) >= 2:
                # Handle titles like "Univ.Prof."
                if name_parts[0].endswith('.'):
                    first_name = name_parts[1] if len(name_parts) > 1 else name_parts[0]
                    last_name = ' '.join(name_parts[2:]) if len(name_parts) > 2 else ''
                else:
                    first_name = name_parts[0]
                    last_name = ' '.join(name_parts[1:])
            else:
                first_name = customer['name']
                last_name = ''
            
            # Get current timestamp with timezone
            current_time = datetime.now(timezone.utc).isoformat()
            
            # Convert dates to datetime with timezone
            order_created_datetime = convert_date_to_datetime_with_timezone(order['order_date'])
            order_updated_datetime = convert_date_to_datetime_with_timezone(order['order_date'])
            refund_date_datetime = convert_date_to_datetime_with_timezone(refund['refund_date'])
            
            # Find the ordered variant ID from the lookup
            ordered_variant_id = find_ordered_variant_id(
                ordered_variants_lookup, 
                order_id, 
                line_item_id, 
                variant_id, 
                product_id
            )
            
            # Skip this refund if we can't find the corresponding ordered variant
            if ordered_variant_id is None:
                print(f"    Warning: Skipping refund {refund['refund_id']} - no matching ordered variant found")
                continue
            
            # Create row data
            row = {
                'id': str(uuid.uuid4()),
                'ordered_variant_id': ordered_variant_id,
                'parent_order_external_id': order_id,
                'refund_external_id': refund['refund_id'],
                'refund_date_created': refund_date_datetime,
                'refund_amount': refund['refund_amount'],
                'refund_reason': refund['reason'],
                'refunded_by': 'system',  # Default value
                'refunded_payment': True,  # Assuming payment was refunded
                'order_status': order['status'],
                'order_total_amount': order['total_amount'],
                'order_currency': 'EUR',
                'order_created_at': order_created_datetime,
                'order_updated_at': order_updated_datetime,
                'customer_email': customer['email'],
                'customer_phone_number': '',  # Not available in source data
                'shipping_first_name': first_name,
                'shipping_last_name': last_name,
                'shipping_address_1': order['street'],
                'shipping_address_2': '',  # Not available in source data
                'shipping_city': order['city'],
                'shipping_state': '',  # Not available in source data
                'shipping_postcode': order['postal_code'],
                'shipping_country_code': order['country'][:2].upper() if len(order['country']) > 2 else order['country'].upper(),
                'product_external_id': product_id,
                'product_title': product['product_name'],
                'product_description': f"{product['category']} product",
                'variant_external_id': variant_id,
                'variant_title': f"{product['product_name']} - {variant['color']} - {variant['size']}",
                'variant_sku': variant['sku'],
                'variant_price': variant['price'],
                'variant_attributes': create_variant_attributes_json(variant),
                'variant_image_id': '',  # Not available in source data
                'variant_image_src': '',  # Not available in source data
                'returned_line_item_external_id': line_item_id,
                'returned_quantity': returned_quantity,
                'returned_unit_price': returned_unit_price,
                'returned_subtotal': returned_subtotal,
                'returned_subtotal_tax': returned_subtotal_tax,
                'returned_total': returned_total,
                'returned_total_tax': returned_total_tax,
                'returned_currency': 'EUR',
                'tax_class': 'standard',  # Default tax class
                'taxes': create_taxes_json(),
                'profile_id': '',  # Will be NULL, to be set by foreign key
                'group_order_id': convert_order_id_to_uuid(order_id),
                'created_at': current_time,
                'updated_at': current_time,
                'category_id': '',  # Will be NULL, to be set by foreign key
                'category': product['category']
            }
            
            all_rows.append(row)
    
    # Create DataFrame and export to CSV
    df = pd.DataFrame(all_rows, columns=headers)
    df.to_csv(output_file, index=False, encoding='utf-8')
    
    print(f"✓ Successfully exported {len(all_rows)} returned variants to {output_file}")
    print(f"  Total datasets processed: {len(suffixes)}")
    print(f"  File size: {os.path.getsize(output_file) / 1024:.1f} KB")
    
    return df

def main():
    print("Starting CSV analysis for returned variants...")
    
    # Read all CSV files
    data = read_all_csv_files()
    
    # Create mappings
    print("\nCreating relationship mappings...")
    mappings = create_mappings(data)
    
    # Show some statistics
    print(f"\nDATA SUMMARY:")
    total_refunds = 0
    for suffix in ['A', 'B', 'C', 'D']:
        refund_count = len(data['refunds'][suffix])
        total_refunds += refund_count
        print(f"  Dataset {suffix}:")
        print(f"    Refunds: {refund_count}")
        print(f"    Line Items: {len(data['line_items'][suffix])}")
        print(f"    Orders: {len(data['orders'][suffix])}")
        print(f"    Customers: {len(data['customers'][suffix])}")
        print(f"    Products: {len(data['products'][suffix])}")
        print(f"    Variants: {len(data['variants'][suffix])}")
    
    print(f"\nTOTAL REFUNDS TO EXPORT: {total_refunds}")
    
    # Export all returned variants to CSV
    print("\n" + "="*60)
    print("EXPORTING RETURNED VARIANTS TO CSV")
    print("="*60)
    
    exported_df = export_all_returned_variants_to_csv(data, mappings)
    
    print("\n" + "="*60)
    print("EXPORT COMPLETED!")
    print("="*60)
    print(f"File: kirill_convert_maria_returned_variants.csv")
    print(f"Total records: {len(exported_df)}")
    print(f"Columns: {len(exported_df.columns)}")
    print("\nFirst few rows preview:")
    print(exported_df[['id', 'refund_external_id', 'parent_order_external_id', 'customer_email', 'product_title', 'returned_quantity']].head())

if __name__ == "__main__":
    main()
