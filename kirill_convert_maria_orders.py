import pandas as pd
import os
import sys
import uuid
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

def export_all_line_items_to_csv(data, mappings, output_file="kirill_convert_maria_ordered_variants.csv"):
    """Export all line items with related data to CSV file"""
    print(f"Exporting all line items to {output_file}...")
    
    # Define the CSV headers as specified
    headers = [
        'id', 'store_id', 'group_order_id', 'order_external_id', 'order_status', 
        'order_total_amount', 'order_currency', 'order_created_at', 'order_updated_at',
        'customer_email', 'customer_phone_number', 'client_ip', 
        'shipping_first_name', 'shipping_last_name', 'shipping_address_1', 'shipping_address_2',
        'shipping_city', 'shipping_state', 'shipping_postcode', 'shipping_country_code',
        'product_external_id', 'product_title', 'product_description', 'product_vendor', 'product_type',
        'variant_external_id', 'variant_title', 'variant_sku', 'variant_price', 'variant_attributes',
        'variant_image_id', 'variant_image_src', 'line_item_external_id', 'line_item_quantity',
        'line_item_unit_price', 'line_item_total_price', 'line_item_currency', 'line_item_subtotal',
        'created_at', 'updated_at'
    ]
    
    # Store ID as specified
    store_id = '1e27b743-d66d-41a4-8b4e-876b051a5948'
    
    # List to store all rows
    all_rows = []
    
    # Process each dataset (A, B, C, D)
    suffixes = ['A', 'B', 'C', 'D']
    
    for suffix in suffixes:
        print(f"  Processing dataset {suffix}...")
        
        line_items_df = data['line_items'][suffix]
        orders_df = data['orders'][suffix]
        customers_df = data['customers'][suffix]
        products_df = data['products'][suffix]
        variants_df = data['variants'][suffix]
        
        # Create lookup dictionaries for faster access
        orders_dict = orders_df.set_index('order_id').to_dict('index')
        customers_dict = customers_df.set_index('customer_id').to_dict('index')
        products_dict = products_df.set_index('product_id').to_dict('index')
        variants_dict = variants_df.set_index('variant_id').to_dict('index')
        
        # Process each line item
        for _, line_item in line_items_df.iterrows():
            # Get related data
            order_id = line_item['order_id']
            variant_id = line_item['variant_id']
            
            order = orders_dict[order_id]
            customer_id = order['customer_id']
            customer = customers_dict[customer_id]
            variant = variants_dict[variant_id]
            product_id = variant['product_id']
            product = products_dict[product_id]
            
            # Calculate prices
            unit_price = variant['price']
            quantity = line_item['quantity']
            discount = line_item['discount']
            total_price = (unit_price * quantity) - discount
            if total_price < 0:
                total_price = 0
            subtotal = total_price  # Assuming subtotal is same as total after discount
            
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
            
            # Create variant attributes string
            variant_attributes = f"Color: {variant['color']}, Size: {variant['size']}"
            
            # Get current timestamp with timezone
            current_time = datetime.now(timezone.utc).isoformat()
            
            # Convert order dates to datetime with timezone
            order_created_datetime = convert_date_to_datetime_with_timezone(order['order_date'])
            order_updated_datetime = convert_date_to_datetime_with_timezone(order['order_date'])
            
            # Create row data
            row = {
                'id': str(uuid.uuid4()),
                'store_id': store_id,
                'group_order_id': convert_order_id_to_uuid(order_id),
                'order_external_id': order_id,
                'order_status': order['status'],
                'order_total_amount': order['total_amount'],
                'order_currency': 'EUR',  # Assuming EUR based on European addresses
                'order_created_at': order_created_datetime,
                'order_updated_at': order_updated_datetime,
                'customer_email': customer['email'],
                'customer_phone_number': '',  # Not available in source data
                'client_ip': '',  # Not available in source data
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
                'product_vendor': 'Default Vendor',  # Not available in source data
                'product_type': product['category'],
                'variant_external_id': variant_id,
                'variant_title': f"{product['product_name']} - {variant['color']} - {variant['size']}",
                'variant_sku': variant['sku'],
                'variant_price': variant['price'],
                'variant_attributes': variant_attributes,
                'variant_image_id': '',  # Not available in source data
                'variant_image_src': '',  # Not available in source data
                'line_item_external_id': line_item['line_item_id'],
                'line_item_quantity': quantity,
                'line_item_unit_price': unit_price,
                'line_item_total_price': total_price,
                'line_item_currency': 'EUR',
                'line_item_subtotal': subtotal,
                'created_at': current_time,
                'updated_at': current_time
            }
            
            all_rows.append(row)
    
    # Create DataFrame and export to CSV
    df = pd.DataFrame(all_rows, columns=headers)
    df.to_csv(output_file, index=False, encoding='utf-8')
    
    print(f"✓ Successfully exported {len(all_rows)} line items to {output_file}")
    print(f"  Total datasets processed: {len(suffixes)}")
    print(f"  File size: {os.path.getsize(output_file) / 1024:.1f} KB")
    
    return df

def find_all_data_for_line_item(line_item_id, data, mappings):
    """Find all connected data for a specific line item"""
    print(f"\n{'='*60}")
    print(f"TRACING ALL DATA CONNECTED TO LINE ITEM: {line_item_id}")
    print(f"{'='*60}")
    
    # Find which suffix this line item belongs to
    suffix = line_item_id.split('_')[0]  # Extract 'A', 'B', 'C', or 'D'
    
    # 1. Get the line item itself
    line_items_df = data['line_items'][suffix]
    line_item = line_items_df[line_items_df['line_item_id'] == line_item_id]
    
    if line_item.empty:
        print(f"ERROR: Line item {line_item_id} not found!")
        return
    
    line_item_row = line_item.iloc[0]
    print(f"\nLINE ITEM:")
    print(f"  ID: {line_item_row['line_item_id']}")
    print(f"  Order ID: {line_item_row['order_id']}")
    print(f"  Variant ID: {line_item_row['variant_id']}")
    print(f"  Quantity: {line_item_row['quantity']}")
    print(f"  Discount: {line_item_row['discount']}")
    
    # 2. Get the order
    order_id = line_item_row['order_id']
    orders_df = data['orders'][suffix]
    order = orders_df[orders_df['order_id'] == order_id].iloc[0]
    
    print(f"\nORDER:")
    print(f"  ID: {order['order_id']}")
    print(f"  Customer ID: {order['customer_id']}")
    print(f"  Date: {order['order_date']}")
    print(f"  Status: {order['status']}")
    print(f"  Payment Method: {order['payment_method']}")
    print(f"  Address: {order['street']}, {order['city']}, {order['postal_code']}, {order['country']}")
    print(f"  Total Amount: {order['total_amount']}")
    
    # 3. Get the customer
    customer_id = order['customer_id']
    customers_df = data['customers'][suffix]
    customer = customers_df[customers_df['customer_id'] == customer_id].iloc[0]
    
    print(f"\nCUSTOMER:")
    print(f"  ID: {customer['customer_id']}")
    print(f"  Name: {customer['name']}")
    print(f"  Email: {customer['email']}")
    print(f"  Signup Date: {customer['signup_date']}")
    print(f"  Address: {customer['street']}, {customer['city']}, {customer['postal_code']}, {customer['country']}")
    print(f"  Profile: {customer['profile']}")
    
    # 4. Get the variant
    variant_id = line_item_row['variant_id']
    variants_df = data['variants'][suffix]
    variant = variants_df[variants_df['variant_id'] == variant_id].iloc[0]
    
    print(f"\nVARIANT:")
    print(f"  ID: {variant['variant_id']}")
    print(f"  Product ID: {variant['product_id']}")
    print(f"  Color: {variant['color']}")
    print(f"  Size: {variant['size']}")
    print(f"  SKU: {variant['sku']}")
    print(f"  Price: {variant['price']}")
    
    # 5. Get the product
    product_id = variant['product_id']
    products_df = data['products'][suffix]
    product = products_df[products_df['product_id'] == product_id].iloc[0]
    
    print(f"\nPRODUCT:")
    print(f"  ID: {product['product_id']}")
    print(f"  Name: {product['product_name']}")
    print(f"  Category: {product['category']}")
    print(f"  Base Price: {product['base_price']}")
    print(f"  Created At: {product['created_at']}")
    
    # 6. Check for refunds
    refunds_df = data['refunds'][suffix]
    related_refunds = refunds_df[refunds_df['line_item_id'] == line_item_id]
    
    if not related_refunds.empty:
        print(f"\nREFUNDS:")
        for _, refund in related_refunds.iterrows():
            print(f"  Refund ID: {refund['refund_id']}")
            print(f"  Date: {refund['refund_date']}")
            print(f"  Quantity Refunded: {refund['quantity_refunded']}")
            print(f"  Refund Amount: {refund['refund_amount']}")
            print(f"  Reason: {refund['reason']}")
            print(f"  ---")
    else:
        print(f"\nREFUNDS: None")
    
    # 7. Show other line items in the same order
    other_line_items = line_items_df[
        (line_items_df['order_id'] == order_id) & 
        (line_items_df['line_item_id'] != line_item_id)
    ]
    
    if not other_line_items.empty:
        print(f"\nOTHER LINE ITEMS IN SAME ORDER:")
        for _, item in other_line_items.iterrows():
            print(f"  {item['line_item_id']}: Variant {item['variant_id']}, Qty: {item['quantity']}, Discount: {item['discount']}")
    else:
        print(f"\nOTHER LINE ITEMS IN SAME ORDER: None")

def main():
    print("Starting CSV analysis...")
    
    # Read all CSV files
    data = read_all_csv_files()
    
    # Create mappings
    print("\nCreating relationship mappings...")
    mappings = create_mappings(data)
    
    # Show some statistics
    print(f"\nDATA SUMMARY:")
    total_line_items = 0
    for suffix in ['A', 'B', 'C', 'D']:
        line_item_count = len(data['line_items'][suffix])
        total_line_items += line_item_count
        print(f"  Dataset {suffix}:")
        print(f"    Line Items: {line_item_count}")
        print(f"    Orders: {len(data['orders'][suffix])}")
        print(f"    Customers: {len(data['customers'][suffix])}")
        print(f"    Products: {len(data['products'][suffix])}")
        print(f"    Variants: {len(data['variants'][suffix])}")
        print(f"    Refunds: {len(data['refunds'][suffix])}")
    
    print(f"\nTOTAL LINE ITEMS TO EXPORT: {total_line_items}")
    
    # Export all line items to CSV
    print("\n" + "="*60)
    print("EXPORTING TO CSV")
    print("="*60)
    
    exported_df = export_all_line_items_to_csv(data, mappings)
    
    print("\n" + "="*60)
    print("EXPORT COMPLETED!")
    print("="*60)
    print(f"File: kirill_convert_maria_ordered_variants.csv")
    print(f"Total records: {len(exported_df)}")
    print(f"Columns: {len(exported_df.columns)}")
    print("\nFirst few rows preview:")
    print(exported_df[['id', 'order_external_id', 'customer_email', 'product_title', 'variant_sku', 'line_item_quantity']].head())
    
    # Optional: Show example of how to trace specific line items
    print(f"\nTo trace any specific line item, you can still call:")
    print("find_all_data_for_line_item('LINE_ITEM_ID', data, mappings)")
    print("Examples: 'A_L5', 'B_L10', 'C_L25', 'D_L100'")

if __name__ == "__main__":
    main()
