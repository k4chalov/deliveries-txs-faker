import pandas as pd
import numpy as np
import uuid
from datetime import datetime, timedelta
import random
from faker import Faker
import unicodedata

# Initialize Faker for generating additional fields
fake = Faker()
Faker.seed(42)
np.random.seed(42)
random.seed(42)

def clean_text_for_import(text):
    """Clean text to be ASCII-safe for database import"""
    if pd.isna(text) or text is None:
        return None
    
    # Convert to string if not already
    text = str(text)
    
    # Normalize unicode characters to ASCII equivalents
    # This converts ü->u, ö->o, ß->ss, etc.
    text = unicodedata.normalize('NFD', text)
    text = text.encode('ascii', 'ignore').decode('ascii')
    
    return text

def load_maria_data(company_letter):
    """Load all Maria script data for a specific company"""
    try:
        customers = pd.read_csv(f'maria_script/customers_{company_letter}.csv')
        orders = pd.read_csv(f'maria_script/orders_{company_letter}.csv')
        line_items = pd.read_csv(f'maria_script/line_items_{company_letter}.csv')
        products = pd.read_csv(f'maria_script/products_{company_letter}.csv')
        variants = pd.read_csv(f'maria_script/variants_{company_letter}.csv')
        refunds = pd.read_csv(f'maria_script/refunds_{company_letter}.csv')
        
        return customers, orders, line_items, products, variants, refunds
    except FileNotFoundError as e:
        print(f"Error loading data for company {company_letter}: {e}")
        return None, None, None, None, None, None

def generate_duplicates_format_with_mapping(customers, orders, line_items, products, variants, company_letter):
    """Convert Maria data to duplicates.csv format"""
    
    # Merge all data together
    # Start with line_items as base
    df = line_items.copy()
    
    # Add order information
    df = df.merge(orders, on='order_id', how='left')
    
    # Add variant information
    df = df.merge(variants, on='variant_id', how='left')
    
    # Add product information
    df = df.merge(products, on='product_id', how='left')
    
    # Add customer information (with suffix to avoid column conflicts)
    df = df.merge(customers, on='customer_id', how='left', suffixes=('', '_customer'))
    
    # Generate required fields for duplicates format
    duplicates_data = []
    line_item_mapping = {}  # Maps line_item_id to record id
    
    for _, row in df.iterrows():
        # Generate UUIDs and use actual Maria data for external IDs
        record_id = str(uuid.uuid4())
        store_id = str(uuid.uuid4())
        group_order_id = str(uuid.uuid4())
        order_external_id = row['order_id']  # Use actual order ID from Maria data
        product_external_id = row['product_id']  # Use actual product ID from Maria data
        variant_external_id = row['variant_id']  # Use actual variant ID from Maria data
        line_item_external_id = row['line_item_id']  # Use actual line item ID from Maria data
        
        # Generate additional fields
        client_ip = fake.ipv4()
        phone = fake.phone_number() if random.random() > 0.3 else None
        
        # Calculate prices (ensure no null values)
        unit_price = row['price'] if pd.notna(row['price']) else 0.0
        quantity = row['quantity'] if pd.notna(row['quantity']) else 1
        total_price = unit_price * quantity
        discount_amount = total_price * (row['discount'] / 100) if pd.notna(row['discount']) and row['discount'] > 0 else 0
        subtotal = total_price - discount_amount
        
        # Generate variant attributes
        variant_attrs = {
            "color": row['color'],
            "size": row['size']
        }
        
        # Add some random attributes based on sector
        if row['category'] == 'fashion':
            variant_attrs["style"] = random.choice(["Casual", "Formal", "Sport", "Classic"])
        elif row['category'] == 'electronics':
            variant_attrs["material"] = random.choice(["Metal", "Plastic", "Glass"])
        elif row['category'] == 'beauty':
            variant_attrs["material"] = random.choice(["Glass", "Plastic", "Metal"])
            
        variant_attributes_str = str(variant_attrs).replace("'", '"')
        
        # Generate image data
        img_id = f"img_{random.randint(100000, 999999)}"
        img_src = f"https://cdn.example.com/products/{img_id}.jpg" if random.random() > 0.2 else None
        
        # Generate product description
        descriptions = [
            "High-quality product with excellent features.",
            "Premium design meets functionality.",
            "Crafted with attention to detail.",
            "Perfect for everyday use.",
            "Innovative design and superior quality."
        ]
        
        # Convert dates
        order_created = pd.to_datetime(row['order_date']).strftime('%Y-%m-%dT%H:%M:%SZ')
        order_updated = (pd.to_datetime(row['order_date']) + timedelta(days=random.randint(0, 30))).strftime('%Y-%m-%dT%H:%M:%SZ')
        created_at = (pd.to_datetime(row['order_date']) + timedelta(hours=random.randint(-24, 24))).strftime('%Y-%m-%dT%H:%M:%SZ')
        updated_at = (pd.to_datetime(row['order_date']) + timedelta(days=random.randint(0, 60))).strftime('%Y-%m-%dT%H:%M:%SZ')
        
        # Generate currency based on country (use customer country if available, otherwise order country)
        currency_map = {
            'Germany': 'EUR',
            'Switzerland': 'CHF', 
            'Austria': 'EUR'
        }
        country = row.get('country_customer', row.get('country', 'Germany'))
        currency = currency_map.get(country, 'EUR')
        
        # Generate country codes
        country_codes = {
            'Germany': random.choice(['DE', 'GE', 'GM']),
            'Switzerland': random.choice(['CH', 'SW', 'SZ']),
            'Austria': random.choice(['AT', 'AU', 'OS'])
        }
        country_code = country_codes.get(country, 'DE')
        
        # Generate vendor names
        vendors = ["TechCorp", "MaxValue", "PrimeBrand", "SuperiorGoods", "EliteProducts", 
                  "QualityFirst", "MegaStore", "ProLine", "BestChoice", "UltimateBrand"]
        vendor = random.choice(vendors) if random.random() > 0.3 else None
        
        # Generate product types
        product_types = ["Electronics", "Fashion", "Beauty", "Home & Garden", "Sports & Outdoors", 
                        "Books", "Toys & Games", "Health & Beauty", "Jewelry & Watches", 
                        "Pet Supplies", "Tools & Hardware"]
        product_type = random.choice(product_types) if random.random() > 0.2 else None
        
        # Ensure order_total_amount is never null
        order_total = row['total_amount'] if pd.notna(row['total_amount']) else total_price
        
        duplicates_record = {
            'id': record_id,
            'store_id': store_id,
            'group_order_id': group_order_id,
            'order_external_id': order_external_id,
            'order_status': row['status'],
            'order_total_amount': round(order_total, 2),
            'order_currency': currency,
            'order_created_at': order_created,
            'order_updated_at': order_updated,
            'customer_email': clean_text_for_import(row['email']),
            'customer_phone_number': phone,
            'client_ip': client_ip,
            'shipping_first_name': clean_text_for_import(row['name'].split()[0]),
            'shipping_last_name': clean_text_for_import(' '.join(row['name'].split()[1:]) if len(row['name'].split()) > 1 else row['name'].split()[0]),
            'shipping_address_1': clean_text_for_import(row['street']),
            'shipping_address_2': None if random.random() > 0.3 else f"Apt {random.randint(100, 999)}",
            'shipping_city': clean_text_for_import(row['city']),
            'shipping_state': fake.state() if country == 'Germany' else None,
            'shipping_postcode': row['postal_code'],
            'shipping_country_code': country_code,
            'product_external_id': product_external_id,
            'product_title': row['product_name'],
            'product_description': random.choice(descriptions),
            'product_vendor': vendor,
            'product_type': product_type,
            'variant_external_id': variant_external_id,
            'variant_title': f"{row['product_name']} - {row['color']} / {row['size']}" if pd.notna(row['color']) and pd.notna(row['size']) else row['product_name'],
            'variant_sku': row['sku'],
            'variant_price': round(unit_price, 2),
            'variant_attributes': variant_attributes_str,
            'variant_image_id': img_id if img_src else None,
            'variant_image_src': img_src,
            'line_item_external_id': line_item_external_id,
            'line_item_quantity': int(quantity),
            'line_item_unit_price': round(unit_price, 2),
            'line_item_total_price': round(total_price, 2),
            'line_item_currency': currency,
            'line_item_subtotal': round(subtotal, 2) if subtotal != total_price else None,
            'created_at': created_at,
            'updated_at': updated_at
        }
        
        duplicates_data.append(duplicates_record)
        
        # Store mapping from line_item_id to record_id for returns processing
        line_item_mapping[row['line_item_id']] = record_id
    
    return pd.DataFrame(duplicates_data), line_item_mapping

def generate_returns_format_with_mapping(customers, orders, line_items, products, variants, refunds, company_letter, line_item_mapping):
    """Convert Maria data to returnes_from.csv format"""
    
    if refunds.empty:
        return pd.DataFrame()
    
    # Merge refunds with line items (both have order_id, so we'll get order_id_x and order_id_y)
    df = refunds.merge(line_items, on='line_item_id', how='left')
    
    # Use order_id_x (from refunds) as the main order_id and drop order_id_y
    if 'order_id_x' in df.columns and 'order_id_y' in df.columns:
        df['order_id'] = df['order_id_x']  # Use refunds order_id as primary
        df = df.drop(['order_id_x', 'order_id_y'], axis=1)
    
    # Add order information
    df = df.merge(orders, on='order_id', how='left')
    
    # Add variant information
    df = df.merge(variants, on='variant_id', how='left')
    
    # Add product information
    df = df.merge(products, on='product_id', how='left')
    
    # Add customer information (with suffix to avoid column conflicts)
    df = df.merge(customers, on='customer_id', how='left', suffixes=('', '_customer'))
    
    returns_data = []
    
    for _, row in df.iterrows():
        # Generate UUIDs and external IDs
        record_id = str(uuid.uuid4())
        
        # Get the ordered_variant_id from the mapping (this should reference the duplicates record)
        ordered_variant_id = line_item_mapping.get(row['line_item_id'])
        if ordered_variant_id is None:
            # Fallback if mapping not found (shouldn't happen in normal cases)
            ordered_variant_id = str(uuid.uuid4())
        group_order_id = str(uuid.uuid4())
        parent_order_external_id = row['order_id']  # Use actual order ID from Maria data
        refund_external_id = row['refund_id']  # Use actual refund ID from Maria data
        product_external_id = row['product_id']  # Use actual product ID from Maria data
        variant_external_id = row['variant_id']  # Use actual variant ID from Maria data
        returned_line_item_external_id = row['line_item_id']  # Use actual line item ID from Maria data
        
        # Generate additional fields
        refunded_by_options = [None, "Return Department", "Customer Service", "Manager", "Quality Assurance", "Billing Department"]
        refunded_by = random.choice(refunded_by_options)
        refunded_payment = random.choice([True, False])
        
        # Generate return status
        return_statuses = ["fully_returned", "partially_returned", "return_processed", "refunded"]
        return_status = random.choice(return_statuses)
        
        # Calculate amounts (ensure no null values)
        unit_price = row['price'] if pd.notna(row['price']) else 0.0
        returned_subtotal = row['refund_amount'] if pd.notna(row['refund_amount']) else 0.0
        returned_total = returned_subtotal
        
        # Generate currency based on country (use customer country if available, otherwise order country)
        currency_map = {
            'Germany': 'EUR',
            'Switzerland': 'CHF', 
            'Austria': 'EUR'
        }
        country = row.get('country_customer', row.get('country', 'Germany'))
        currency = currency_map.get(country, 'EUR')
        
        # Generate country codes
        country_codes = {
            'Germany': random.choice(['DE', 'GE', 'GM']),
            'Switzerland': random.choice(['CH', 'SW', 'SZ']),
            'Austria': random.choice(['AT', 'AU', 'OS'])
        }
        country_code = country_codes.get(country, 'DE')
        
        # Generate variant attributes
        variant_attrs = {
            "color": row['color'],
            "size": row['size']
        }
        
        # Add some random attributes based on sector
        if row['category'] == 'fashion':
            variant_attrs["style"] = random.choice(["Casual", "Formal", "Sport", "Classic"])
        elif row['category'] == 'electronics':
            variant_attrs["material"] = random.choice(["Metal", "Plastic", "Glass"])
        elif row['category'] == 'beauty':
            variant_attrs["material"] = random.choice(["Glass", "Plastic", "Metal"])
            
        variant_attributes_str = str(variant_attrs).replace("'", '"')
        
        # Generate image data
        img_id = f"img_{random.randint(100000, 999999)}"
        img_src = f"https://cdn.example.com/products/{img_id}.jpg" if random.random() > 0.3 else ""
        
        # Generate product description
        descriptions = [
            "High-quality product with excellent features.",
            "Premium design meets functionality.", 
            "Crafted with attention to detail.",
            "Perfect for everyday use.",
            "Innovative design and superior quality."
        ]
        
        # Convert dates
        refund_date_created = pd.to_datetime(row['refund_date']).strftime('%Y-%m-%dT%H:%M:%SZ')
        order_created = pd.to_datetime(row['order_date']).strftime('%Y-%m-%dT%H:%M:%SZ')
        order_updated = pd.to_datetime(row['refund_date']).strftime('%Y-%m-%dT%H:%M:%SZ')
        created_at = refund_date_created
        updated_at = refund_date_created
        
        # Generate phone number
        phone = fake.phone_number() if random.random() > 0.4 else ""
        
        returns_record = {
            'id': record_id,
            'ordered_variant_id': ordered_variant_id,
            'group_order_id': group_order_id,
            'parent_order_external_id': parent_order_external_id,
            'refund_external_id': refund_external_id,
            'refund_date_created': refund_date_created,
            'refund_amount': round(row['refund_amount'], 2),
            'refund_reason': row['reason'],
            'refunded_by': refunded_by,
            'refunded_payment': refunded_payment,
            'order_status': return_status,
            'order_total_amount': round(row['total_amount'] if pd.notna(row['total_amount']) else returned_total, 2),
            'order_currency': currency,
            'order_created_at': order_created,
            'order_updated_at': order_updated,
            'customer_email': clean_text_for_import(row['email']),
            'customer_phone_number': phone,
            'shipping_first_name': clean_text_for_import(row['name'].split()[0]),
            'shipping_last_name': clean_text_for_import(' '.join(row['name'].split()[1:]) if len(row['name'].split()) > 1 else row['name'].split()[0]),
            'shipping_address_1': clean_text_for_import(row['street']),
            'shipping_address_2': None if random.random() > 0.3 else f"Apt {random.randint(100, 999)}",
            'shipping_city': clean_text_for_import(row['city']),
            'shipping_state': fake.state() if country == 'Germany' else None,
            'shipping_postcode': row['postal_code'],
            'shipping_country_code': country_code,
            'product_external_id': product_external_id,
            'product_title': row['product_name'],
            'product_description': random.choice(descriptions),
            'variant_external_id': variant_external_id,
            'variant_title': f"{row['product_name']} - {row['color']} / {row['size']}" if pd.notna(row['color']) and pd.notna(row['size']) else row['product_name'],
            'variant_sku': row['sku'],
            'variant_price': round(unit_price, 2),
            'variant_attributes': variant_attributes_str,
            'variant_image_id': img_id if img_src else None,
            'variant_image_src': img_src,
            'returned_line_item_external_id': returned_line_item_external_id,
            'returned_quantity': int(row['quantity_refunded']) if pd.notna(row['quantity_refunded']) else 1,
            'returned_unit_price': round(unit_price, 2),
            'returned_subtotal': round(returned_subtotal, 2),
            'returned_subtotal_tax': None,
            'returned_total': round(returned_total, 2),
            'returned_total_tax': None,
            'returned_currency': currency,
            'tax_class': None,
            'taxes': None,
            'created_at': created_at,
            'updated_at': updated_at
        }
        
        returns_data.append(returns_record)
    
    return pd.DataFrame(returns_data)

def convert_maria_data_to_target_format(companies=['A', 'B', 'C', 'D'], output_prefix="maria_converted"):
    """Convert all Maria data to target formats"""
    
    all_duplicates = []
    all_returns = []
    line_item_id_mapping = {}  # Maps line_item_id to duplicates record id
    
    for company in companies:
        print(f"Processing company {company}...")
        
        # Load Maria data
        customers, orders, line_items, products, variants, refunds = load_maria_data(company)
        
        if customers is None:
            print(f"Skipping company {company} due to missing data")
            continue
            
        # Convert to duplicates format and capture ID mapping
        duplicates_df, company_mapping = generate_duplicates_format_with_mapping(customers, orders, line_items, products, variants, company)
        all_duplicates.append(duplicates_df)
        line_item_id_mapping.update(company_mapping)
        
        # Convert to returns format using the ID mapping
        returns_df = generate_returns_format_with_mapping(customers, orders, line_items, products, variants, refunds, company, company_mapping)
        if not returns_df.empty:
            all_returns.append(returns_df)
        
        print(f"Company {company}: {len(duplicates_df)} order lines, {len(returns_df)} returns")
    
    # Combine all companies
    if all_duplicates:
        final_duplicates = pd.concat(all_duplicates, ignore_index=True)
        duplicates_filename = f"{output_prefix}_duplicates.csv"
        final_duplicates.to_csv(duplicates_filename, index=False)
        print(f"Saved {len(final_duplicates)} records to {duplicates_filename}")
    
    if all_returns:
        final_returns = pd.concat(all_returns, ignore_index=True)
        
        # Filter out returns with invalid foreign key references
        if all_duplicates:
            valid_ids = set(final_duplicates['id'])
            before_count = len(final_returns)
            final_returns = final_returns[final_returns['ordered_variant_id'].isin(valid_ids)]
            after_count = len(final_returns)
            if before_count != after_count:
                print(f"Filtered out {before_count - after_count} returns with invalid references")
        
        returns_filename = f"{output_prefix}_returns.csv"
        final_returns.to_csv(returns_filename, index=False)
        print(f"Saved {len(final_returns)} records to {returns_filename}")
    
    return final_duplicates if all_duplicates else None, final_returns if all_returns else None

if __name__ == "__main__":
    # Convert Maria data to target formats
    print("Converting Maria script data to target formats...")
    
    duplicates_df, returns_df = convert_maria_data_to_target_format()
    
    print("\nConversion completed!")
    if duplicates_df is not None:
        print(f"Duplicates dataset: {len(duplicates_df)} records")
    if returns_df is not None:
        print(f"Returns dataset: {len(returns_df)} records")
