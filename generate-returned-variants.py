#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generator for fake returned_variants data based on existing ordered_variants data.
Creates realistic return scenarios for e-commerce order data for seeding the flat.returned_variants table.

Uses the 'id' field from the input CSV as 'ordered_variant_id' to establish proper foreign key relationships.

Usage:
  python generate-returned-variants.py --input test_duplicates_10k.csv --out returned_variants.csv --return-rate 0.15 --seed 42
"""

import argparse
import csv
import json
import random
import uuid
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, List, Optional, Tuple

from faker import Faker

# -------------------- Configuration --------------------
DEFAULTS = {
    'seed': 42,
    'return_rate': 0.15,  # 15% of orders will have returns
    'partial_return_rate': 0.3,  # 30% of returns will be partial
    'input_file': 'datasets/test_duplicates_10k.csv',
    'output_file': 'returned_variants.csv'
}

# -------------------- Data Pools --------------------
RETURN_REASONS = [
    'Defective item',
    'Wrong item received',
    'Item not as described',
    'Changed mind',
    'Too small',
    'Too large',
    'Poor quality',
    'Damaged during shipping',
    'Late delivery',
    'Duplicate order',
    'Color not as expected',
    'Material issues',
    'Sizing issues',
    'Customer dissatisfaction',
    'Product malfunction',
    'Missing parts',
    'Incorrect specifications',
    'Better price found elsewhere',
    'No longer needed',
    'Gift return'
]

REFUNDED_BY_OPTIONS = [
    'Customer Service',
    'Auto-refund System',
    'Return Department',
    'Manager',
    'Support Agent',
    'Quality Assurance',
    'Billing Department'
]

ORDER_STATUSES_AFTER_RETURN = [
    'partially_returned',
    'fully_returned',
    'return_processed',
    'refunded',
    'cancelled'
]

TAX_CLASSES = [
    'standard',
    'reduced',
    'zero',
    'exempt',
    'digital',
    'shipping'
]

# Initialize Faker
fake = Faker()

# -------------------- Helper Functions --------------------
def generate_uuid() -> str:
    """Generate a UUID4 string."""
    return str(uuid.uuid4())

def generate_external_id(prefix: str) -> str:
    """Generate an external ID with prefix."""
    return f"{prefix}_{random.randint(1, 9_999_999)}"

def generate_timestamp_after(base_timestamp: str, days_range: Tuple[int, int] = (1, 30)) -> str:
    """Generate a timestamp after the base timestamp."""
    base_dt = datetime.fromisoformat(base_timestamp.replace('Z', ''))
    days_after = random.randint(days_range[0], days_range[1])
    return_dt = base_dt + timedelta(days=days_after)
    return return_dt.isoformat() + 'Z'

def calculate_tax_amount(subtotal: Decimal, tax_rate: float = 0.08) -> Decimal:
    """Calculate tax amount based on subtotal."""
    tax = subtotal * Decimal(str(tax_rate))
    return tax.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

def generate_taxes_json(subtotal: Decimal, currency: str) -> Optional[str]:
    """Generate realistic taxes JSON structure."""
    if random.random() < 0.3:  # 30% chance of no tax data
        return None
    
    tax_rate = random.uniform(0.05, 0.12)  # 5-12% tax rate
    tax_amount = calculate_tax_amount(subtotal, tax_rate)
    
    taxes = [
        {
            "title": "Sales Tax",
            "rate": round(tax_rate, 4),
            "amount": str(tax_amount),
            "currency": currency
        }
    ]
    
    # Sometimes add additional taxes
    if random.random() < 0.2:
        additional_rate = random.uniform(0.01, 0.03)
        additional_amount = calculate_tax_amount(subtotal, additional_rate)
        taxes.append({
            "title": "City Tax",
            "rate": round(additional_rate, 4),
            "amount": str(additional_amount),
            "currency": currency
        })
    
    return json.dumps(taxes)

# -------------------- Return Data Generation --------------------
def should_return_order(order_status: str) -> bool:
    """Determine if an order should have returns based on its status."""
    # Only delivered, completed, and shipped orders can have returns
    returnable_statuses = ['delivered', 'completed', 'shipped']
    return order_status in returnable_statuses

def generate_return_data(original_record: Dict, return_date: str) -> Dict:
    """Generate return-specific data for a returned item."""
    # Determine return quantity (partial or full)
    original_quantity = int(original_record['line_item_quantity'])
    
    # 30% chance of partial return, otherwise full return
    if random.random() < DEFAULTS['partial_return_rate'] and original_quantity > 1:
        returned_quantity = random.randint(1, original_quantity - 1)
    else:
        returned_quantity = original_quantity
    
    # Calculate return amounts
    unit_price = Decimal(str(original_record['line_item_unit_price']))
    returned_subtotal = unit_price * returned_quantity
    
    # Sometimes apply a restocking fee or partial refund
    refund_percentage = 1.0
    if random.random() < 0.1:  # 10% chance of partial refund
        refund_percentage = random.uniform(0.7, 0.95)
    
    returned_subtotal_after_fees = returned_subtotal * Decimal(str(refund_percentage))
    returned_subtotal_after_fees = returned_subtotal_after_fees.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
    
    # Calculate taxes (commented out - return amount calculated without tax)
    # returned_subtotal_tax = calculate_tax_amount(returned_subtotal_after_fees) if random.random() < 0.7 else None
    # returned_total = returned_subtotal_after_fees + (returned_subtotal_tax or Decimal('0'))
    # returned_total_tax = returned_subtotal_tax
    
    # Return amount based only on item total price (no tax)
    returned_subtotal_tax = None
    returned_total = returned_subtotal_after_fees
    returned_total_tax = None
    
    # Generate refund data
    refund_external_id = generate_external_id('REF') if random.random() < 0.9 else None
    refund_amount = returned_total if random.random() < 0.95 else None
    refunded_payment = random.choice([True, False]) if random.random() < 0.8 else False
    
    return {
        'refund_external_id': refund_external_id,
        'refund_date_created': return_date,
        'refund_amount': refund_amount,
        'refund_reason': random.choice(RETURN_REASONS),
        'refunded_by': random.choice(REFUNDED_BY_OPTIONS) if random.random() < 0.8 else None,
        'refunded_payment': refunded_payment,
        'returned_line_item_external_id': generate_external_id('RETLINE'),
        'returned_quantity': returned_quantity,
        'returned_unit_price': unit_price,
        'returned_subtotal': returned_subtotal_after_fees,
        'returned_subtotal_tax': returned_subtotal_tax,
        'returned_total': returned_total,
        'returned_total_tax': returned_total_tax,
        'returned_currency': original_record['line_item_currency'],
        'tax_class': None,  # Commented out tax calculations
        'taxes': None  # Commented out tax calculations
        # 'tax_class': random.choice(TAX_CLASSES) if random.random() < 0.6 else None,
        # 'taxes': generate_taxes_json(returned_subtotal_after_fees, original_record['line_item_currency'])
    }

def update_order_status_for_return(original_status: str, is_partial_return: bool) -> str:
    """Update order status based on return type."""
    if is_partial_return:
        return 'partially_returned'
    else:
        # For full returns, choose an appropriate status
        return random.choice(['fully_returned', 'return_processed', 'refunded'])

def process_ordered_variants_file(input_file: str, return_rate: float, seed: int) -> List[Dict]:
    """Process the ordered variants CSV and generate returns."""
    random.seed(seed)
    fake.seed_instance(seed)
    
    returned_records = []
    
    # Read the input CSV
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        records = list(reader)
    
    # Group records by order_external_id to handle returns at order level
    orders = {}
    for record in records:
        order_id = record['order_external_id']
        if order_id not in orders:
            orders[order_id] = []
        orders[order_id].append(record)
    
    print(f"Processing {len(orders)} unique orders from {len(records)} line items...")
    
    # Process each order for potential returns
    for order_id, order_records in orders.items():
        # Check if this order should have returns
        if not should_return_order(order_records[0]['order_status']):
            continue
            
        if random.random() > return_rate:
            continue
        
        # Determine return date (after order creation)
        order_created = order_records[0]['order_created_at']
        return_date = generate_timestamp_after(order_created, (3, 60))  # 3-60 days after order
        
        # Decide which items to return (can be partial)
        items_to_return = order_records.copy()
        
        # Sometimes only return some items from the order
        if len(items_to_return) > 1 and random.random() < 0.4:
            num_items_to_return = random.randint(1, len(items_to_return))
            items_to_return = random.sample(items_to_return, num_items_to_return)
        
        # Generate return records for selected items
        for original_record in items_to_return:
            return_data = generate_return_data(original_record, return_date)
            
            # Check if this is a partial return
            is_partial_return = return_data['returned_quantity'] < int(original_record['line_item_quantity'])
            
            # Create the returned variant record
            returned_record = {
                'id': generate_uuid(),
                'ordered_variant_id': original_record['id'],  # Foreign key to original ordered_variants record
                'group_order_id': original_record['group_order_id'],  # Copy group_order_id from input
                'parent_order_external_id': original_record['order_external_id'],
                'refund_external_id': return_data['refund_external_id'],
                'refund_date_created': return_data['refund_date_created'],
                'refund_amount': return_data['refund_amount'],
                'refund_reason': return_data['refund_reason'],
                'refunded_by': return_data['refunded_by'],
                'refunded_payment': return_data['refunded_payment'],
                'order_status': update_order_status_for_return(original_record['order_status'], is_partial_return),
                'order_total_amount': original_record['order_total_amount'],
                'order_currency': original_record['order_currency'],
                'order_created_at': original_record['order_created_at'],
                'order_updated_at': return_data['refund_date_created'],  # Updated when returned
                'customer_email': original_record['customer_email'],
                'customer_phone_number': original_record['customer_phone_number'],
                'shipping_first_name': original_record['shipping_first_name'],
                'shipping_last_name': original_record['shipping_last_name'],
                'shipping_address_1': original_record['shipping_address_1'],
                'shipping_address_2': original_record['shipping_address_2'],
                'shipping_city': original_record['shipping_city'],
                'shipping_state': original_record['shipping_state'],
                'shipping_postcode': original_record['shipping_postcode'],
                'shipping_country_code': original_record['shipping_country_code'],
                'product_external_id': original_record['product_external_id'],
                'product_title': original_record['product_title'],
                'product_description': original_record['product_description'],
                'variant_external_id': original_record['variant_external_id'],
                'variant_title': original_record['variant_title'],
                'variant_sku': original_record['variant_sku'],
                'variant_price': original_record['variant_price'],
                'variant_attributes': original_record['variant_attributes'],
                'variant_image_id': original_record['variant_image_id'],
                'variant_image_src': original_record['variant_image_src'],
                'returned_line_item_external_id': return_data['returned_line_item_external_id'],
                'returned_quantity': return_data['returned_quantity'],
                'returned_unit_price': return_data['returned_unit_price'],
                'returned_subtotal': return_data['returned_subtotal'],
                'returned_subtotal_tax': return_data['returned_subtotal_tax'],
                'returned_total': return_data['returned_total'],
                'returned_total_tax': return_data['returned_total_tax'],
                'returned_currency': return_data['returned_currency'],
                'tax_class': return_data['tax_class'],
                'taxes': return_data['taxes'],
                'created_at': return_date,
                'updated_at': return_date
            }
            
            returned_records.append(returned_record)
    
    return returned_records

# -------------------- CSV Output --------------------
def write_csv(records: List[Dict], output_file: str):
    """Write returned variants records to CSV file."""
    if not records:
        print("No returned variant records to write!")
        return
    
    # Define fieldnames based on the updated schema
    fieldnames = [
        'id', 'ordered_variant_id', 'group_order_id', 'parent_order_external_id', 'refund_external_id', 
        'refund_date_created', 'refund_amount', 'refund_reason', 'refunded_by', 
        'refunded_payment', 'order_status', 'order_total_amount', 'order_currency', 
        'order_created_at', 'order_updated_at', 'customer_email', 'customer_phone_number',
        'shipping_first_name', 'shipping_last_name', 'shipping_address_1',
        'shipping_address_2', 'shipping_city', 'shipping_state', 'shipping_postcode',
        'shipping_country_code', 'product_external_id', 'product_title',
        'product_description', 'variant_external_id', 'variant_title', 'variant_sku',
        'variant_price', 'variant_attributes', 'variant_image_id', 'variant_image_src',
        'returned_line_item_external_id', 'returned_quantity', 'returned_unit_price',
        'returned_subtotal', 'returned_subtotal_tax', 'returned_total',
        'returned_total_tax', 'returned_currency', 'tax_class', 'taxes',
        'created_at', 'updated_at'
    ]
    
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)
    
    print(f"Generated {len(records)} returned variant records and saved to {output_file}")

# -------------------- CLI --------------------
def main():
    parser = argparse.ArgumentParser(description="Generate fake returned_variants data from ordered_variants CSV")
    parser.add_argument('--input', default=DEFAULTS['input_file'], 
                       help=f"Input ordered variants CSV file (default: {DEFAULTS['input_file']})")
    parser.add_argument('--out', default=DEFAULTS['output_file'], 
                       help=f"Output CSV file (default: {DEFAULTS['output_file']})")
    parser.add_argument('--return-rate', type=float, default=DEFAULTS['return_rate'],
                       help=f"Return rate (0.0-1.0) (default: {DEFAULTS['return_rate']})")
    parser.add_argument('--seed', type=int, default=DEFAULTS['seed'],
                       help=f"Random seed (default: {DEFAULTS['seed']})")
    
    args = parser.parse_args()
    
    # Validate return rate
    if not 0.0 <= args.return_rate <= 1.0:
        print("Error: Return rate must be between 0.0 and 1.0")
        return
    
    print(f"Processing {args.input} with {args.return_rate:.1%} return rate and seed {args.seed}...")
    
    try:
        returned_records = process_ordered_variants_file(args.input, args.return_rate, args.seed)
        write_csv(returned_records, args.out)
        
        # Print summary
        print(f"\nSummary:")
        print(f"  Total returned variant records: {len(returned_records)}")
        
        if returned_records:
            # Count unique orders with returns
            unique_returned_orders = len(set(r['parent_order_external_id'] for r in returned_records))
            print(f"  Unique orders with returns: {unique_returned_orders}")
            
            # Count refund statistics
            refunded_records = len([r for r in returned_records if r['refunded_payment']])
            print(f"  Records with payment refunded: {refunded_records}")
            
            # Count partial vs full returns
            partial_returns = len([r for r in returned_records if r['order_status'] == 'partially_returned'])
            full_returns = len(returned_records) - partial_returns
            print(f"  Partial returns: {partial_returns}")
            print(f"  Full returns: {full_returns}")
            
            # Show return reasons distribution
            reason_counts = {}
            for record in returned_records:
                reason = record['refund_reason']
                reason_counts[reason] = reason_counts.get(reason, 0) + 1
            
            print(f"  Top return reasons:")
            for reason, count in sorted(reason_counts.items(), key=lambda x: x[1], reverse=True)[:5]:
                print(f"    {reason}: {count}")
            
            print(f"\n  Sample record: {returned_records[0]}")
    
    except FileNotFoundError:
        print(f"Error: Input file '{args.input}' not found!")
    except Exception as e:
        print(f"Error processing file: {e}")

if __name__ == "__main__":
    main()
