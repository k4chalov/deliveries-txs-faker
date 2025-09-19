#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generator for fake ordered_variants data with duplicate detection scenarios.
Creates realistic e-commerce order data with variants for seeding the flat.ordered_variants table.
Generates 9 normal orders + 1 order with 2-6 duplicates using pollution/dirty algorithms.

Usage:
  python generate-ordered-variants.py --out ordered_variants.csv --orders 10 --seed 42
"""

import argparse
import csv
import json
import random
import re
import string
import uuid
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, List, Tuple, Optional

from faker import Faker
from mimesis import Person, Address, Text, Internet
from mimesis.locales import Locale

# -------------------- Configuration --------------------
DEFAULTS = {
    'seed': 42,
    'orders': 100,  # Default number of orders to generate
    'max_variants_per_order': 5,
    # 'max_variants_per_order': 1,
    'max_quantity_per_item': 20,
    'output_file': 'ordered_variants.csv'
}

# Columns to hide from CSV output
hidden_columns_csv = ["duplicate_group_id"]

# -------------------- Data Pools --------------------
ORDER_STATUSES = [
    'pending', 'processing', 'shipped', 'delivered', 'cancelled', 'refunded', 'completed'
]

CURRENCIES = ['USD', 'EUR', 'GBP', 'CAD', 'AUD', 'CHF', 'JPY']

PRODUCT_CATEGORIES = [
    'Electronics', 'Clothing', 'Home & Garden', 'Sports & Outdoors', 'Books',
    'Health & Beauty', 'Toys & Games', 'Automotive', 'Tools & Hardware',
    'Jewelry & Watches', 'Music & Movies', 'Pet Supplies'
]

PRODUCT_ADJECTIVES = [
    'Premium', 'Deluxe', 'Professional', 'Classic', 'Modern', 'Vintage',
    'Eco-Friendly', 'Wireless', 'Portable', 'Heavy-Duty', 'Lightweight',
    'Waterproof', 'Stainless', 'Digital', 'Smart', 'Ultra', 'Pro', 'Max'
]

PRODUCT_NOUNS = [
    'Widget', 'Device', 'Tool', 'Gadget', 'Accessory', 'Component', 'Kit',
    'Set', 'System', 'Solution', 'Product', 'Item', 'Unit', 'Piece'
]

VARIANT_ATTRIBUTES = {
    'color': ['Red', 'Blue', 'Green', 'Black', 'White', 'Gray', 'Silver', 'Gold', 'Brown', 'Purple'],
    'size': ['XS', 'S', 'M', 'L', 'XL', 'XXL', '32', '34', '36', '38', '40', '42'],
    'material': ['Cotton', 'Polyester', 'Leather', 'Metal', 'Plastic', 'Wood', 'Glass', 'Ceramic'],
    'style': ['Classic', 'Modern', 'Vintage', 'Casual', 'Formal', 'Sport', 'Business']
}

VENDORS = [
    'TechCorp', 'GlobalMart', 'PrimeBrand', 'MegaStore', 'EliteProducts',
    'InnovateCo', 'QualityFirst', 'BestChoice', 'TopTier', 'UltimateBrand',
    'SuperiorGoods', 'ExcellenceCorp', 'PremiumPlus', 'MaxValue', 'ProLine'
]

# Initialize Faker and Mimesis
fake = Faker()
person = Person(Locale.EN)
address = Address(Locale.EN)
text = Text(Locale.EN)
internet = Internet()

# -------------------- Helper Functions --------------------
def generate_uuid() -> str:
    """Generate a UUID4 string."""
    return str(uuid.uuid4())

def generate_external_id(prefix: str) -> str:
    """Generate an external ID with prefix."""
    return f"{prefix}_{random.randint(1, 9_999_999)}"

def generate_duplicate_id() -> str:
    """Generate a duplicate ID for linking related records."""
    return f"DUP_{random.randint(1, 9_999_999)}"

def generate_ip_address() -> str:
    """Generate a random IP address using Faker."""
    return fake.ipv4()

# -------------------- Advanced Data Pollution/Dirty Algorithms --------------------

# Keyboard layout for realistic typos (QWERTY)
KEYBOARD_LAYOUT = {
    'q': 'wa', 'w': 'qase', 'e': 'wsdr', 'r': 'edft', 't': 'rfgy', 'y': 'tghu', 'u': 'yhji', 'i': 'ujko', 'o': 'iklp', 'p': 'ol',
    'a': 'qwsz', 's': 'qwazxed', 'd': 'wserfcx', 'f': 'drtgvc', 'g': 'ftyhbv', 'h': 'gyujnb', 'j': 'huikmn', 'k': 'jiolm', 'l': 'kop',
    'z': 'asx', 'x': 'zsdc', 'c': 'xdfv', 'v': 'cfgb', 'b': 'vghn', 'n': 'bhjm', 'm': 'njk',
    '1': '2', '2': '13', '3': '24', '4': '35', '5': '46', '6': '57', '7': '68', '8': '79', '9': '80', '0': '9'
}

# Common OCR and handwriting mistakes
OCR_MISTAKES = {
    '0': ['O', 'o', 'Q'], 'O': ['0', 'o', 'Q'], 'o': ['0', 'O'],
    '1': ['l', 'I', '|'], 'l': ['1', 'I'], 'I': ['1', 'l'],
    '5': ['S', 's'], 'S': ['5', 's'], 's': ['S', '5'],
    '6': ['G', 'g'], 'G': ['6', 'g'], 'g': ['G', '6'],
    '8': ['B', 'b'], 'B': ['8', 'b'], 'b': ['B', '8'],
    'rn': ['m'], 'm': ['rn'], 'cl': ['d'], 'd': ['cl']
}

# Common phonetic mistakes
PHONETIC_MISTAKES = {
    'ph': 'f', 'f': 'ph', 'c': 'k', 'k': 'c', 'z': 's', 's': 'z',
    'i': 'y', 'y': 'i', 'ei': 'ie', 'ie': 'ei'
}

def introduce_realistic_typos(text: str, prob: float = 0.3) -> str:
    """Introduce realistic typos based on keyboard layout and human patterns."""
    if not text or random.random() > prob:
        return text
    
    text_list = list(text.lower())
    num_errors = random.randint(1, min(3, max(1, len(text) // 5)))  # Scale errors with text length
    
    for _ in range(num_errors):
        if len(text_list) < 2:
            break
            
        error_type = random.choices(
            ['keyboard_adjacent', 'transposition', 'omission', 'insertion', 'ocr_mistake', 'phonetic'],
            weights=[0.4, 0.2, 0.15, 0.1, 0.1, 0.05]
        )[0]
        
        pos = random.randint(0, len(text_list) - 1)
        char = text_list[pos]
        
        if error_type == 'keyboard_adjacent' and char in KEYBOARD_LAYOUT:
            # Replace with adjacent key
            adjacent_chars = KEYBOARD_LAYOUT[char]
            if adjacent_chars:
                text_list[pos] = random.choice(adjacent_chars)
                
        elif error_type == 'transposition' and pos < len(text_list) - 1:
            # Swap adjacent characters (very common human error)
            text_list[pos], text_list[pos + 1] = text_list[pos + 1], text_list[pos]
            
        elif error_type == 'omission' and len(text_list) > 3:
            # Skip a character (fast typing error)
            text_list.pop(pos)
            
        elif error_type == 'insertion':
            # Accidentally hit a key twice
            if char in KEYBOARD_LAYOUT:
                insert_char = random.choice([char] + list(KEYBOARD_LAYOUT[char]))
                text_list.insert(pos, insert_char)
            else:
                text_list.insert(pos, char)  # Double character
                
        elif error_type == 'ocr_mistake' and char in OCR_MISTAKES:
            # OCR-like mistakes
            text_list[pos] = random.choice(OCR_MISTAKES[char])
            
        elif error_type == 'phonetic':
            # Phonetic spelling mistakes
            text_str = ''.join(text_list)
            for wrong, right in PHONETIC_MISTAKES.items():
                if wrong in text_str:
                    text_str = text_str.replace(wrong, right, 1)
                    text_list = list(text_str)
                    break
    
    return ''.join(text_list)

def pollute_email(email: str) -> str:
    """Apply realistic pollution to email addresses with proper domain-specific rules."""
    if not email:
        return email
    
    local, domain = email.split('@', 1)
    
    # Determine if this is Gmail (where dots don't matter and +aliases work)
    is_gmail = domain.lower() in ['gmail.com', 'googlemail.com']
    
    # Weighted distribution of common email errors
    if is_gmail:
        # Gmail-specific pollution (dots and +tags are acceptable variations)
        pollution_type = random.choices(
            ['typo_local', 'typo_domain', 'domain_mistake', 'gmail_dot_variation', 'gmail_plus_alias', 'case_variation'],
            weights=[0.2, 0.1, 0.15, 0.3, 0.2, 0.05]
        )[0]
    else:
        # Other domains (dots and +tags create different email addresses)
        pollution_type = random.choices(
            ['typo_local', 'typo_domain', 'domain_mistake', 'case_variation', 'number_variation'],
            weights=[0.4, 0.2, 0.25, 0.1, 0.05]
        )[0]
    
    if pollution_type == 'typo_local':
        # Introduce realistic typos in local part
        local = introduce_realistic_typos(local, 0.5)
    elif pollution_type == 'typo_domain':
        # Typos in domain part
        domain = introduce_realistic_typos(domain, 0.3)
    elif pollution_type == 'domain_mistake':
        # Common domain mistakes based on real data
        domain_mistakes = {
            'gmail.com': ['gmai.com', 'gmial.com', 'gmail.co', 'gmaill.com', 'gmailcom', 'gail.com'],
            'yahoo.com': ['yaho.com', 'yahoo.co', 'yahooo.com', 'yhoo.com', 'ymail.com'],
            'hotmail.com': ['hotmai.com', 'hotmal.com', 'hotmial.com', 'hotmailcom', 'htmail.com'],
            'outlook.com': ['outlok.com', 'outlook.co', 'outloo.com'],
            'aol.com': ['ao.com', 'aoll.com', 'aol.co'],
            'icloud.com': ['iclou.com', 'icloud.co', 'icoud.com'],
            'protonmail.com': ['protonmai.com', 'protonmail.co', 'proton.com']
        }
        if domain in domain_mistakes:
            domain = random.choice(domain_mistakes[domain])
    elif pollution_type == 'gmail_dot_variation' and is_gmail:
        # Gmail-specific: dots can be added/removed (same email address)
        if '.' not in local and len(local) > 3:
            # Add dots at valid positions
            positions = random.sample(range(1, len(local)), min(2, len(local) - 2))
            local_list = list(local)
            for pos in sorted(positions, reverse=True):
                local_list.insert(pos, '.')
            local = ''.join(local_list)
        elif '.' in local:
            # Remove some dots (Gmail ignores them anyway)
            local = local.replace('.', '', random.randint(1, local.count('.')))
    elif pollution_type == 'gmail_plus_alias' and is_gmail:
        # Gmail-specific: add +alias (same email address)
        aliases = ['work', 'shop', 'personal', 'home', 'business', 'spam', 'newsletter', 
                  str(random.randint(1, 999)), str(random.randint(2000, 2024))]
        alias = random.choice(aliases)
        # Remove existing +alias if present
        if '+' in local:
            local = local.split('+')[0]
        local = f"{local}+{alias}"
    elif pollution_type == 'case_variation':
        # Mix case (most email servers are case-insensitive for local part)
        local = ''.join(random.choice([c.upper(), c.lower()]) for c in local)
    elif pollution_type == 'number_variation':
        # Add or modify numbers (creates a different email address)
        if any(c.isdigit() for c in local):
            # Modify existing numbers
            local = ''.join(str(random.randint(0, 9)) if c.isdigit() and random.random() < 0.5 else c for c in local)
        else:
            # Add numbers
            local += str(random.randint(1, 999))
    
    return f"{local}@{domain}"

def pollute_phone(phone: str) -> str:
    """Apply realistic pollution to phone numbers based on real-world patterns."""
    if not phone:
        return phone
    
    # Extract digits only
    digits = re.sub(r'\D', '', phone)
    if len(digits) < 10:
        return phone
    
    # Weighted distribution of phone number errors
    pollution_type = random.choices(
        ['format_variation', 'digit_transposition', 'digit_substitution', 'partial_number', 'extra_digits', 'spacing_errors'],
        weights=[0.3, 0.2, 0.2, 0.1, 0.1, 0.1]
    )[0]
    
    if pollution_type == 'format_variation':
        # Realistic format variations people actually use
        formats = [
            f"({digits[:3]}) {digits[3:6]}-{digits[6:]}",
            f"{digits[:3]}-{digits[3:6]}-{digits[6:]}",
            f"{digits[:3]}.{digits[3:6]}.{digits[6:]}",
            f"{digits[:3]} {digits[3:6]} {digits[6:]}",
            f"+1 {digits[:3]} {digits[3:6]} {digits[6:]}",
            f"+1({digits[:3]}){digits[3:6]}-{digits[6:]}",
            f"1-{digits[:3]}-{digits[3:6]}-{digits[6:]}",
            digits,  # No formatting
            f"{digits[:3]}-{digits[3:]}"  # Area code separated
        ]
        return random.choice(formats)
    elif pollution_type == 'digit_transposition':
        # Swap adjacent digits (common typing error)
        digits_list = list(digits)
        pos = random.randint(0, len(digits_list) - 2)
        digits_list[pos], digits_list[pos + 1] = digits_list[pos + 1], digits_list[pos]
        digits = ''.join(digits_list)
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    elif pollution_type == 'digit_substitution':
        # Replace digits with similar looking ones or adjacent keys
        digit_mistakes = {
            '0': ['O', '9', '1'], '1': ['l', 'I', '7'], '2': ['Z', '3'], '3': ['E', '8'],
            '4': ['A', '7'], '5': ['S', '6'], '6': ['G', '9'], '7': ['T', '1'],
            '8': ['B', '3'], '9': ['g', '6', '0']
        }
        digits_list = list(digits)
        pos = random.randint(0, len(digits_list) - 1)
        if digits_list[pos] in digit_mistakes:
            if random.random() < 0.7:  # Usually replace with number
                digits_list[pos] = str(random.randint(0, 9))
            else:  # Sometimes with letter (OCR-like error)
                digits_list[pos] = random.choice(digit_mistakes[digits_list[pos]])
        digits = ''.join(digits_list)
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    elif pollution_type == 'partial_number':
        # Incomplete numbers (common in rushed data entry)
        if random.random() < 0.5:
            # Missing last 1-2 digits
            digits = digits[:-random.randint(1, 2)]
        else:
            # Missing area code
            digits = digits[3:]
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}" if len(digits) >= 10 else digits
    elif pollution_type == 'extra_digits':
        # Extra digits (extension, country code, etc.)
        extras = ['1', '001', random.choice(['123', '456', '789'])]  # Common extensions
        digits = random.choice(extras) + digits
        return f"+{digits[:1]} ({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
    elif pollution_type == 'spacing_errors':
        # Inconsistent spacing
        return f"({digits[:3]}){digits[3:6]} - {digits[6:]}" if random.random() < 0.5 else f"( {digits[:3]} ) {digits[3:6]} - {digits[6:]}"
    
    return phone

def pollute_address(address: str) -> str:
    """Apply realistic pollution to addresses based on real-world patterns."""
    if not address:
        return address
    
    # Weighted distribution of address errors
    pollution_type = random.choices(
        ['typo', 'abbreviation_variation', 'case_inconsistency', 'spacing_errors', 'number_errors', 'direction_errors'],
        weights=[0.25, 0.3, 0.15, 0.15, 0.1, 0.05]
    )[0]
    
    if pollution_type == 'typo':
        return introduce_realistic_typos(address, 0.3)
    elif pollution_type == 'abbreviation_variation':
        # Realistic address abbreviation variations
        abbrev_variations = {
            # Street types
            'Street': ['St', 'St.', 'Str', 'Strt', 'street'],
            'Avenue': ['Ave', 'Ave.', 'Av', 'Avn', 'avenue'],
            'Drive': ['Dr', 'Dr.', 'Drv', 'Driv', 'drive'],
            'Road': ['Rd', 'Rd.', 'Ro', 'road'],
            'Boulevard': ['Blvd', 'Blvd.', 'Bld', 'Blv', 'boulevard'],
            'Lane': ['Ln', 'Ln.', 'lane'],
            'Court': ['Ct', 'Ct.', 'court'],
            'Place': ['Pl', 'Pl.', 'place'],
            'Circle': ['Cir', 'Cir.', 'circle'],
            'Way': ['Wy', 'way'],
            # Unit types
            'Apartment': ['Apt', 'Apt.', '#', 'Unit', 'apartment'],
            'Suite': ['Ste', 'Ste.', '#', 'Su', 'suite'],
            'Unit': ['Apt', '#', 'unit'],
            'Building': ['Bldg', 'Bldg.', 'Bld', 'building'],
            # Directions
            'North': ['N', 'N.', 'north'],
            'South': ['S', 'S.', 'south'],
            'East': ['E', 'E.', 'east'],
            'West': ['W', 'W.', 'west'],
            'Northeast': ['NE', 'N.E.', 'northeast'],
            'Northwest': ['NW', 'N.W.', 'northwest'],
            'Southeast': ['SE', 'S.E.', 'southeast'],
            'Southwest': ['SW', 'S.W.', 'southwest']
        }
        for full, variations in abbrev_variations.items():
            if full in address:
                address = address.replace(full, random.choice(variations))
                break
            # Check lowercase version
            elif full.lower() in address.lower():
                # Find the actual case in the address and replace
                pattern = re.compile(re.escape(full), re.IGNORECASE)
                address = pattern.sub(random.choice(variations), address, 1)
                break
    elif pollution_type == 'case_inconsistency':
        # Realistic case variations people actually use
        case_patterns = [
            str.upper,  # ALL CAPS
            str.lower,  # all lowercase
            str.title,  # Title Case
            lambda x: x.capitalize(),  # First letter only
            lambda x: ''.join(c.upper() if i == 0 or x[i-1] == ' ' else c.lower() for i, c in enumerate(x))  # Proper case
        ]
        address = random.choice(case_patterns)(address)
    elif pollution_type == 'spacing_errors':
        # Common spacing mistakes
        if random.random() < 0.4:
            address = address.replace(' ', '  ')  # Double spaces
        elif random.random() < 0.3:
            address = address.replace(' ', '')  # No spaces
        elif random.random() < 0.3:
            address = ' ' + address + ' '  # Extra spaces at ends
    elif pollution_type == 'number_errors':
        # Street number errors
        numbers = re.findall(r'\d+', address)
        if numbers:
            old_num = random.choice(numbers)
            # Common number errors: transpose digits, off by one, etc.
            if len(old_num) > 1 and random.random() < 0.5:
                # Transpose digits
                digits = list(old_num)
                pos = random.randint(0, len(digits) - 2)
                digits[pos], digits[pos + 1] = digits[pos + 1], digits[pos]
                new_num = ''.join(digits)
            else:
                # Off by small amount
                new_num = str(max(1, int(old_num) + random.randint(-5, 5)))
            address = address.replace(old_num, new_num, 1)
    elif pollution_type == 'direction_errors':
        # Mix up directions (common in data entry)
        direction_swaps = {'N': 'S', 'S': 'N', 'E': 'W', 'W': 'E', 'NE': 'NW', 'NW': 'NE', 'SE': 'SW', 'SW': 'SE'}
        for old_dir, new_dir in direction_swaps.items():
            if f' {old_dir} ' in address:
                address = address.replace(f' {old_dir} ', f' {new_dir} ', 1)
                break
    
    return address.strip()

def pollute_name(name: str) -> str:
    """Apply realistic pollution to names based on real-world patterns."""
    if not name:
        return name
    
    # Weighted distribution of name errors
    pollution_type = random.choices(
        ['typo', 'case_variation', 'nickname_substitution', 'initial_variation', 'cultural_variation', 'hyphenation'],
        weights=[0.25, 0.2, 0.2, 0.15, 0.1, 0.1]
    )[0]
    
    if pollution_type == 'typo':
        return introduce_realistic_typos(name, 0.25)
    elif pollution_type == 'case_variation':
        # Realistic case patterns for names
        case_patterns = [
            str.upper,  # JOHN
            str.lower,  # john
            str.title,  # John
            lambda x: x.capitalize(),  # John (first letter only)
            lambda x: ''.join(c.upper() if i == 0 or (i > 0 and x[i-1] == ' ') else c.lower() for i, c in enumerate(x))
        ]
        return random.choice(case_patterns)(name)
    elif pollution_type == 'nickname_substitution':
        # Extended nickname mapping based on real usage
        nickname_map = {
            'Alexander': ['Alex', 'Xander', 'Al', 'Alec'], 'Alexandra': ['Alex', 'Alexa', 'Sandra', 'Sasha'],
            'Andrew': ['Andy', 'Drew'], 'Anthony': ['Tony', 'Anton'], 'Benjamin': ['Ben', 'Benny', 'Benji'],
            'Catherine': ['Cat', 'Cathy', 'Kate', 'Katie'], 'Christopher': ['Chris', 'Christie', 'Topher'],
            'Daniel': ['Dan', 'Danny'], 'David': ['Dave', 'Davey'], 'Edward': ['Ed', 'Eddie', 'Ted'],
            'Elizabeth': ['Liz', 'Beth', 'Betty', 'Eliza'], 'Emily': ['Em', 'Emmy'], 'Gregory': ['Greg'],
            'James': ['Jim', 'Jimmy', 'Jamie'], 'Jennifer': ['Jen', 'Jenny'], 'Jessica': ['Jess', 'Jessie'],
            'John': ['Johnny', 'Jack'], 'Jonathan': ['Jon', 'Johnny'], 'Joseph': ['Joe', 'Joey'],
            'Joshua': ['Josh'], 'Kenneth': ['Ken', 'Kenny'], 'Margaret': ['Maggie', 'Meg', 'Peggy'],
            'Matthew': ['Matt', 'Matty'], 'Michael': ['Mike', 'Mick', 'Mickey'], 'Nicholas': ['Nick', 'Nicky'],
            'Patricia': ['Pat', 'Patty', 'Tricia'], 'Rebecca': ['Becky', 'Becca'], 'Richard': ['Rick', 'Rich', 'Dick'],
            'Robert': ['Bob', 'Rob', 'Bobby'], 'Samuel': ['Sam', 'Sammy'], 'Stephen': ['Steve', 'Stevie'],
            'Susan': ['Sue', 'Susie'], 'Thomas': ['Tom', 'Tommy'], 'William': ['Bill', 'Will', 'Billy']
        }
        if name in nickname_map:
            return random.choice(nickname_map[name])
    elif pollution_type == 'initial_variation':
        # Various initial patterns
        if random.random() < 0.6:
            return name[0] + '.'  # J.
        elif random.random() < 0.5:
            return name[0]  # J
        else:
            return name[0].lower() + '.'  # j.
    elif pollution_type == 'cultural_variation':
        # Cultural name variations (simplified)
        cultural_variations = {
            'John': ['Jon', 'Johan', 'Juan', 'Giovanni'], 'Mary': ['Maria', 'Marie'],
            'Peter': ['Pedro', 'Pietro'], 'Paul': ['Pablo', 'Paolo'],
            'Michael': ['Miguel', 'Michele'], 'David': ['Davide']
        }
        if name in cultural_variations:
            return random.choice(cultural_variations[name])
    elif pollution_type == 'hyphenation':
        # Add or remove hyphens in compound names
        if '-' in name:
            if random.random() < 0.5:
                return name.replace('-', ' ')  # Mary-Jane -> Mary Jane
            else:
                return name.replace('-', '')  # Mary-Jane -> MaryJane
        elif ' ' in name and len(name.split()) == 2:
            return '-'.join(name.split())  # Mary Jane -> Mary-Jane
    
    return name

def generate_product_title() -> str:
    """Generate a product title."""
    adjective = random.choice(PRODUCT_ADJECTIVES)
    noun = random.choice(PRODUCT_NOUNS)
    category = random.choice(PRODUCT_CATEGORIES)
    
    patterns = [
        f"{adjective} {noun}",
        f"{adjective} {category} {noun}",
        f"{category} {noun}",
        f"{adjective} {category}"
    ]
    return random.choice(patterns)

def generate_product_description() -> Optional[str]:
    """Generate a product description using Mimesis."""
    if random.random() < 0.2:  # 20% chance of no description
        return None
    return text.sentence()[:100]  # Limit to 100 characters

def generate_variant_title(product_title: str) -> str:
    """Generate a variant title based on product title."""
    if random.random() < 0.3:
        return product_title  # Same as product title
    
    # Add variant-specific attributes
    attributes = []
    if random.random() < 0.7:
        attributes.append(random.choice(VARIANT_ATTRIBUTES['color']))
    if random.random() < 0.5:
        attributes.append(random.choice(VARIANT_ATTRIBUTES['size']))
    
    if attributes:
        return f"{product_title} - {' / '.join(attributes)}"
    return product_title

def generate_sku() -> Optional[str]:
    """Generate a SKU (sometimes None)."""
    if random.random() < 0.1:  # 10% chance of no SKU
        return None
    
    letters = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=2))
    numbers = ''.join(random.choices('0123456789', k=4))
    return f"{letters}-{numbers}"

def generate_variant_attributes() -> Optional[dict]:
    """Generate variant attributes as JSON (sometimes None)."""
    if random.random() < 0.3:  # 30% chance of no attributes
        return None
    
    attrs = {}
    for attr_type, values in VARIANT_ATTRIBUTES.items():
        if random.random() < 0.4:  # 40% chance for each attribute type
            attrs[attr_type] = random.choice(values)
    
    return attrs if attrs else None

def generate_image_data() -> Tuple[Optional[str], Optional[str]]:
    """Generate image ID and source URL (sometimes None)."""
    if random.random() < 0.4:  # 40% chance of no image
        return None, None
    
    image_id = f"img_{random.randint(100000, 999999)}"
    image_src = f"https://cdn.example.com/products/{image_id}.jpg"
    return image_id, image_src

def generate_price() -> Decimal:
    """Generate a realistic price."""
    # Price ranges based on typical e-commerce
    ranges = [
        (5, 50),      # 40% - low price items
        (50, 200),    # 30% - medium price items  
        (200, 1000),  # 20% - high price items
        (1000, 5000)  # 10% - premium items
    ]
    weights = [0.4, 0.3, 0.2, 0.1]
    
    price_range = random.choices(ranges, weights=weights)[0]
    price = random.uniform(price_range[0], price_range[1])
    return Decimal(str(price)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

def generate_timestamp(start_date: datetime, end_date: datetime) -> str:
    """Generate a random timestamp between start and end dates."""
    delta = end_date - start_date
    random_seconds = random.randint(0, int(delta.total_seconds()))
    timestamp = start_date + timedelta(seconds=random_seconds)
    return timestamp.isoformat() + 'Z'

# Removed normalized field generation as per user request

# -------------------- Main Generation Logic --------------------
def generate_realistic_email(first_name: str, last_name: str) -> str:
    """Generate a realistic email address with proper domain rules."""
    # Common email domains with their specific rules
    domains = {
        'gmail.com': 'gmail',
        'yahoo.com': 'yahoo',  
        'hotmail.com': 'outlook',
        'outlook.com': 'outlook',
        'aol.com': 'other',
        'icloud.com': 'other',
        'protonmail.com': 'other'
    }
    
    domain = random.choice(list(domains.keys()))
    domain_type = domains[domain]
    
    # Clean names for email
    first_clean = re.sub(r'[^a-zA-Z]', '', first_name.lower())
    last_clean = re.sub(r'[^a-zA-Z]', '', last_name.lower())
    
    # Generate base local part
    patterns = [
        f"{first_clean}.{last_clean}",
        f"{first_clean}{last_clean}", 
        f"{first_clean[0]}.{last_clean}",
        f"{first_clean}.{last_clean[0]}",
        f"{first_clean}_{last_clean}",
        f"{last_clean}.{first_clean}"
    ]
    
    local_part = random.choice(patterns)
    
    # Add numbers sometimes (realistic pattern)
    if random.random() < 0.4:
        number_patterns = [
            str(random.randint(1, 99)),
            str(random.randint(1980, 2005)),  # Birth years
            str(random.randint(1, 999))
        ]
        local_part += random.choice(number_patterns)
    
    return f"{local_part}@{domain}"

def generate_realistic_phone() -> str:
    """Generate a realistic US phone number with proper formatting."""
    # US phone number format: +1 (XXX) XXX-XXXX
    # Area codes: avoid 0, 1 in first digit, and some reserved ranges
    valid_area_codes = [
        # Major US cities
        212, 646, 917, 347,  # NYC
        213, 323, 424, 747,  # LA
        312, 773, 872,       # Chicago
        415, 628,            # San Francisco
        202,                 # Washington DC
        305, 786,            # Miami
        404, 678, 470,       # Atlanta
        617, 857,            # Boston
        # Other common area codes
        201, 203, 206, 207, 208, 209, 210, 214, 215, 216, 217, 218, 219,
        224, 225, 228, 229, 231, 234, 239, 240, 248, 251, 252, 253, 254,
        256, 260, 262, 267, 269, 270, 276, 281, 301, 302, 303, 304, 307,
        308, 309, 310, 313, 314, 315, 316, 317, 318, 319, 320, 321, 330,
        331, 334, 336, 337, 339, 341, 351, 352, 360, 361, 364, 365, 386,
        401, 402, 403, 405, 406, 407, 408, 409, 410, 412, 413, 414, 417,
        419, 423, 425, 430, 432, 434, 435, 440, 443, 445, 458, 463, 469,
        470, 475, 478, 479, 480, 484, 501, 502, 503, 504, 505, 507, 508,
        509, 510, 512, 513, 515, 516, 517, 518, 520, 530, 540, 541, 551,
        559, 561, 562, 563, 564, 567, 570, 571, 573, 574, 575, 580, 585,
        586, 601, 602, 603, 605, 606, 607, 608, 609, 610, 612, 614, 615,
        616, 618, 619, 620, 623, 626, 630, 631, 636, 641, 646, 650, 651,
        657, 660, 661, 662, 667, 669, 678, 682, 701, 702, 703, 704, 706,
        707, 708, 712, 713, 714, 715, 716, 717, 718, 719, 720, 724, 725,
        727, 731, 732, 734, 737, 740, 743, 747, 754, 757, 760, 762, 763,
        765, 770, 772, 774, 775, 779, 781, 785, 786, 787, 801, 802, 803,
        804, 805, 806, 808, 810, 812, 813, 814, 815, 816, 817, 818, 828,
        830, 831, 832, 835, 843, 845, 847, 848, 850, 856, 857, 858, 859,
        860, 862, 863, 864, 865, 870, 872, 878, 901, 903, 904, 906, 907,
        908, 909, 910, 912, 913, 914, 915, 916, 917, 918, 919, 920, 925,
        928, 929, 930, 931, 934, 936, 937, 940, 941, 947, 949, 951, 952,
        954, 956, 959, 970, 971, 972, 973, 978, 979, 980, 984, 985, 989
    ]
    
    area_code = random.choice(valid_area_codes)
    
    # Exchange code: 2-9 for first digit, 0-9 for second and third
    exchange = random.randint(200, 999)
    
    # Last four digits: 0000-9999 (but avoid 0000, 1111, etc.)
    while True:
        last_four = random.randint(1000, 9999)
        # Avoid patterns like 1111, 2222, etc.
        if len(set(str(last_four))) > 1:
            break
    
    # Format options (realistic variations)
    formats = [
        f"({area_code}) {exchange}-{last_four}",        # Most common
        f"{area_code}-{exchange}-{last_four}",          # Common
        f"+1 ({area_code}) {exchange}-{last_four}",     # International
        f"1-{area_code}-{exchange}-{last_four}",        # With country code
        f"{area_code}.{exchange}.{last_four}",          # Dot separated
    ]
    
    # Weight towards more common formats
    weights = [0.5, 0.25, 0.1, 0.1, 0.05]
    return random.choices(formats, weights=weights)[0]

def generate_customer_data() -> Dict:
    """Generate customer data using improved generators."""
    first_name = person.first_name()
    last_name = person.last_name()
    email = generate_realistic_email(first_name, last_name) if random.random() < 0.9 else None
    phone = generate_realistic_phone() if random.random() < 0.7 else None
    
    return {
        'email': email,
        'phone_number': phone,
        'first_name': first_name,
        'last_name': last_name
    }

def generate_shipping_data() -> Dict:
    """Generate shipping address data using Mimesis for higher quality."""
    first_name = person.first_name()
    last_name = person.last_name()
    
    # Build a complete address
    street_num = address.street_number()
    street_name = address.street_name()
    street_suffix = address.street_suffix()
    addr1 = f"{street_num} {street_name} {street_suffix}"
    
    return {
        'first_name': first_name,
        'last_name': last_name,
        'address_1': addr1,
        'address_2': f"Apt {random.randint(1, 999)}" if random.random() < 0.3 else None,
        'city': address.city(),
        'state': address.state(),
        'postcode': address.postal_code(),
        'country_code': address.country_code()
    }

def create_polluted_customer_data(original_customer: Dict) -> Dict:
    """Create a polluted version of customer data for duplicates."""
    polluted = original_customer.copy()
    
    # Pollute email with high probability
    if polluted['email'] and random.random() < 0.8:
        polluted['email'] = pollute_email(polluted['email'])
    
    # Pollute phone with medium probability
    if polluted['phone_number'] and random.random() < 0.6:
        polluted['phone_number'] = pollute_phone(polluted['phone_number'])
    
    return polluted

def create_polluted_shipping_data(original_shipping: Dict) -> Dict:
    """Create a polluted version of shipping data for duplicates."""
    polluted = original_shipping.copy()
    
    # Pollute names
    if random.random() < 0.7:
        polluted['first_name'] = pollute_name(polluted['first_name'])
    if random.random() < 0.7:
        polluted['last_name'] = pollute_name(polluted['last_name'])
    
    # Pollute address
    if random.random() < 0.8:
        polluted['address_1'] = pollute_address(polluted['address_1'])
    if polluted['address_2'] and random.random() < 0.5:
        polluted['address_2'] = pollute_address(polluted['address_2'])
    
    # Pollute city
    if random.random() < 0.4:
        polluted['city'] = pollute_address(polluted['city'])
    
    return polluted

def generate_order_data(group_order_id: int, start_date: datetime, end_date: datetime) -> Dict:
    """Generate order-level data."""
    currency = random.choice(CURRENCIES)
    created_at = generate_timestamp(start_date, end_date)
    
    # Updated timestamp should be same or later
    created_dt = datetime.fromisoformat(created_at.replace('Z', ''))
    updated_at = generate_timestamp(created_dt, end_date)
    
    return {
        'order_external_id': generate_external_id('ORD'),
        'order_status': random.choice(ORDER_STATUSES),
        'order_currency': currency,
        'order_created_at': created_at,
        'order_updated_at': updated_at,
        'client_ip': generate_ip_address()
    }

def generate_product_data() -> Dict:
    """Generate product data."""
    title = generate_product_title()
    
    return {
        'product_external_id': generate_external_id('PROD'),
        'product_title': title,
        'product_description': generate_product_description(),
        'product_vendor': random.choice(VENDORS) if random.random() < 0.8 else None,
        'product_type': random.choice(PRODUCT_CATEGORIES) if random.random() < 0.6 else None
    }

def generate_variant_data(product_title: str) -> Dict:
    """Generate variant data."""
    variant_title = generate_variant_title(product_title)
    image_id, image_src = generate_image_data()
    variant_attrs = generate_variant_attributes()
    
    return {
        # 'variant_external_id': generate_external_id('VAR') if random.random() < 0.9 else None,
        'variant_external_id': generate_external_id('VAR'),
        'variant_title': variant_title,
        'variant_sku': generate_sku(),
        'variant_price': generate_price(),
        'variant_attributes': json.dumps(variant_attrs) if variant_attrs else None,
        'variant_image_id': image_id,
        'variant_image_src': image_src
    }

def generate_line_item_data(variant_price: Decimal, currency: str) -> Dict:
    """Generate line item data."""
    quantity = random.randint(1, DEFAULTS['max_quantity_per_item'])
    unit_price = variant_price
    total_price = unit_price * quantity
    
    # Sometimes have a subtotal (before taxes/shipping)
    subtotal = None
    if random.random() < 0.6:
        subtotal = total_price * Decimal(random.uniform(0.85, 0.95)).quantize(Decimal('0.01'))
    
    return {
        'line_item_external_id': generate_external_id('LINE'),
        'line_item_quantity': quantity,
        'line_item_unit_price': unit_price,
        'line_item_total_price': total_price,
        'line_item_currency': currency,
        'line_item_subtotal': subtotal
    }

def calculate_order_total(line_items: List[Dict]) -> Decimal:
    """Calculate total order amount from line items."""
    total = sum(item['line_item_total_price'] for item in line_items)
    # Add some variance for taxes, shipping, etc.
    variance = Decimal(random.uniform(0.95, 1.15)).quantize(Decimal('0.01'))
    return (total * variance).quantize(Decimal('0.01'))

def generate_ordered_variants_data(num_orders: int, seed: int = 42) -> List[Dict]:
    """Generate the main dataset with realistic duplicate patterns."""
    random.seed(seed)
    fake.seed_instance(seed)
    
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2024, 12, 31)
    
    # Removed store_ids generation as per user request
    
    records = []
    
    # Calculate how many orders should have duplicates (about 10% of orders)
    duplicate_frequency = max(1, num_orders // 10)  # Every 10th order gets duplicates
    order_count = 0
    
    for order_num in range(num_orders):
        # Decide if this order should have duplicates
        should_have_duplicates = (order_num + 1) % duplicate_frequency == 0
        
        if should_have_duplicates:
            # Generate the original order with duplicates
            duplicate_group_id = generate_duplicate_id()
            original_order = generate_single_order(order_num, start_date, end_date, group_order_id=None, duplicate_group_id=duplicate_group_id)
            records.extend(original_order)
            
            # Generate 2-6 duplicate versions of this order
            num_duplicates = random.randint(2, 6)
            
            # Extract original customer and shipping data for pollution
            original_customer = {
                'email': original_order[0]['customer_email'],
                'phone_number': original_order[0]['customer_phone_number'],
                'first_name': original_order[0]['customer_email'].split('@')[0] if original_order[0]['customer_email'] else person.first_name(),
                'last_name': person.last_name()
            }
            
            original_shipping = {
                'first_name': original_order[0]['shipping_first_name'],
                'last_name': original_order[0]['shipping_last_name'],
                'address_1': original_order[0]['shipping_address_1'],
                'address_2': original_order[0]['shipping_address_2'],
                'city': original_order[0]['shipping_city'],
                'state': original_order[0]['shipping_state'],
                'postcode': original_order[0]['shipping_postcode'],
                'country_code': original_order[0]['shipping_country_code']
            }
            
            for dup_num in range(num_duplicates):
                duplicate_order = generate_duplicate_order(
                    order_num * 1000 + dup_num + 1,  # Unique order number for duplicates
                    start_date,
                    end_date,
                    generate_uuid(),  # Each duplicate order gets its own unique group_order_id
                    duplicate_group_id,  # Same duplicate_group_id links them as duplicates
                    original_customer,
                    original_shipping,
                    original_order
                )
                records.extend(duplicate_order)
        else:
            # Generate a normal order (no duplicates)
            order_records = generate_single_order(order_num, start_date, end_date, group_order_id=None, duplicate_group_id=None)
            records.extend(order_records)
    
    return records

def generate_single_order(order_num: int, start_date: datetime, end_date: datetime, 
                         group_order_id: Optional[str] = None, duplicate_group_id: Optional[str] = None) -> List[Dict]:
    """Generate a single order with its line items."""
    # Generate order-level data
    if group_order_id is None:
        group_order_id = generate_uuid()  # Generate unique group_order_id for this order
    
    order_data = generate_order_data(order_num, start_date, end_date)
    customer_data = generate_customer_data()
    shipping_data = generate_shipping_data()
    
    # Generate line items for this order
    num_variants = random.randint(1, DEFAULTS['max_variants_per_order'])
    line_items = []
    
    for _ in range(num_variants):
        product_data = generate_product_data()
        variant_data = generate_variant_data(product_data['product_title'])
        line_item_data = generate_line_item_data(
            variant_data['variant_price'], 
            order_data['order_currency']
        )
        line_items.append({**product_data, **variant_data, **line_item_data})
    
    # Calculate order total
    order_total = calculate_order_total(line_items)
    
    # Create records for each line item
    order_records = []
    for line_item in line_items:
        record = {
            'id': generate_uuid(),
            'store_id': '1e27b743-d66d-41a4-8b4e-876b051a5948',  # Static store ID for all rows
            'created_at': generate_timestamp(start_date, end_date),
            'updated_at': generate_timestamp(start_date, end_date),
            'group_order_id': group_order_id,  # Shared group_order_id for all variants in this order
            'duplicate_group_id': duplicate_group_id,  # Links duplicate orders together
            
            # Order data
            **order_data,
            'order_total_amount': order_total,
            
            # Customer data
            'customer_email': customer_data['email'],
            'customer_phone_number': customer_data['phone_number'],
            
            # Shipping data (with shipping_ prefix)
            'shipping_first_name': shipping_data['first_name'],
            'shipping_last_name': shipping_data['last_name'],
            'shipping_address_1': shipping_data['address_1'],
            'shipping_address_2': shipping_data['address_2'],
            'shipping_city': shipping_data['city'],
            'shipping_state': shipping_data['state'],
            'shipping_postcode': shipping_data['postcode'],
            'shipping_country_code': shipping_data['country_code'],
            
            # Product and variant data
            **line_item
        }
        
        order_records.append(record)
    
    return order_records

def generate_duplicate_order(order_num: int, start_date: datetime, end_date: datetime,
                           group_order_id: str, duplicate_group_id: str, original_customer: Dict, original_shipping: Dict,
                           original_order: List[Dict]) -> List[Dict]:
    """Generate a duplicate order with polluted data."""
    # Create polluted customer and shipping data
    polluted_customer = create_polluted_customer_data(original_customer)
    polluted_shipping = create_polluted_shipping_data(original_shipping)
    
    # Generate new order data (different order ID, timestamps, etc.)
    order_data = generate_order_data(order_num, start_date, end_date)
    
    # Use same products but potentially different quantities/prices
    duplicate_records = []
    
    for orig_record in original_order:
        # Sometimes vary the quantity slightly
        new_quantity = orig_record['line_item_quantity']
        if random.random() < 0.3:  # 30% chance to vary quantity
            new_quantity = max(1, new_quantity + random.randint(-1, 2))
        
        # Recalculate prices
        unit_price = orig_record['line_item_unit_price']
        total_price = unit_price * new_quantity
        subtotal = None
        if random.random() < 0.6:
            subtotal = total_price * Decimal(random.uniform(0.85, 0.95)).quantize(Decimal('0.01'))
        
        record = {
            'id': generate_uuid(),
            'store_id': '1e27b743-d66d-41a4-8b4e-876b051a5948',  # Static store ID for all rows
            'created_at': generate_timestamp(start_date, end_date),
            'updated_at': generate_timestamp(start_date, end_date),
            'group_order_id': group_order_id,  # Unique group_order_id for this specific order
            'duplicate_group_id': duplicate_group_id,  # Links duplicate orders together
            
            # Order data (new order details)
            **order_data,
            'order_total_amount': calculate_order_total([{'line_item_total_price': total_price}]),
            
            # Polluted customer data
            'customer_email': polluted_customer['email'],
            'customer_phone_number': polluted_customer['phone_number'],
            
            # Polluted shipping data
            'shipping_first_name': polluted_shipping['first_name'],
            'shipping_last_name': polluted_shipping['last_name'],
            'shipping_address_1': polluted_shipping['address_1'],
            'shipping_address_2': polluted_shipping['address_2'],
            'shipping_city': polluted_shipping['city'],
            'shipping_state': polluted_shipping['state'],
            'shipping_postcode': polluted_shipping['postcode'],
            'shipping_country_code': polluted_shipping['country_code'],
            
            # Same product data
            'product_external_id': orig_record['product_external_id'],
            'product_title': orig_record['product_title'],
            'product_description': orig_record['product_description'],
            'product_vendor': orig_record['product_vendor'],
            'product_type': orig_record['product_type'],
            'variant_external_id': orig_record['variant_external_id'],
            'variant_title': orig_record['variant_title'],
            'variant_sku': orig_record['variant_sku'],
            'variant_price': orig_record['variant_price'],
            'variant_attributes': orig_record['variant_attributes'],
            'variant_image_id': orig_record['variant_image_id'],
            'variant_image_src': orig_record['variant_image_src'],
            
            # Updated line item data
            'line_item_external_id': generate_external_id('LINE'),
            'line_item_quantity': new_quantity,
            'line_item_unit_price': unit_price,
            'line_item_total_price': total_price,
            'line_item_currency': order_data['order_currency'],
            'line_item_subtotal': subtotal
        }
        
        duplicate_records.append(record)
    
    return duplicate_records

# -------------------- CSV Output --------------------
def write_csv(records: List[Dict], output_file: str):
    """Write records to CSV file, filtering out hidden columns."""
    if not records:
        print("No records to write!")
        return
    
    all_fieldnames = [
        'id', 'store_id', 'group_order_id', 'order_external_id', 'order_status', 'order_total_amount', 'order_currency',
        'order_created_at', 'order_updated_at', 'customer_email', 'customer_phone_number',
        'client_ip', 'shipping_first_name', 'shipping_last_name', 'shipping_address_1',
        'shipping_address_2', 'shipping_city', 'shipping_state', 'shipping_postcode',
        'shipping_country_code', 'product_external_id', 'product_title', 'product_description',
        'product_vendor', 'product_type', 'variant_external_id', 'variant_title', 'variant_sku',
        'variant_price', 'variant_attributes', 'variant_image_id', 'variant_image_src',
        'line_item_external_id', 'line_item_quantity', 'line_item_unit_price',
        'line_item_total_price', 'line_item_currency', 'line_item_subtotal',
        'created_at', 'updated_at', 'duplicate_group_id'
    ]
    
    # Filter out hidden columns
    visible_fieldnames = [field for field in all_fieldnames if field not in hidden_columns_csv]
    
    # Filter records to only include visible fields
    filtered_records = []
    for record in records:
        filtered_record = {key: value for key, value in record.items() if key in visible_fieldnames}
        filtered_records.append(filtered_record)
    
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=visible_fieldnames)
        writer.writeheader()
        writer.writerows(filtered_records)
    
    hidden_count = len(hidden_columns_csv)
    if hidden_count > 0:
        print(f"Hidden {hidden_count} columns: {', '.join(hidden_columns_csv)}")
    print(f"Generated {len(records)} records with {len(visible_fieldnames)} visible columns and saved to {output_file}")

# -------------------- CLI --------------------
def main():
    parser = argparse.ArgumentParser(description="Generate fake ordered_variants data")
    parser.add_argument('--out', default=DEFAULTS['output_file'], 
                       help=f"Output CSV file (default: {DEFAULTS['output_file']})")
    parser.add_argument('--orders', type=int, default=DEFAULTS['orders'],
                       help=f"Number of orders to generate (default: {DEFAULTS['orders']})")
    parser.add_argument('--seed', type=int, default=DEFAULTS['seed'],
                       help=f"Random seed (default: {DEFAULTS['seed']})")
    parser.add_argument('--max-variants', type=int, default=DEFAULTS['max_variants_per_order'],
                       help=f"Max variants per order (default: {DEFAULTS['max_variants_per_order']})")
    parser.add_argument('--max-quantity', type=int, default=DEFAULTS['max_quantity_per_item'],
                       help=f"Max quantity per line item (default: {DEFAULTS['max_quantity_per_item']})")
    
    args = parser.parse_args()
    
    # Update defaults with CLI args
    DEFAULTS['max_variants_per_order'] = args.max_variants
    DEFAULTS['max_quantity_per_item'] = args.max_quantity
    
    print(f"Generating {args.orders} orders with seed {args.seed}...")
    records = generate_ordered_variants_data(args.orders, args.seed)
    
    write_csv(records, args.out)
    
    # Print summary
    total_records = len(records)
    unique_orders = len(set(r['order_external_id'] for r in records))
    avg_variants_per_order = total_records / unique_orders if unique_orders > 0 else 0
    
    # Count duplicate groups using duplicate_group_id
    duplicate_groups = len(set(r['duplicate_group_id'] for r in records if r['duplicate_group_id'] is not None))
    duplicate_records = len([r for r in records if r['duplicate_group_id'] is not None])
    normal_records = total_records - duplicate_records
    
    print(f"Summary:")
    print(f"  Total records: {total_records}")
    print(f"  Normal records: {normal_records}")
    print(f"  Duplicate records: {duplicate_records}")
    print(f"  Duplicate groups: {duplicate_groups}")
    print(f"  Unique orders: {unique_orders}")
    print(f"  Avg variants per order: {avg_variants_per_order:.1f}")
    
    # Show duplicate group details
    if duplicate_groups > 0:
        for dup_group_id in set(r['duplicate_group_id'] for r in records if r['duplicate_group_id'] is not None):
            group_records = [r for r in records if r['duplicate_group_id'] == dup_group_id]
            group_orders = len(set(r['order_external_id'] for r in group_records))
            print(f"  Duplicate group {dup_group_id}: {len(group_records)} records across {group_orders} orders")
    
    print(f"  Sample record: {records[0] if records else 'None'}")

if __name__ == "__main__":
    main()
