import pandas as pd
import numpy as np
import random
from faker import Faker
from datetime import datetime, timedelta

# Configuración de Faker por país
faker_de = Faker('de_DE')
faker_ch = Faker('de_CH')
faker_at = Faker('de_AT')
Faker.seed(42)
np.random.seed(42)
random.seed(42)

# Configuración sectorial
SECTORS = {
    "fashion": {
        "profiles": ["exploratory", "impulsive", "responsible", "strategic"],
        "distribution": [0.4, 0.3, 0.2, 0.1],
        "variants_range": (3, 8)
    },
    "electronics": {
        "profiles": ["responsible", "strategic", "exploratory", "impulsive"],
        "distribution": [0.5, 0.3, 0.1, 0.1],
        "variants_range": (1, 2)
    },
    "beauty": {
        "profiles": ["impulsive", "responsible", "exploratory", "strategic"],
        "distribution": [0.5, 0.3, 0.1, 0.1],
        "variants_range": (2, 5)
    }
}

def get_variant_attributes(sector):
    if sector == "fashion":
        colors = ["Red", "Blue", "Black", "White", "Green", "Beige"]
        sizes = ["XS", "S", "M", "L", "XL", "XXL"]
    elif sector == "beauty":
        colors = ["Ivory", "Beige", "Caramel", "Mocha", "Rosewood"]
        sizes = ["Cream", "Liquid", "Powder", "Stick"]
    elif sector == "electronics":
        colors = ["Black", "Silver", "White"]
        sizes = ["128GB", "256GB", "512GB", "1TB"]
    return colors, sizes

def generate_company_data(company_id, sector, n_customers):
    config = SECTORS[sector]
    customers, products, variants = [], [], []
    customer_profiles = {}


    # Clientes
    for customer_id in range(1, n_customers + 1):
        profile = np.random.choice(config["profiles"], p=config["distribution"])
        customer_profiles[customer_id] = profile
        country = np.random.choice(["Germany", "Switzerland", "Austria"], p=[0.5, 0.3, 0.2])
        faker_local = {"Germany": faker_de, "Switzerland": faker_ch, "Austria": faker_at}[country]

        customers.append({
            "customer_id": f"{company_id}_{customer_id}",
            "name": faker_local.name(),
            "email": faker_local.email(),
            "signup_date": faker_local.date_between(start_date='-730d', end_date='today'),
            "country": country,
            "city": faker_local.city(),
            "postal_code": faker_local.postcode(),
            "street": faker_local.street_address(),
            "profile": profile
        })

    df_customers = pd.DataFrame(customers)

    # Productos y variantes
    colors, sizes = get_variant_attributes(sector)
    variant_counter = 1  # contador global

    for product_id in range(1, 36):  # ~35 productos
        pid = f"{company_id}_{product_id}"
        base_price = round(random.uniform(10, 200), 2)

        products.append({
            "product_id": pid,
            "product_name": f"Product {company_id}-{product_id}",
            "category": sector,
            "base_price": base_price,
            "created_at": faker_local.date_between(start_date='-730d', end_date='today')
        })

        n_variants = random.randint(*config["variants_range"])
        used = set()

        for _ in range(n_variants):
            while True:
                color, size = random.choice(colors), random.choice(sizes)
                if (color, size) not in used:
                    used.add((color, size))
                    break

            variants.append({
                "variant_id": f"{company_id}_V{variant_counter}",
                "product_id": pid,
                "color": color,
                "size": size,
                "sku": f"{company_id}-P{product_id}-C{color[:2]}-S{size[:2]}",
                "price": round(base_price * random.uniform(0.9, 1.1), 2)
            })
            variant_counter += 1

    df_products = pd.DataFrame(products)
    df_variants = pd.DataFrame(variants)

    return df_customers, df_products, df_variants, customer_profiles


df_customers_A, df_products_A, df_variants_A, profiles_A = generate_company_data("A", "fashion", 230)
df_customers_B, df_products_B, df_variants_B, profiles_B = generate_company_data("B", "electronics", 230)
df_customers_C, df_products_C, df_variants_C, profiles_C = generate_company_data("C", "beauty", 230)

def generate_orders_and_lines(df_customers, df_variants, customer_profiles, company_id, sector):
    orders = []
    line_items = []
    order_id_counter = 1
    line_item_id_counter = 1

    def sample_num_orders(profile):
        if profile == "responsible":
            return np.random.choice([1, 2, 3], p=[0.6, 0.3, 0.1])
        elif profile == "exploratory":
            return np.random.choice([2, 3, 4, 5], p=[0.2, 0.3, 0.3, 0.2])
        elif profile == "impulsive":
            return np.random.choice([1, 2], p=[0.5, 0.5])
        else:
            return np.random.randint(5, 11)

    def sample_num_items(sector):
        if sector == "fashion":
            return np.random.choice([1, 2, 3, 4, 5], p=[0.1, 0.2, 0.3, 0.25, 0.15])
        elif sector == "beauty":
            return np.random.choice([1, 2, 3], p=[0.6, 0.3, 0.1])
        elif sector == "electronics":
            return np.random.choice([1, 2], p=[0.7, 0.3])
        return 1

    def generate_discount(profile):
        if profile == "impulsive": return round(random.uniform(10, 30), 2)
        elif profile == "strategic": return round(random.uniform(15, 35), 2)
        elif profile == "exploratory": return round(random.uniform(5, 15), 2)
        return 0.0

    for _, customer in df_customers.iterrows():
        profile = customer_profiles[int(customer["customer_id"].split("_")[1])]
        n_orders = sample_num_orders(profile)

        for _ in range(n_orders):
            order_id = f"{company_id}_O{order_id_counter}"
            order_date = pd.to_datetime(customer["signup_date"]) + pd.to_timedelta(np.random.randint(0, 731), unit='D')

            orders.append({
                "order_id": order_id,
                "customer_id": customer["customer_id"],
                "order_date": order_date,
                "status": np.random.choice(["completed", "refunded", "cancelled"], p=[0.85, 0.10, 0.05]),
                "payment_method": np.random.choice(
                    ["Credit Card", "TWINT", "Invoice", "PayPal", "Bank Transfer", "Gift Card"],
                    p=[0.30, 0.20, 0.20, 0.15, 0.10, 0.05]),
                "street": customer["street"],
                "city": customer["city"],
                "postal_code": customer["postal_code"],
                "country": customer["country"]
            })

            num_items = sample_num_items(sector)
            used_variants = set()
            product_for_bracketing = None

            for i in range(num_items):
                # Aplicar bracketing condicional
                if sector in ["fashion", "beauty"] and i > 0 and num_items > 1 and random.random() < (0.33 if sector == "fashion" else 0.15):
                    same_product_variants = df_variants[df_variants["product_id"] == product_for_bracketing]
                    variant = same_product_variants.sample(1).iloc[0]
                else:
                    while True:
                        variant = df_variants.sample(1).iloc[0]
                        if variant["variant_id"] not in used_variants:
                            product_for_bracketing = variant["product_id"]
                            break

                used_variants.add(variant["variant_id"])
                discount = generate_discount(profile)
                quantity = np.random.randint(1, 4)

                line_items.append({
                    "line_item_id": f"{company_id}_L{line_item_id_counter}",
                    "order_id": order_id,
                    "variant_id": variant["variant_id"],
                    "quantity": quantity,
                    "discount": discount
                })

                line_item_id_counter += 1

            order_id_counter += 1

    return pd.DataFrame(orders), pd.DataFrame(line_items)

df_orders_A, df_line_items_A = generate_orders_and_lines(df_customers_A, df_variants_A, profiles_A, "A", "fashion")
df_orders_B, df_line_items_B = generate_orders_and_lines(df_customers_B, df_variants_B, profiles_B, "B", "electronics")
df_orders_C, df_line_items_C = generate_orders_and_lines(df_customers_C, df_variants_C, profiles_C, "C", "beauty")

def calculate_order_totals(df_line_items, df_variants, df_orders):
    # Unir con precios
    variant_prices = df_variants[["variant_id", "price"]]
    line_items_with_price = df_line_items.merge(variant_prices, on="variant_id", how="left")

    # Calcular subtotal por línea
    line_items_with_price["line_total"] = (
        line_items_with_price["price"] *
        line_items_with_price["quantity"] *
        (1 - line_items_with_price["discount"] / 100)
    )

    # Agrupar por order_id
    order_totals = line_items_with_price.groupby("order_id")["line_total"].sum().reset_index()
    order_totals.rename(columns={"line_total": "total_amount"}, inplace=True)

    # Merge con df_orders
    df_orders_updated = df_orders.merge(order_totals, on="order_id", how="left")
    df_orders_updated["total_amount"] = df_orders_updated["total_amount"].fillna(0.0).round(2)

    return df_orders_updated

def generate_refunds(df_orders, df_line_items, df_variants, customer_profiles, company_id):
    return_probs = {
        "responsible": 0.15,
        "exploratory": 0.50,
        "impulsive": 0.40,
        "strategic": 0.75
    }

    refund_reasons = [
        "Wrong size", "Defective product", "Item not as described",
        "Changed mind", "Ordered by mistake"
    ]

    def generate_return_days(profile):
        if profile == "impulsive":
            return random.randint(1, 3)
        elif profile == "exploratory":
            return random.randint(5, 10)
        elif profile == "strategic":
            return random.randint(10, 20)
        else:
            return random.randint(3, 7)

    def num_items_to_refund(n_lines):
        roll = random.random()
        if roll < 0.55: return 1
        elif roll < 0.85: return min(2, n_lines)
        else: return min(3, n_lines)

    # Bracketing detection
    item_info = df_line_items.merge(df_variants[["variant_id", "product_id"]], on="variant_id")
    bracketing_orders = item_info.groupby(["order_id", "product_id"])["variant_id"].nunique()
    bracketing_orders = bracketing_orders[bracketing_orders > 1].reset_index()["order_id"].unique()

    refunds = []
    refund_id = 1
    df_orders["order_date"] = pd.to_datetime(df_orders["order_date"])

    for _, order in df_orders.iterrows():
        customer_id = order["customer_id"]
        profile = customer_profiles.get(int(customer_id.split("_")[1]), "responsible")
        base_prob = return_probs.get(profile, 0.15)

        if order["order_id"] in bracketing_orders:
            base_prob = min(base_prob + 0.2, 0.95)

        if random.random() <= base_prob:
            order_lines = df_line_items[df_line_items["order_id"] == order["order_id"]]
            if not order_lines.empty:
                n_to_refund = num_items_to_refund(len(order_lines))
                refunded_lines = order_lines.sample(n=n_to_refund)

                for _, line in refunded_lines.iterrows():
                    quantity_refunded = random.randint(1, line["quantity"])
                    variant_price = df_variants[df_variants["variant_id"] == line["variant_id"]]["price"].values[0]
                    refund_amount = round(variant_price * quantity_refunded * (1 - line["discount"] / 100), 2)
                    days = generate_return_days(profile)
                    refund_date = order["order_date"] + timedelta(days=days)

                    refunds.append({
                        "refund_id": f"{company_id}_R{refund_id}",
                        "order_id": order["order_id"],
                        "line_item_id": line["line_item_id"],
                        "refund_date": refund_date,
                        "quantity_refunded": quantity_refunded,
                        "refund_amount": refund_amount,
                        "reason": random.choice(refund_reasons)
                    })
                    refund_id += 1

    return pd.DataFrame(refunds)

# Cálculo de totales (solo para A inicialmente, B y C se calcularán después de agregar clientes compartidos)
df_orders_A = calculate_order_totals(df_line_items_A, df_variants_A, df_orders_A)

# Generación de devoluciones (solo para A inicialmente, B y C se generarán después)
df_refunds_A = generate_refunds(df_orders_A, df_line_items_A, df_variants_A, profiles_A, "A")

# Función para copiar clientes entre empresas (con nuevos IDs)
def create_shared_customers(base_customers, target_customers, share_pct, base_company, target_company, starting_id):
    shared_customers = []
    shared_ids = np.random.choice(base_customers['customer_id'], size=int(len(base_customers) * share_pct), replace=False)

    for i, cid in enumerate(shared_ids, start=1):
        original = base_customers[base_customers['customer_id'] == cid].iloc[0]
        shared_customer = {
            "customer_id": f"{target_company}_{starting_id + i}",
            "name": original["name"],
            "email": original["email"],
            "signup_date": original["signup_date"],
            "country": original["country"],
            "city": original["city"],
            "postal_code": original["postal_code"],
            "street": original["street"],
            "profile": original["profile"]
        }
        shared_customers.append(shared_customer)

    df_shared = pd.DataFrame(shared_customers)
    updated_target = pd.concat([target_customers, df_shared], ignore_index=True)

    return updated_target, df_shared

def generate_orders_for_shared(df_customers, df_variants, shared_customers, profiles, company_id, starting_order_id, sector):
    orders = []
    line_items = []
    order_id_counter = starting_order_id
    line_item_id_counter = 1

    def sample_num_orders(profile):
        if profile == "responsible":
            return np.random.choice([1, 2, 3], p=[0.6, 0.3, 0.1])
        elif profile == "exploratory":
            return np.random.choice([2, 3, 4, 5], p=[0.2, 0.3, 0.3, 0.2])
        elif profile == "impulsive":
            return np.random.choice([1, 2], p=[0.5, 0.5])
        else:
            return np.random.randint(5, 11)

    def sample_num_items(sector):
        if sector == "fashion":
            return np.random.choice([1, 2, 3, 4, 5], p=[0.1, 0.2, 0.3, 0.25, 0.15])
        elif sector == "beauty":
            return np.random.choice([1, 2, 3], p=[0.6, 0.3, 0.1])
        elif sector == "electronics":
            return np.random.choice([1, 2], p=[0.7, 0.3])
        return 1

    def generate_discount(profile):
        if profile == "impulsive": return round(random.uniform(10, 30), 2)
        elif profile == "strategic": return round(random.uniform(15, 35), 2)
        elif profile == "exploratory": return round(random.uniform(5, 15), 2)
        return 0.0

    for _, customer in shared_customers.iterrows():
        cid_num = int(customer["customer_id"].split("_")[1])
        profile = profiles.get(cid_num, "responsible")
        n_orders = sample_num_orders(profile)

        for _ in range(n_orders):
            order_id = f"{company_id}_O{order_id_counter}"
            order_date = pd.to_datetime(customer["signup_date"]) + pd.to_timedelta(np.random.randint(0, 731), unit='D')

            orders.append({
                "order_id": order_id,
                "customer_id": customer["customer_id"],
                "order_date": order_date,
                "status": np.random.choice(["completed", "refunded", "cancelled"], p=[0.85, 0.10, 0.05]),
                "payment_method": np.random.choice(
                    ["Credit Card", "TWINT", "Invoice", "PayPal", "Bank Transfer", "Gift Card"],
                    p=[0.30, 0.20, 0.20, 0.15, 0.10, 0.05]),
                "street": customer["street"],
                "city": customer["city"],
                "postal_code": customer["postal_code"],
                "country": customer["country"]
            })

            num_items = sample_num_items(sector)
            used_variants = set()
            product_for_bracketing = None

            for i in range(num_items):
                if sector in ["fashion", "beauty"] and i > 0 and random.random() < (0.33 if sector == "fashion" else 0.15):
                    same_product_variants = df_variants[df_variants["product_id"] == product_for_bracketing]
                    variant = same_product_variants.sample(1).iloc[0]
                else:
                    while True:
                        variant = df_variants.sample(1).iloc[0]
                        if variant["variant_id"] not in used_variants:
                            product_for_bracketing = variant["product_id"]
                            break

                used_variants.add(variant["variant_id"])
                discount = generate_discount(profile)
                quantity = np.random.randint(1, 4)

                line_items.append({
                    "line_item_id": f"{company_id}_L{line_item_id_counter}",
                    "order_id": order_id,
                    "variant_id": variant["variant_id"],
                    "quantity": quantity,
                    "discount": discount
                })

                line_item_id_counter += 1

            order_id_counter += 1

    return pd.DataFrame(orders), pd.DataFrame(line_items)

# 1. Seleccionar 20% de clientes de A
total_shared_pct = 0.20
shared_ids_total = np.random.choice(df_customers_A["customer_id"], size=int(len(df_customers_A) * total_shared_pct), replace=False)
shared_base = df_customers_A[df_customers_A["customer_id"].isin(shared_ids_total)].copy()

# 2. Dividir en tres grupos: A-B, A-C, A-B-C
n_total = len(shared_base)
ab_ids = shared_base.sample(frac=0.35, random_state=42)
remaining = shared_base.drop(ab_ids.index)
ac_ids = remaining.sample(frac=0.538, random_state=42)  # ~35% de total
abc_ids = remaining.drop(ac_ids.index)

# 3. Copiar a B y C con nuevos IDs
next_id_B = df_customers_B.shape[0] + 1
next_id_C = df_customers_C.shape[0] + 1

# A-B (solo)
shared_AB = ab_ids.copy()
shared_AB["customer_id"] = [f"B_{i}" for i in range(next_id_B, next_id_B + len(shared_AB))]
df_customers_B = pd.concat([df_customers_B, shared_AB], ignore_index=True)
next_id_B += len(shared_AB)

# A-C (solo)
shared_AC = ac_ids.copy()
shared_AC["customer_id"] = [f"C_{i}" for i in range(next_id_C, next_id_C + len(shared_AC))]
df_customers_C = pd.concat([df_customers_C, shared_AC], ignore_index=True)
next_id_C += len(shared_AC)

# A-B-C (compartidos con ambos)
shared_ABC_base = abc_ids.copy()

shared_ABC_B = shared_ABC_base.copy()
shared_ABC_B["customer_id"] = [f"B_{i}" for i in range(next_id_B, next_id_B + len(shared_ABC_base))]
df_customers_B = pd.concat([df_customers_B, shared_ABC_B], ignore_index=True)

shared_ABC_C = shared_ABC_base.copy()
shared_ABC_C["customer_id"] = [f"C_{i}" for i in range(next_id_C, next_id_C + len(shared_ABC_base))]
df_customers_C = pd.concat([df_customers_C, shared_ABC_C], ignore_index=True)

# 4. Generar pedidos para clientes compartidos en B
start_order_id_B = df_orders_B.shape[0] + 1
shared_orders_B1, shared_lines_B1 = generate_orders_for_shared(
    df_customers_B, df_variants_B, shared_AB, profiles_B, "B", start_order_id_B,"electronics"
)
shared_orders_B2, shared_lines_B2 = generate_orders_for_shared(
    df_customers_B, df_variants_B, shared_ABC_B, profiles_B, "B", start_order_id_B + len(shared_orders_B1), "electronics"
)

df_orders_B = pd.concat([df_orders_B, shared_orders_B1, shared_orders_B2], ignore_index=True)
df_line_items_B = pd.concat([df_line_items_B, shared_lines_B1, shared_lines_B2], ignore_index=True)

# 5. Generar pedidos para clientes compartidos en C
start_order_id_C = df_orders_C.shape[0] + 1
shared_orders_C1, shared_lines_C1 = generate_orders_for_shared(
    df_customers_C, df_variants_C, shared_AC, profiles_C, "C", start_order_id_C,"beauty"
)
shared_orders_C2, shared_lines_C2 = generate_orders_for_shared(
    df_customers_C, df_variants_C, shared_ABC_C, profiles_C, "C", start_order_id_C + len(shared_orders_C1), "beauty"
)

df_orders_C = pd.concat([df_orders_C, shared_orders_C1, shared_orders_C2], ignore_index=True)
df_line_items_C = pd.concat([df_line_items_C, shared_lines_C1, shared_lines_C2], ignore_index=True)

# Ahora calcular totales para B y C después de agregar todos los pedidos
df_orders_B = calculate_order_totals(df_line_items_B, df_variants_B, df_orders_B)
df_orders_C = calculate_order_totals(df_line_items_C, df_variants_C, df_orders_C)

# Generar devoluciones para B y C después de calcular totales
df_refunds_B = generate_refunds(df_orders_B, df_line_items_B, df_variants_B, profiles_B, "B")
df_refunds_C = generate_refunds(df_orders_C, df_line_items_C, df_variants_C, profiles_C, "C")

def inject_behavioral_noise(df_customers, df_orders, df_line_items, df_refunds, df_variants, profiles, company_id):
    df_orders = df_orders.copy()
    df_line_items = df_line_items.copy()
    df_refunds = df_refunds.copy()

    # Seleccionar 5% de clientes aleatoriamente
    n_customers = len(df_customers)
    noisy_customers = np.random.choice(df_customers['customer_id'], size=int(0.05 * n_customers), replace=False)

    for customer_id in noisy_customers:
        cid_num = int(customer_id.split("_")[1])
        profile = profiles.get(cid_num, "responsible")
        customer_orders = df_orders[df_orders["customer_id"] == customer_id]
        if customer_orders.empty:
            continue

        if profile == "responsible":
            # Simular devoluciones como strategic
            for order_id in customer_orders["order_id"]:
                order_lines = df_line_items[df_line_items["order_id"] == order_id]
                for _, line in order_lines.iterrows():
                    variant_price = df_variants[df_variants["variant_id"] == line["variant_id"]]["price"].values[0]
                    df_refunds = pd.concat([df_refunds, pd.DataFrame([{
                        "refund_id": f"{company_id}_R{df_refunds.shape[0] + 1}",
                        "order_id": order_id,
                        "line_item_id": line["line_item_id"],
                        "refund_date": df_orders.loc[df_orders["order_id"] == order_id, "order_date"].iloc[0] + timedelta(days=random.randint(10, 20)),
                        "quantity_refunded": line["quantity"],
                        "refund_amount": variant_price * line["quantity"] * (1 - line["discount"] / 100),
                        "reason": random.choice(["Changed mind", "Wrong size"])
                    }])], ignore_index=True)

        elif profile == "impulsive":
            # Simular devoluciones tardías como strategic
            late_orders = customer_orders.sample(min(2, len(customer_orders)))
            for order_id in late_orders["order_id"]:
                order_lines = df_line_items[df_line_items["order_id"] == order_id]
                line = order_lines.sample(1).iloc[0]
                variant_price = df_variants[df_variants["variant_id"] == line["variant_id"]]["price"].values[0]
                df_refunds = pd.concat([df_refunds, pd.DataFrame([{
                    "refund_id": f"{company_id}_R{df_refunds.shape[0] + 1}",
                    "order_id": order_id,
                    "line_item_id": line["line_item_id"],
                    "refund_date": df_orders.loc[df_orders["order_id"] == order_id, "order_date"].iloc[0] + timedelta(days=random.randint(15, 25)),
                    "quantity_refunded": 1,
                    "refund_amount": variant_price * (1 - line["discount"] / 100),
                    "reason": "Late regret"
                }])], ignore_index=True)

        elif profile == "strategic":
            # Simular comportamiento más "responsable"
            orders = customer_orders.sample(min(2, len(customer_orders)))
            for order_id in orders["order_id"]:
                lines = df_line_items[df_line_items["order_id"] == order_id]
                if len(lines) > 1:
                    df_line_items = df_line_items.drop(lines.sample(len(lines) - 1).index)

        elif profile == "exploratory":
            # Simular comportamiento más minimalista
            few_orders = customer_orders.sample(min(1, len(customer_orders)))
            for order_id in few_orders["order_id"]:
                lines = df_line_items[df_line_items["order_id"] == order_id]
                if len(lines) > 1:
                    df_line_items = df_line_items.drop(lines.sample(len(lines) - 1).index)

        # Añadir comportamiento extremo: devuelven todo
        if random.random() < 0.5:
            for order_id in customer_orders["order_id"]:
                order_lines = df_line_items[df_line_items["order_id"] == order_id]
                for _, line in order_lines.iterrows():
                    variant_price = df_variants[df_variants["variant_id"] == line["variant_id"]]["price"].values[0]
                    df_refunds = pd.concat([df_refunds, pd.DataFrame([{
                        "refund_id": f"{company_id}_R{df_refunds.shape[0] + 1}",
                        "order_id": order_id,
                        "line_item_id": line["line_item_id"],
                        "refund_date": df_orders.loc[df_orders["order_id"] == order_id, "order_date"].iloc[0] + timedelta(days=random.randint(5, 15)),
                        "quantity_refunded": line["quantity"],
                        "refund_amount": variant_price * line["quantity"] * (1 - line["discount"] / 100),
                        "reason": "Returns everything"
                    }])], ignore_index=True)

    return df_line_items, df_refunds


df_line_items_A, df_refunds_A = inject_behavioral_noise(df_customers_A, df_orders_A, df_line_items_A, df_refunds_A, df_variants_A, profiles_A, "A")
df_line_items_B, df_refunds_B = inject_behavioral_noise(df_customers_B, df_orders_B, df_line_items_B, df_refunds_B, df_variants_B, profiles_B, "B")
df_line_items_C, df_refunds_C = inject_behavioral_noise(df_customers_C, df_orders_C, df_line_items_C, df_refunds_C, df_variants_C, profiles_C, "C")

empresa_A = {
    "customers": df_customers_A,
    "orders": df_orders_A,
    "line_items": df_line_items_A,
    "refunds": df_refunds_A,
    "products": df_products_A,
    "variants": df_variants_A
}

empresa_B = {
    "customers": df_customers_B,
    "orders": df_orders_B,
    "line_items": df_line_items_B,
    "refunds": df_refunds_B,
    "products": df_products_B,
    "variants": df_variants_B
}

empresa_C = {
    "customers": df_customers_C,
    "orders": df_orders_C,
    "line_items": df_line_items_C,
    "refunds": df_refunds_C,
    "products": df_products_C,
    "variants": df_variants_C
}

# Ejemplo para Empresa A
for name, df in empresa_A.items():
    df.to_csv(f"{name}_A.csv", index=False)

# Repite para B y C
for name, df in empresa_B.items():
    df.to_csv(f"{name}_B.csv", index=False)

for name, df in empresa_C.items():
    df.to_csv(f"{name}_C.csv", index=False)

# Crear datos base de Empresa D (igual que A - sector moda)
df_customers_D, df_products_D, df_variants_D, profiles_D = generate_company_data("D", "fashion",230 )

# Compartir 7% de clientes con A, B o C
total_shared_d = int(0.07 * len(df_customers_D))

# Crear un pool de clientes únicos de A, B y C
sources_pool = pd.concat([
    df_customers_A[["name", "email", "street", "postal_code", "city", "country", "profile"]],
    df_customers_B[["name", "email", "street", "postal_code", "city", "country", "profile"]],
    df_customers_C[["name", "email", "street", "postal_code", "city", "country", "profile"]]
], ignore_index=True).drop_duplicates()

# Seleccionar 7% al azar desde ese pool
shared_d_base = sources_pool.sample(n=total_shared_d, random_state=42).reset_index(drop=True)

# Generar nuevos customer_id para D
next_id_D = df_customers_D.shape[0] + 1
shared_d_base["customer_id"] = [f"D_{i}" for i in range(next_id_D, next_id_D + total_shared_d)]

# Formatear estructura igual que df_customers_D
shared_d_base["signup_date"] = pd.to_datetime(np.random.choice(pd.date_range("2022-01-01", "2024-12-31"), size=len(shared_d_base)))
shared_d_full = shared_d_base[[
    "customer_id", "name", "email", "signup_date", "country", "city", "postal_code", "street", "profile"
]]

# Añadir a la base de clientes de D
df_customers_D = pd.concat([df_customers_D, shared_d_full], ignore_index=True)

# Generar pedidos y líneas de pedido para todos los clientes de D
df_orders_D, df_line_items_D = generate_orders_for_shared(
    df_customers_D, df_variants_D, df_customers_D, profiles_D, "D", 1,"fashion"
)

# Calcular total_amount para pedidos de D
df_orders_D = calculate_order_totals(df_line_items_D, df_variants_D, df_orders_D)

# Generar devoluciones
df_refunds_D = generate_refunds(
    df_orders_D, df_line_items_D, df_variants_D, profiles_D, "D"
)

# Aplicar ruido conductual
df_line_items_D, df_refunds_D = inject_behavioral_noise(
    df_customers_D, df_orders_D, df_line_items_D, df_refunds_D, df_variants_D, profiles_D, "D"
)

# Guardar la empresa D como diccionario
empresa_D = {
    "customers": df_customers_D,
    "orders": df_orders_D,
    "line_items": df_line_items_D,
    "refunds": df_refunds_D,
    "products": df_products_D,
    "variants": df_variants_D
}
for name, df in empresa_D.items():
    df.to_csv(f"{name}_D.csv", index=False)
