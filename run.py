#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Switzerland-only generator of transactions with realistic human-made duplicates.
Fields: date, first_name, last_name, email, address_1, address_2, city, state (canton)
Plus: canonical_id (ground-truth person), is_duplicate (variant account)

CH specifics:
- Swiss first/last names (DE/FR/IT)
- Cities (Zürich, Genève/Genève, Lausanne, Lugano, etc.)
- Cantons (ZH, GE, VD, TI, …) as "state"
- Multilingual street forms & abbreviations: Strasse/Str., Rue/Rte/Che., Via/Viale, Weg/Wg., Gasse/G., Platz/Pl., Quai, Piazza/P.za
- Swiss email domains: bluewin.ch, sunrise.ch, gmx.ch, hispeed.ch, proton.me, etc.
- Diacritics noise: ä↔ae, ö↔oe, ü↔ue, é/è/ê↔e, à↔a

Run:
  python messy_dupes_ch.py --out transactions_ch.csv --people 300 --dup-rate 0.6 --seed 42
"""

import argparse, csv, random, math
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

# -------------------- Defaults (override via CLI flags) --------------------
DEFAULTS = dict(
    seed=42,
    people=100000,                  # unique persons (canonical clusters)
    avg_tx_per_person=3.0,       # Poisson-like average transactions per person
    dup_rate=0.6,                # share of persons that will have duplicate accounts
    max_dup_accounts=10, #3,          # max extra accounts per person (in addition to the clean one)
    start_date="2023-01-01",
    end_date="2025-09-01",
)

# -------------------- Switzerland data pools --------------------
FIRST_NAMES = [
    # German
    "Lukas","Luca","Lea","Leah","Noah","Lina","Mia","Ben","Elina","Finn","Nina","Jonas","Leonie","Fabian",
    "Matthias","Matteo","Michael","Michi","Sebastian","Sebi","Andreas","Andi","Thomas","Tom","Florian","Flavio",
    "Julia","Julian","Nadine","Tobias","Tobi","Raphael","Reto","Urs",
    # French
    "Louis","Hugo","Jules","Léa","Élise","Camille","Chloé","Emma","Noémie","Théo","Mathieu","Baptiste",
    "Arthur","Zoé","Léo","Émile","Anaïs","François","Guillaume","Victor",
    # Italian
    "Lorenzo","Giorgio","Giulia","Martina","Alessia","Davide","Nicola","Simone","Matteo","Riccardo","Elena","Paolo",
    # Multilingual common
    "Alexander","Alex","Sarah","Sara","Daniel","Samuel","David","Marco","Marc","Martin"
]

LAST_NAMES = [
    # German/Swiss-German
    "Müller","Meier","Keller","Weber","Schmid","Schneider","Hug","Huber","Fischer","Schumacher","Kunz","Frei",
    "Schäfer","Baumann","Bachmann","Vogel","Graf","Kohler","Brunner","Ziegler","Steiner","Wenger",
    # French/Swiss-French
    "Dupont","Durand","Morel","Lambert","Mercier","Blanc","Girard","Roux","Perrin","Meyer",
    # Italian/Swiss-Italian
    "Bianchi","Rossi","Russo","Galli","Ricci","Lombardi","Ferrari","Esposito","Conti","Greco",
    # Multilingual common
    "Martin","Bernard","Lehmann","Fischer","Schmidlin","Suter"
]

# Nickname / variant map (not exhaustive; enough to cause realistic drift)
NICKNAMES = {
    "Alexander":["Alex","Xander","Sascha"],
    "Matthias":["Mat","Matt","Matteo"],
    "Michael":["Mike","Michi"],
    "Sebastian":["Sebi","Bastian"],
    "Andreas":["Andi","Andy"],
    "Thomas":["Tom","Thom"],
    "Guillaume":["Gui","William"],
    "François":["Francois","Franck"],
    "Lorenzo":["Enzo"],
    "Giulia":["Julia"],
    "Matteo":["Matt"],
    "Davide":["David"],
    "Riccardo":["Ricky","Riccardo"],
    "Luca":["Luke"],
    "Noah":["Noa"],
    "Elena":["Helena"],
    "Marco":["Marc"],
}

# Swiss cities with canton codes (state column will store canton or its full name)
CITY_TO_CANTON = [
    ("Zürich", "ZH"), ("Winterthur","ZH"), ("Uster","ZH"), ("Wädenswil","ZH"),
    ("Bern","BE"), ("Biel/Bienne","BE"), ("Thun","BE"),
    ("Luzern","LU"), ("Kriens","LU"),
    ("Uri","UR"),
    ("Schwyz","SZ"), ("Pfäffikon","SZ"),
    ("Stans","NW"), ("Sarnen","OW"),
    ("Glarus","GL"),
    ("Zug","ZG"),
    ("Fribourg","FR"), ("Freiburg","FR"),
    ("Solothurn","SO"),
    ("Basel","BS"), ("Liestal","BL"),
    ("Schaffhausen","SH"),
    ("St. Gallen","SG"), ("Rapperswil-Jona","SG"),
    ("Herisau","AR"), ("Appenzell","AI"),
    ("Chur","GR"), ("Davos","GR"), ("St. Moritz","GR"),
    ("Aarau","AG"), ("Baden","AG"),
    ("Frauenfeld","TG"),
    ("Lugano","TI"), ("Bellinzona","TI"), ("Locarno","TI"),
    ("Lausanne","VD"), ("Nyon","VD"), ("Montreux","VD"), ("Vevey","VD"),
    ("Sion","VS"), ("Sitten","VS"), ("Brig","VS"),
    ("Neuchâtel","NE"),
    ("Genève","GE"), ("Geneva","GE"),
    ("Delémont","JU"),
    ("Zermatt","VS"), ("Interlaken","BE"), ("Yverdon-les-Bains","VD")
]

# Canton code to full name map (subset)
CANTON_FULL = {
    "ZH":"Zürich","BE":"Bern","LU":"Luzern","UR":"Uri","SZ":"Schwyz","OW":"Obwalden","NW":"Nidwalden","GL":"Glarus",
    "ZG":"Zug","FR":"Fribourg","SO":"Solothurn","BS":"Basel-Stadt","BL":"Basel-Landschaft","SH":"Schaffhausen",
    "AR":"Appenzell Ausserrhoden","AI":"Appenzell Innerrhoden","SG":"St. Gallen","GR":"Graubünden","AG":"Aargau",
    "TG":"Thurgau","TI":"Ticino","VD":"Vaud","VS":"Valais","NE":"Neuchâtel","GE":"Genève","JU":"Jura"
}

# Street vocabulary & abbreviations across DE/FR/IT
STREET_PREFIXES = [
    # German
    "Bahnhofstrasse","Schulstrasse","Hauptstrasse","Seestrasse","Kirchweg","Dorfstrasse","Wiesenweg","Gartenstrasse",
    # French
    "Rue de", "Rue du", "Rue des", "Avenue", "Boulevard", "Route de", "Chemin de", "Quai",
    # Italian
    "Via", "Viale", "Vicolo", "Piazza"
]
STREET_ABBREV = [
    # German
    ("Strasse","Str."), ("Weg","Wg."), ("Gasse","G."), ("Platz","Pl."),
    # French
    ("Avenue","Av."), ("Boulevard","Bd."), ("Route","Rte"), ("Chemin","Che."), ("Place","Pl."),
    # Italian
    ("Viale","Vle."), ("Piazza","P.za"), ("Vicolo","Vic.")
]

APT_WORDS = [
    # German
    "Wohnung","Whg.","Haus","Hausnr.","Nr.","Stg.",
    # French
    "Appartement","App.","n°","Bât.","Entrée",
    # Italian
    "Appartamento","Scala","Interno","Piano",
    # Common
    "Apt","Unit","Suite","Ste"
]

EMAIL_DOMAINS = [
    "gmail.com","googlemail.com","outlook.com","icloud.com","proton.me","hotmail.com","aol.com",
    "bluewin.ch","sunrise.ch","gmx.ch","hispeed.ch","yahoo.com"
]

# limited adjacency map (QWERTZ/QWERTY-ish) for typos
ADJACENT = {
    'a':'qsxz','s':'awedxz','d':'sfrecz','f':'dgrtv','g':'fhtybv','h':'gjyubn',
    'j':'hkui m','k':'jil,o','l':'ko;p','o':'9ip','i':'8uo','e':'w34r','r':'e45t',
    't':'r56y','y':'t67u','u':'y78i','m':'njk','n':'bhjm','c':'xv','z':'asx'
}

# -------------------- Noise helpers --------------------
def rand_bool(p: float) -> bool:
    return random.random() < p

def random_adjacent_char(c: str) -> str:
    c_low = c.lower()
    if c_low in ADJACENT and rand_bool(0.8):
        repl = random.choice(ADJACENT[c_low].replace(' ', ''))
        return repl.upper() if c.isupper() else repl
    return c

def introduce_typo(s: str, prob: float = 0.25) -> str:
    if not s or random.random() > prob:
        return s
    s_list = list(s)
    ops = ["sub","del","swap","dup"]
    n_ops = random.choice([1,1,2])
    for _ in range(n_ops):
        if not s_list: break
        i = random.randrange(len(s_list))
        op = random.choice(ops)
        if op == "sub":
            s_list[i] = random_adjacent_char(s_list[i])
        elif op == "del" and len(s_list) > 3:
            s_list.pop(i)
        elif op == "swap" and i < len(s_list)-1:
            s_list[i], s_list[i+1] = s_list[i+1], s_list[i]
        elif op == "dup":
            s_list.insert(i, s_list[i])
    return "".join(s_list)

def random_case_noise(s: str, prob: float = 0.15) -> str:
    if not s or random.random() > prob: return s
    styles = [
        str.lower, str.upper, str.title,
        lambda x: x.capitalize(),
        lambda x: "".join(ch.upper() if random.random()<0.2 else ch for ch in x),
    ]
    return random.choice(styles)(s)

def random_spaces_punct(s: str, prob: float = 0.15) -> str:
    if not s or random.random() > prob: return s
    s = s.replace("  ", " ")
    if rand_bool(0.5): s = s.replace(" ", "  ")
    if rand_bool(0.5): s = s.replace(",", "")
    if rand_bool(0.5): s = s.replace(".", "")
    return s.strip()

def diacritics_noise(s: str, prob: float = 0.25) -> str:
    """Swiss-like diacritics drift: ä/ae, ö/oe, ü/ue, é/è/ê/e, à/a, ï/i, ç/c."""
    if not s or random.random() > prob: return s
    repl = [
        ("ä","ae"),("ö","oe"),("ü","ue"),
        ("Ä","Ae"),("Ö","Oe"),("Ü","Ue"),
        ("é","e"),("è","e"),("ê","e"),("É","E"),("È","E"),("Ê","E"),
        ("à","a"),("ï","i"),("ë","e"),("ç","c"),
    ]
    out = s
    for a,b in repl:
        if a in out and rand_bool(0.5):
            out = out.replace(a, b)
    return out

def abbrev_address(addr1: str) -> str:
    out = addr1
    for a,b in STREET_ABBREV:
        if a in out and rand_bool(0.6):
            out = out.replace(a, b).replace(a.capitalize(), b)
    return out

def random_apt() -> str:
    base = random.choice(["", ""] + APT_WORDS)  # sometimes absent
    if not base: return ""
    num = str(random.randint(1, 350))
    sep = random.choice([" ", " #", "-", "  "])
    return f"{base}{sep}{num}"

def noisy_address(addr1: str, addr2: str, city: str, canton_code: str) -> Tuple[str,str,str,str]:
    # Abbrev + typos + spacing + case + diacritics
    addr1 = " ".join(addr1.split())
    addr1 = abbrev_address(addr1)
    addr1 = diacritics_noise(addr1, 0.25)
    addr1 = introduce_typo(addr1, 0.20)
    addr1 = random_spaces_punct(addr1, 0.25)
    addr1 = random_case_noise(addr1, 0.15)

    # move apt into address_1 or generate a new apt
    if rand_bool(0.12) and addr2:
        addr1 = f"{addr1} {addr2}"
        addr2 = ""
    if rand_bool(0.35):
        addr2 = random_apt()
    else:
        addr2 = diacritics_noise(addr2, 0.25) if addr2 else addr2
        addr2 = introduce_typo(addr2, 0.20) if addr2 else addr2
        addr2 = random_case_noise(addr2, 0.15) if addr2 else addr2

    # City variants (Genève/Geneva, Sion/Sitten etc.)
    city_variants = {
        "Genève":["Genève","Geneva"],
        "Geneva":["Geneva","Genève"],
        "Fribourg":["Fribourg","Freiburg"],
        "Freiburg":["Freiburg","Fribourg"],
        "Sion":["Sion","Sitten"],
        "Sitten":["Sitten","Sion"],
        "Neuchâtel":["Neuchâtel","Neuchatel"],
        "Zürich":["Zürich","Zurich"],
        "Luzern":["Luzern","Lucerne"],
        "Chur":["Chur","Coira"],  # Romansh/Italian variant sometimes seen
        "Biel/Bienne":["Biel","Bienne","Biel/Bienne"],
    }
    if city in city_variants and rand_bool(0.6):
        city = random.choice(city_variants[city])
    city = diacritics_noise(city, 0.35)
    city = introduce_typo(city, 0.12)
    city = random_case_noise(city, 0.10)

    # Canton as state: use code or full name, with minor noise
    canton_name = CANTON_FULL.get(canton_code, canton_code)
    state = random.choice([canton_code, canton_name])
    if rand_bool(0.12):
        state = diacritics_noise(state, 1.0)
        state = introduce_typo(state, 0.5)
        state = random_case_noise(state, 0.5)

    return addr1.strip(), (addr2 or "").strip(), city.strip(), state.strip()

def email_for(first: str, last: str) -> str:
    local = f"{first}.{last}".lower().replace(" ", "")
    local = diacritics_noise(local, 1.0)  # replace diacritics with ascii-ish variants
    domain = random.choice(EMAIL_DOMAINS)
    return f"{local}@{domain}"

def email_variant(email: str) -> str:
    local, domain = email.split("@", 1)
    # gmail/googlemail variations
    if domain in ["gmail.com", "googlemail.com"]:
        if rand_bool(0.6): local = local.replace(".", "")
        if rand_bool(0.5):
            tag = random.choice(["work","shop","news","spam", str(random.randint(1,999))])
            local = f"{local}+{tag}"
        if domain == "googlemail.com" and rand_bool(0.5):
            domain = "gmail.com"
    # domain typo or swap
    if rand_bool(0.2): domain = introduce_typo(domain, 1.0)
    if rand_bool(0.2): domain = random.choice(EMAIL_DOMAINS)
    if rand_bool(0.15): local = introduce_typo(local, 1.0)
    return f"{local}@{domain}"

def jitter_name(first: str, last: str) -> Tuple[str,str]:
    if first in NICKNAMES and rand_bool(0.5):
        first = random.choice(NICKNAMES[first])
    if rand_bool(0.08):
        first, last = last, first  # swapped columns
    first = random_case_noise(diacritics_noise(introduce_typo(first, 0.25), 0.25), 0.20)
    last  = random_case_noise(diacritics_noise(introduce_typo(last,  0.18), 0.25), 0.15)
    return first, last

# -------------------- Generators --------------------
def make_clean_address() -> Tuple[str,str,str,str]:
    # Choose a locale form: DE pattern "Bahnhofstrasse 12", FR "12 Rue de X", IT "Via X 12"
    locale = random.choice(["DE","FR","IT"])

    street_root = random.choice(STREET_PREFIXES)
    number = str(random.randint(1, 299))

    if locale == "DE":
        # e.g., "Bahnhofstrasse 12" or "Gartenstrasse 7"
        addr1 = f"{street_root} {number}"
    elif locale == "FR":
        # e.g., "12 Rue de Lausanne" / "12 Av. de la Gare"
        addr1 = f"{number} {street_root}"
    else:  # IT
        # e.g., "Via Roma 12" / "Viale Stazione 5"
        addr1 = f"{street_root} {number}"

    # Occasionally add a place-like suffix (Platz/Place/Piazza) already covered in STREET_PREFIXES
    addr2 = random.choice(["", "", random_apt()])

    # City & canton
    city, canton_code = random.choice(CITY_TO_CANTON)
    state = canton_code

    # Clean spacing
    addr1 = " ".join(addr1.split())
    addr2 = " ".join((addr2 or "").split())
    return addr1, addr2, city, state

def random_date(start: datetime, end: datetime) -> str:
    delta = end - start
    seconds = random.randint(0, int(delta.total_seconds()))
    return (start + timedelta(seconds=seconds)).date().isoformat()

def poisson_like(mu: float) -> int:
    L = math.exp(-mu)
    k = 0
    p = 1.0
    while p > L:
        k += 1
        p *= random.random()
    return max(k-1, 0)

def build_dataset(cfg: dict) -> List[Dict]:
    random.seed(cfg["seed"])
    start = datetime.fromisoformat(cfg["start_date"])
    end   = datetime.fromisoformat(cfg["end_date"])

    # 1) Base people (clean)
    people = []
    for pid in range(cfg["people"]):
        first = random.choice(FIRST_NAMES)
        last  = random.choice(LAST_NAMES)
        a1, a2, city, canton = make_clean_address()
        email = email_for(first, last)
        people.append({
            "canonical_id": f"id_{pid:05d}",
            "first_name": first,
            "last_name": last,
            "email": email,
            "address_1": a1,
            "address_2": a2,
            "city": city,
            "state": canton  # store canton code initially
        })

    rows = []
    rec_id = 1

    # 2) For each person, create duplicate accounts and transactions
    for person in people:
        dup_accounts = 0
        if rand_bool(cfg["dup_rate"]):
            dup_accounts = random.randint(1, cfg["max_dup_accounts"])

        accounts = [person]  # 0th is clean baseline
        for _ in range(dup_accounts):
            f, l = jitter_name(person["first_name"], person["last_name"])
            e = email_variant(person["email"]) if rand_bool(0.9) else email_for(f, l)
            a1, a2, c, s = noisy_address(person["address_1"], person["address_2"], person["city"], person["state"])
            acc = dict(person)
            acc.update(first_name=f, last_name=l, email=e, address_1=a1, address_2=a2, city=c, state=s)
            accounts.append(acc)

        n_tx = max(1, poisson_like(cfg["avg_tx_per_person"]))
        for _ in range(n_tx):
            acc = random.choice(accounts)
            rows.append({
                "record_id": f"r{rec_id:07d}",
                "date": random_date(start, end),
                "first_name": acc["first_name"],
                "last_name": acc["last_name"],
                "email": acc["email"],
                "address_1": acc["address_1"],
                "address_2": acc["address_2"],
                "city": acc["city"],
                "state": acc["state"],
                "canonical_id": person["canonical_id"],
                "is_duplicate": acc is not accounts[0],
            })
            rec_id += 1

    # shuffle (stable-seeded)
    random.Random(cfg["seed"]).shuffle(rows)
    return rows

# -------------------- CLI --------------------
def main():
    ap = argparse.ArgumentParser(description="Generate Swiss messy transactions with realistic duplicates.")
    ap.add_argument("--out", default="transactions_ch.csv", help="Output CSV path.")
    ap.add_argument("--people", type=int, default=DEFAULTS["people"], help="Unique persons (canonical clusters).")
    ap.add_argument("--avg-tx", type=float, default=DEFAULTS["avg_tx_per_person"], help="Avg transactions per person.")
    ap.add_argument("--dup-rate", type=float, default=DEFAULTS["dup_rate"], help="Share of persons having dup accounts.")
    ap.add_argument("--max-dup", type=int, default=DEFAULTS["max_dup_accounts"], help="Max extra accounts per person.")
    ap.add_argument("--start", default=DEFAULTS["start_date"], help="Start date (YYYY-MM-DD).")
    ap.add_argument("--end", default=DEFAULTS["end_date"], help="End date (YYYY-MM-DD).")
    ap.add_argument("--seed", type=int, default=DEFAULTS["seed"], help="RNG seed.")
    args = ap.parse_args()

    cfg = dict(
        seed=args.seed,
        people=args.people,
        avg_tx_per_person=args.avg_tx,
        dup_rate=args.dup_rate,
        max_dup_accounts=args.max_dup,
        start_date=args.start,
        end_date=args.end
    )

    rows = build_dataset(cfg)

    # Write CSV
    fieldnames = ["record_id","date","first_name","last_name","email","address_1","address_2","city","state","canonical_id","is_duplicate"]
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    # Quick summary to stdout
    total = len(rows)
    dup_rows = sum(1 for r in rows if r["is_duplicate"])
    print(f"Saved {total} rows -> {args.out}")
    print(f"Duplicate rows: {dup_rows} ({dup_rows/total:.1%})")
    print("Sample:", rows[0] if rows else None)

if __name__ == "__main__":
    main()
