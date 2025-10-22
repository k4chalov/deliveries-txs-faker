# Generate Fake Transactions and Deliveries

## Installation

Install required dependencies:
```bash
pip install -r requirements.txt
```

## Generate orders
```bash
python generate-ordered-variants.py --orders 1000 --out ./datasets/test_duplicates_better_email_phone.csv --seed 789
```

```bash
python generate-ordered-variants.py --orders 1000 --out ./datasets/test_duplicates_with_store_ids.csv --seed 5125
```
python generate-ordered-variants.py --orders 1000000 --out ./datasets/test_duplicates_1m.csv --seed 1548


## Generate returns for duplicates dataset
```bash
python generate-returned-variants.py --input datasets/test_duplicates_50k.csv --out ./datasets/test_returnes_from_50k.csv --return-rate 0.15 --seed 42
```


1. put maria_script.py to maria_script folder

2. Run it to populate maria_script folder with Maria data tables
```
python maria_script.py
```

3. Run to generate ordered_variants table input file
```
python kirill_convert_maria_orders.py
```

3. Run to generate returned_variants table input file
```
python kirill_convert_maria_returns.py
```





python generate-ordered-variants.py --orders 5000 --out ./datasets/test_duplicates_5k.csv --seed 513
python generate-returned-variants.py --input datasets/test_duplicates_5k.csv --out ./datasets/test_returnes_from_5k.csv --return-rate 0.15 --seed 3212


python generate-ordered-variants.py --orders 1000 --out ./datasets/test_duplicates_1k.csv --seed 98463
python generate-returned-variants.py --input datasets/test_duplicates_1k.csv --out ./datasets/test_returnes_from_1k.csv --return-rate 0.15 --seed 47612
