# Company XYZ – Promo Sale Data Extractor

## Overview
Extracts total quantities of promo items (x, y, z) purchased per customer aged **18–35** from a SQLite3 database, and saves the result to a semicolon-delimited CSV.

Two solutions are provided:
- **Solution A** – Pure SQL
- **Solution B** – Pandas

Both produce identical output.

---

## Requirements
- Python 3.7+
- pandas

Install dependencies:
```
pip install -r requirements.txt
```

---

## Setup
Place `solution.py` in the same folder as the provided `company_xyz.db`:
```
project/
├── solution.py
├── requirements.txt
├── .gitignore
├── README.md
└── company_xyz.db        ← provided by Company XYZ
```

---

## Usage
```
python solution.py
```

Output files generated in the same folder:
- `output_sql.csv` — result from SQL solution
- `output_pandas.csv` — result from Pandas solution

---

## Output Format
Semicolon-delimited CSV with no decimal quantities:
```
Customer;Age;Item;Quantity
1;21;x;10
2;23;x;1
2;23;y;1
2;23;z;1
3;35;z;2
```

---

## Business Rules Applied
| Rule | How it's handled |
|---|---|
| Age filter 18–35 | `WHERE age BETWEEN 18 AND 35` / boolean mask |
| NULL = not purchased | `AND quantity IS NOT NULL` / `dropna()` |
| Omit zero-total items | `HAVING SUM > 0` / filter after groupby |
| No decimal quantities | `CAST AS INTEGER` / `.astype(int)` |

---

## Assumptions
- The database file is named `company_xyz.db` and located in the same directory as `solution.py`
- Item names are stored in lowercase in the Items table (x, y, z)
- customer_id and age are stored as integers
