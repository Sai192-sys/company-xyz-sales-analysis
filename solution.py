import sqlite3
import pandas as pd


DB_PATH = "company_xyz.db"



# Database connection


def get_connection(db_path):
    
    try:
        conn = sqlite3.connect(db_path)
        print(f"Connected to database: {db_path}")
        return conn
    except sqlite3.Error as e:
        print(f"Error connecting to database: {e}")
        raise



# Solution A: Pure SQL


def solution_sql(conn):
    
    query = """
        SELECT
            c.customer_id                    AS Customer,
            c.age                            AS Age,
            i.item_name                      AS Item,
            CAST(SUM(o.quantity) AS INTEGER) AS Quantity
        FROM Customer c
        JOIN Sales  s ON s.customer_id = c.customer_id
        JOIN Orders o ON o.sales_id    = s.sales_id
        JOIN Items  i ON i.item_id     = o.item_id
        WHERE c.age BETWEEN 18 AND 35
          AND o.quantity IS NOT NULL
        GROUP BY c.customer_id, i.item_id
        HAVING SUM(o.quantity) > 0
        ORDER BY c.customer_id, i.item_name;
    """
    try:
        df = pd.read_sql_query(query, conn)
        return df
    except Exception as e:
        print(f"Error running SQL query: {e}")
        raise



# Solution B: Pandas


def load_tables(conn):
    
    try:
        customer = pd.read_sql_query("SELECT * FROM Customer", conn)
        sales    = pd.read_sql_query("SELECT * FROM Sales",    conn)
        orders   = pd.read_sql_query("SELECT * FROM Orders",   conn)
        items    = pd.read_sql_query("SELECT * FROM Items",    conn)
        return customer, sales, orders, items
    except Exception as e:
        print(f"Error loading tables: {e}")
        raise


def filter_customers(customer):
    
    return customer[(customer["age"] >= 18) & (customer["age"] <= 35)]


def join_tables(customer, sales, orders, items):
    
    return (customer
            .merge(sales,  on="customer_id")
            .merge(orders, on="sales_id")
            .merge(items,  on="item_id"))


def aggregate(df):
    
    df = df.dropna(subset=["quantity"])
    df = (df.groupby(["customer_id", "age", "item_name"], as_index=False)
            .agg(Quantity=("quantity", "sum")))
    df = df[df["Quantity"] > 0]
    df["Quantity"] = df["Quantity"].astype(int)
    return df


def rename_and_sort(df):
    
    return (df.rename(columns={"customer_id": "Customer", "age": "Age", "item_name": "Item"})
              [["Customer", "Age", "Item", "Quantity"]]
              .sort_values(["Customer", "Item"])
              .reset_index(drop=True))


def solution_pandas(conn):
    
    customer, sales, orders, items = load_tables(conn)
    customer = filter_customers(customer)
    df = join_tables(customer, sales, orders, items)
    df = aggregate(df)
    df = rename_and_sort(df)
    return df



# Save to CSV


def save_csv(df, filepath):
    
    try:
        df.to_csv(filepath, sep=";", index=False)
        print(f"  Saved -> {filepath}")
    except Exception as e:
        print(f"Error saving CSV to {filepath}: {e}")
        raise



# Verify both solutions match


def verify(df_sql, df_pandas):
    
    match = df_sql.reset_index(drop=True).equals(df_pandas.reset_index(drop=True))
    if match:
        print("  Both solutions produce identical results.")
    else:
        print("  WARNING: Solutions do not match. Please review.")
    return match



# Main


def main():
    conn = None
    try:
        conn = get_connection(DB_PATH)

        print("\n=== Solution : Pure SQL ===")
        df_sql = solution_sql(conn)
        print(df_sql.to_string(index=False))
        save_csv(df_sql, "output_sql.csv")

        print("\n=== Solution : Pandas ===")
        df_pandas = solution_pandas(conn)
        print(df_pandas.to_string(index=False))
        save_csv(df_pandas, "output_pandas.csv")

        print("\n=== Verification ===")
        verify(df_sql, df_pandas)

    except Exception as e:
        print(f"\nScript failed: {e}")
    finally:
        if conn:
            conn.close()
            print("\nDatabase connection closed.")


if __name__ == "__main__":
    main()
