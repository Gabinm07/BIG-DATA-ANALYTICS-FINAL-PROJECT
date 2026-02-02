import os
import glob
import pandas as pd
import matplotlib.pyplot as plt

# Folder where your *_csv directories live
BASE = r"C:\Users\Gahigi\Desktop\Ecommerce-analytics\outputs"

# Each Spark output is a FOLDER that contains part-xxxxx.csv
CSV_DIRS = {
    "revenue": "revenue_by_category_top10_csv",
    "units": "units_sold_by_category_top10_csv",
    "spenders": "top_spenders_top10_csv",
    "orders": "top_users_by_orders_top10_csv",
    "pairs": "also_bought_pairs_top10_csv",
}

def first_csv(folder):
    files = glob.glob(os.path.join(folder, "*.csv"))
    if not files:
        raise FileNotFoundError(f"No CSV found in: {folder}")
    return files[0]

def pick_col(df, candidates, label):
    for c in candidates:
        if c in df.columns:
            return c
    raise KeyError(
        f"[{label}] None of these columns exist: {candidates}\n"
        f"Available columns: {list(df.columns)}"
    )

def save_barh(df, x, y, title, out_png):
    df = df.copy()

    # y numeric
    df[y] = pd.to_numeric(df[y], errors="coerce")
    df = df.dropna(subset=[y])

    # clean labels
    df[x] = df[x].astype(str).fillna("")
    df = df[df[x].str.strip() != ""]

    # sort so biggest appears at top
    df = df.sort_values(y, ascending=True)

    plt.figure(figsize=(12, 6))
    plt.barh(df[x], df[y])
    plt.title(title)
    plt.xlabel(y)
    plt.tight_layout()
    plt.savefig(out_png, dpi=200)
    plt.close()

def main():
    print(f"📊 Generating plots from: {BASE}")

    # ---------- 1) Revenue by category ----------
    rev_csv = first_csv(os.path.join(BASE, CSV_DIRS["revenue"]))
    df = pd.read_csv(rev_csv)

    x = pick_col(df, ["category_id", "category", "_id", "category_name"], "revenue")
    y = pick_col(df, ["revenue", "total_revenue", "amount"], "revenue")

    save_barh(df, x, y, "Top 10 Categories by Revenue",
              os.path.join(BASE, "01_revenue_by_category.png"))
    print("✅ 01_revenue_by_category.png")

    # ---------- 2) Units sold by category ----------
    units_csv = first_csv(os.path.join(BASE, CSV_DIRS["units"]))
    df = pd.read_csv(units_csv)

    x = pick_col(df, ["category_id", "category", "_id", "category_name"], "units")
    # THIS IS THE FIX: allow many possible names
    y = pick_col(df, ["units_sold", "units", "quantity", "count", "total_units"], "units")

    save_barh(df, x, y, "Top 10 Categories by Units Sold",
              os.path.join(BASE, "02_units_sold_by_category.png"))
    print("✅ 02_units_sold_by_category.png")

    # ---------- 3) Top spenders ----------
    spend_csv = first_csv(os.path.join(BASE, CSV_DIRS["spenders"]))
    df = pd.read_csv(spend_csv)

    x = pick_col(df, ["user_id", "_id", "user"], "spenders")
    y = pick_col(df, ["total_spent", "spent", "revenue", "total"], "spenders")

    save_barh(df, x, y, "Top 10 Users by Total Spending",
              os.path.join(BASE, "03_top_spenders.png"))
    print("✅ 03_top_spenders.png")

    # ---------- 4) Top users by orders ----------
    orders_csv = first_csv(os.path.join(BASE, CSV_DIRS["orders"]))
    df = pd.read_csv(orders_csv)

    x = pick_col(df, ["user_id", "_id", "user"], "orders")
    y = pick_col(df, ["num_orders", "orders", "order_count", "count"], "orders")

    save_barh(df, x, y, "Top 10 Users by Number of Orders",
              os.path.join(BASE, "04_top_users_by_orders.png"))
    print("✅ 04_top_users_by_orders.png")

    # ---------- 5) Also-bought pairs ----------
    pairs_csv = first_csv(os.path.join(BASE, CSV_DIRS["pairs"]))
    df = pd.read_csv(pairs_csv)

    # Try to build a readable label
    ax = pick_col(df, ["product_a_name", "product_x_name", "product_x", "product_a"], "pairs")
    bx = pick_col(df, ["product_b_name", "product_y_name", "product_y", "product_b"], "pairs")
    y  = pick_col(df, ["co_count", "co_purchase_count", "count", "frequency"], "pairs")

    df["pair"] = df[ax].astype(str) + " + " + df[bx].astype(str)

    save_barh(df, "pair", y, "Top 10 Also-Bought Product Pairs",
              os.path.join(BASE, "05_also_bought_pairs.png"))
    print("✅ 05_also_bought_pairs.png")

    print("\nDONE ✅ All plots saved inside:", BASE)

if __name__ == "__main__":
    main()
