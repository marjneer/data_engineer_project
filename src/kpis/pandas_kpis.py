import pandas as pd
import os

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(PROJECT_ROOT, '..', 'outputs')
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')


def load_data():
    customers = pd.read_csv(os.path.join(DATA_DIR, "task_DE_new_customers.csv"))
    orders = pd.read_xml(os.path.join(DATA_DIR, "task_DE_new_orders.xml"))
    return customers, orders

def preprocess(customers, orders):
    import pandas as pd

    # Convert order_date_time to datetime
    orders['order_date_time'] = pd.to_datetime(orders['order_date_time'], errors='coerce')

    # Drop rows with missing mobile numbers
    customers.dropna(subset=['mobile_number'], inplace=True)
    orders.dropna(subset=['mobile_number'], inplace=True)

    # Ensure both columns are strings and strip whitespace
    customers['mobile_number'] = customers['mobile_number'].astype(str).str.strip()
    orders['mobile_number'] = orders['mobile_number'].astype(str).str.strip()

    # Merge
    merged = orders.merge(customers, on='mobile_number', how='left')
    return merged


def get_repeat_customers(df):
    repeat = df.groupby('mobile_number').filter(lambda x: len(x) > 1)
    return repeat[['customer_id', 'customer_name', 'mobile_number']].drop_duplicates()

def get_monthly_trends(df):
    df['month'] = df['order_date_time'].dt.to_period('M')
    monthly = df.groupby('month')['total_amount'].sum().reset_index()
    return monthly

def get_regional_revenue(df):
    regional = df.groupby('region')['total_amount'].sum().reset_index()
    return regional

def get_top_customers(df, top_n=10):
    top = df.groupby(['customer_id', 'customer_name'])['total_amount'].sum().reset_index()
    return top.sort_values(by='total_amount', ascending=False).head(top_n)

def save_output(df, filename):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = os.path.join(OUTPUT_DIR, filename)
    df.to_csv(path, index=False)
    print(f"✅ Saved {filename} at {path}")

if __name__ == "__main__":
    customers, orders = load_data()
    merged_df = preprocess(customers, orders)

    kpi1 = get_repeat_customers(merged_df)
    save_output(kpi1, "repeat_customers_pd.csv")

    kpi2 = get_monthly_trends(merged_df)
    save_output(kpi2, "monthly_trends_pd.csv")

    kpi3 = get_regional_revenue(merged_df)
    save_output(kpi3, "regional_revenue_pd.csv")

    kpi4 = get_top_customers(merged_df)
    save_output(kpi4, "top_customers_pd.csv")

    print("✅ KPI generation complete! Check /outputs folder.")
