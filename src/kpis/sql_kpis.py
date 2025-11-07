import pandas as pd
from src.db.db_connection import get_connection
import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(PROJECT_ROOT, 'outputs')
os.makedirs(OUTPUT_DIR, exist_ok=True)

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

def run_query_to_csv(query, filename):
    conn = get_connection()
    if conn is None:
        print("❌ Cannot connect to DB")
        return
    df = pd.read_sql(query, conn)
    conn.close()
    output_path = os.path.join(OUTPUT_DIR, filename)
    df.to_csv(output_path, index=False)
    print(f"✅ {filename} saved")

def generate_kpis():
    print("🚀 Generating KPIs...")

    # 1️⃣ Repeat Customers
    repeat_customers_query = """
    SELECT mobile_number
    FROM orders
    GROUP BY mobile_number
    HAVING COUNT(order_id) > 1;
    """
    run_query_to_csv(repeat_customers_query, "repeat_customers.csv")

    # 2️⃣ Monthly Trends
    monthly_trends_query = """
    SELECT DATE_FORMAT(order_date_time, '%Y-%m') AS month,
           COUNT(*) AS order_count,
           SUM(total_amount) AS revenue
    FROM orders
    GROUP BY month;
    """
    run_query_to_csv(monthly_trends_query, "monthly_trends.csv")

    # 3️⃣ Regional Revenue
    regional_revenue_query = """
    SELECT c.region, SUM(o.total_amount) AS revenue
    FROM orders o
    JOIN customers c USING(mobile_number)
    GROUP BY c.region;
    """
    run_query_to_csv(regional_revenue_query, "regional_revenue.csv")

    # 4️⃣ Top Spenders (Last 30 Days)
    top_spenders_query = """
    SELECT c.customer_name,
           SUM(o.total_amount) AS total_spend
    FROM orders o
    JOIN customers c USING(mobile_number)
    WHERE order_date_time >= NOW() - INTERVAL 30 DAY
    GROUP BY c.customer_name
    ORDER BY total_spend DESC
    LIMIT 10;
    """
    run_query_to_csv(top_spenders_query, "top_spenders_30d.csv")

    print("✅ All KPIs generated")
