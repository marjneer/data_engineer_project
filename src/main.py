# from src.etl.load_customers import load_customers
# from src.etl.load_orders import load_orders
# from src.kpis.sql_kpis import generate_kpis

# def run_etl_and_kpis():
#     print("🚀 Starting ETL...")
#     load_customers("data/task_DE_new_customers.csv")
#     load_orders("data/task_DE_new_orders.xml")
#     print("✅ ETL Completed!\n")

#     print("🚀 Starting KPI generation...")
#     generate_kpis()
#     print("✅ KPI generation completed!")

# if __name__ == "__main__":
#     run_etl_and_kpis()

import os
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Setup logging
LOG_DIR = os.path.join(os.path.dirname(__file__), '..', 'logs')
os.makedirs(LOG_DIR, exist_ok=True)

log_file = os.path.join(LOG_DIR, f"pipeline_{datetime.now().strftime('%Y%m%d')}.log")

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)

def get_orders_file(data_dir):
    """
    Returns the orders file path.
    Tries yesterday's file first, then falls back to 'task_DE_new_orders.xml'.
    """
    from datetime import datetime, timedelta
    import os
    import logging

    # Try yesterday's file
    yesterday = datetime.now() - timedelta(days=1)
    file_name = f"orders_{yesterday.strftime('%Y%m%d')}.xml"
    file_path = os.path.join(data_dir, file_name)
    if os.path.exists(file_path):
        logging.info(f"Found yesterday's orders file: {file_path}")
        return file_path

    # Fallback to the actual file you have
    fallback_file = os.path.join(data_dir, "task_DE_new_orders.xml")
    if os.path.exists(fallback_file):
        logging.warning(f"Using fallback file: {fallback_file}")
        return fallback_file

    # No file found
    logging.error("No orders file found!")
    return None


from src.etl.load_customers import load_customers
from src.etl.load_orders import load_orders
from src.kpis.sql_kpis import generate_kpis
from src.kpis import pandas_kpis

def main():
    logging.info("=== Pipeline started ===")

    try:
        # 1️⃣ Load yesterday's orders file
        DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
        orders_file = get_orders_file(DATA_DIR)
        if not orders_file:
            logging.error("No input file found. Exiting pipeline.")
            return

        # 2️⃣ Run ETL loaders (CSV + XML to DB)
        logging.info("Starting ETL loaders...")
        customers_file = os.path.join(DATA_DIR, "task_DE_new_customers.csv")
        customers_df = load_customers(customers_file)  # CSV loader
        orders_df = load_orders(orders_file)  # XML loader
        logging.info("ETL loaders completed successfully.")

        # 3️⃣ Run SQL KPIs (if any DB KPIs exist)
        logging.info("Executing DB KPI SQL jobs...")
        generate_kpis()
        logging.info("DB KPI SQL jobs completed.")

        # 4️⃣ Run Pandas KPIs (in-memory)
        logging.info("Running Pandas KPI computations...")
        merged_df = pandas_kpis.preprocess(customers_df, orders_df)

        kpi1 = pandas_kpis.get_repeat_customers(merged_df)
        pandas_kpis.save_output(kpi1, "repeat_customers.csv")

        kpi2 = pandas_kpis.get_monthly_trends(merged_df)
        pandas_kpis.save_output(kpi2, "monthly_trends.csv")

        kpi3 = pandas_kpis.get_regional_revenue(merged_df)
        pandas_kpis.save_output(kpi3, "regional_revenue.csv")

        kpi4 = pandas_kpis.get_top_customers(merged_df)
        pandas_kpis.save_output(kpi4, "top_customers.csv")

        logging.info("Pandas KPI computations completed successfully.")

    except Exception as e:
        logging.exception(f"Pipeline failed: {e}")
    finally:
        logging.info("=== Pipeline finished ===")

if __name__ == "__main__":
    main()