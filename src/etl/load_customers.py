import pandas as pd
from src.db.db_connection import get_connection
    
def load_customers(csv_file_path):
    try:
        df = pd.read_csv(csv_file_path)

        # Clean data
        df['customer_name'] = df['customer_name'].str.strip()
        df['region'] = df['region'].str.strip()
        df.drop_duplicates(subset=['customer_id'], inplace=True)

        # Convert alphanumeric IDs to integer
        df['customer_id'] = df['customer_id'].str.extract(r'(\d+)').astype(int)

        # Insert into DB
        conn = get_connection()
        cursor = conn.cursor()

        insert_query = """
        INSERT INTO customers (customer_id, customer_name, mobile_number, region)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            customer_name=VALUES(customer_name),
            region=VALUES(region);
        """

        for _, row in df.iterrows():
            cursor.execute(insert_query, tuple(row))

        conn.commit()
        cursor.close()
        conn.close()
        print("✅ Customers loaded successfully")

        return df  # ✅ Return the DataFrame for further processing

    except Exception as e:
        print(f"❌ Error loading customers: {e}")
        return pd.DataFrame()  # ✅ Return empty DataFrame on error

