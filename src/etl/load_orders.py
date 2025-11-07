import pandas as pd
from lxml import etree
from src.db.db_connection import get_connection

def load_orders(xml_file_path):
    try:
        tree = etree.parse(xml_file_path)
        root = tree.getroot()

        data = []
        for order in root.findall("order"):
            order_id_str = order.findtext("order_id")
            order_id_int = int(''.join(filter(str.isdigit, order_id_str)))

            data.append({
                "order_id": order_id_int,
                "mobile_number": order.findtext("mobile_number"),
                "order_date_time": order.findtext("order_date_time"),
                "sku_id": order.findtext("sku_id"),
                "sku_count": int(order.findtext("sku_count")),
                "total_amount": float(order.findtext("total_amount")),
            })

        df = pd.DataFrame(data)
        df['order_date_time'] = pd.to_datetime(df['order_date_time'])

        # Insert into DB
        conn = get_connection()
        cursor = conn.cursor()

        insert_query = """
        INSERT INTO orders (order_id, mobile_number, order_date_time, sku_id, sku_count, total_amount)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            order_date_time=VALUES(order_date_time),
            sku_id=VALUES(sku_id),
            sku_count=VALUES(sku_count),
            total_amount=VALUES(total_amount);
        """

        for _, row in df.iterrows():
            cursor.execute(insert_query, tuple(row))

        conn.commit()
        cursor.close()
        conn.close()
        print("✅ Orders loaded successfully")

        return df  # ✅ RETURN THE DATAFRAME

    except Exception as e:
        print(f"❌ Error loading orders: {e}")
        return pd.DataFrame()  # ✅ Return empty DF on error

