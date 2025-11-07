# Sales Analytics Data Engineering Project

This project is a **data engineering pipeline** for processing daily customer and order data to generate business-ready KPIs. It supports both **database-driven processing (MySQL)** and **in-memory processing (Pandas)**. The outputs can be used for reporting, analysis, or visualization (e.g., Tableau).

## Key Features

- Ingests customer and order data from CSV/XML files
- Generates KPIs such as repeat customers, monthly trends, regional revenue, and top spenders
- Provides both SQL-based and Pandas-based pipelines
- Saves all KPI outputs as CSV files in a dedicated `outputs/` folder

---

## Setup (MySQL + Python + venv)

Follow these steps to set up the project environment:

### 1. Clone the project
```bash
git clone <your-repo-url>
cd <project-folder>
```

### 2. Create and activate a virtual environment

**For Windows:**
```bash
python -m venv venv
venv\Scripts\activate
```

**For macOS/Linux:**
```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Install required Python packages
```bash
pip install pandas mysql-connector-python python-dotenv lxml
```

### 4. MySQL Setup

- Install MySQL Server (any recent version)
- Create a database (e.g., `customer_orders`)
- Ensure the database has tables `customers` and `orders`
- Update `src/db/db_connection.py` with your MySQL credentials

### 5. Verify Connection
```python
from src.db.db_connection import get_connection
conn = get_connection()
print(conn)  # Should print connection object
```

---

## How to Run Ingestion + KPIs

### Using SQL Pipeline (MySQL)
```bash
python -m src.kpis.sql_kpis
```

This will generate CSV files in the `outputs/` folder:
- `repeat_customers.csv`
- `monthly_trends.csv`
- `regional_revenue.csv`
- `top_spenders_30d.csv`

### Using Pandas Pipeline (In-memory)
```bash
python -m src.main
```

This will generate CSV files in the `outputs/` folder:
- `repeat_customers_pd.csv`
- `monthly_trends_pd.csv`
- `regional_revenue_pd.csv`
- `top_customers_pd.csv`

---

##Project Structure
DATA_ENGINEER_PROJECT/
├── data/
│   ├── task_DE_new_customers.csv
│   └── task_DE_new_orders.xml
├── logs/
├── outputs/
│   ├── monthly_trends.csv
│   ├── regional_revenue.csv
│   ├── repeat_customers.csv
│   ├── top_customers.csv
│   └── top_spenders_30d.csv
├── sql/
│   └── schema.sql
├── src/
│   ├── db/
│   │   ├── __init__.py
│   │   └── db_connection.py
│   ├── etl/
│   │   ├── __init__.py
│   │   ├── load_customers.py
│   │   └── load_orders.py
│   ├── kpis/
│   │   ├── __init__.py
│   │   ├── pandas_kpis.py
│   │   └── sql_kpis.py
│   └── __init__.py
├── main.py
├── .env.example
├── .gitignore
├── README.md
└── requirements.txt

## ✅ Improvements / Next Steps

- Make the pipeline **scalable** using Airflow or other orchestrators.
- Store raw and processed data on **S3** or another cloud storage.
- Add **automated tests** for data quality and KPI validation.
- Generate **Tableau dashboards** directly from outputs.
- Support **incremental ingestion** for daily files.
