import os
import pandas as pd
import warnings
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SAWarning
from dotenv import load_dotenv

load_dotenv()

# --- 1. KONEKSI ---
def get_mssql_engine():
    """Koneksi ke SQL Server (Source)."""
    user = os.getenv("MSSQL_USER")
    password = os.getenv("MSSQL_PASS")
    server = os.getenv("MSSQL_SERVER")
    database = os.getenv("MSSQL_DB")
    driver = "ODBC Driver 17 for SQL Server"
    conn_str = (
        f"DRIVER={driver};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password};"
    )               
    return create_engine(f"mssql+pyodbc:///?odbc_connect={conn_str}") 
def get_postgres_engine():
    """Koneksi ke PostgreSQL (Target)."""
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASS")
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT")
    database = os.getenv("POSTGRES_DB")
    
    return create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")

# --- 2. TAHAPAN Extract ---
def extract(query):
    """Mengambil data dari SQL Server."""
    print(" 1/3 Extracting data from SQL Server...")
    engine = get_mssql_engine()
    with engine.connect() as conn:
        df = pd.read_sql_query(text(query), conn)
    return df

# 3. Tahapan transform
def transform(df):
    """Membersihkan dan mengolah data."""
    print("2/3 Transforming data...")
    
    # 1. Menggabungkan Nama Depan dan Belakang
    df['full_name'] = df['FirstName'] + ' ' + df['LastName']
    
    # 2. Menghitung Umur berdasarkan BirthDate
    current_year = datetime.now().year
    df['BirthDate'] = pd.to_datetime(df['BirthDate'])
    df['age'] = current_year - df['BirthDate'].dt.year
    
    # 3. Memilih kolom yang dibutuhkan saja dan merapikan nama kolom
    df_clean = df[['full_name', 'JobTitle', 'age', 'BirthDate']].copy()
    df_clean.columns = ['full_name', 'job_title', 'age', 'birth_date']
    
    return df_clean

# 4. tahapan load
def load(df, table_name):
    """Memasukkan data ke PostgreSQL."""
    print(f"3/3 Loading data into PostgreSQL table: {table_name}...")
    engine = get_postgres_engine()
    

    df.to_sql(table_name, engine, if_exists='replace', index=False)
    print("Load Berhasil!")

# --- 5. MAIN RUNNER ---
def run_etl():
    print("ETL Process Started")
    print("-" * 30)
    
    # SQL Query untuk Extract
    query = """
    SELECT TOP 100 
        P.FirstName, P.LastName, E.JobTitle, E.BirthDate
    FROM Person.Person P
    JOIN HumanResources.Employee E ON P.BusinessEntityID = E.BusinessEntityID
    """
    
    try:
        # Jalankan Pipeline
        data_raw = extract(query)
        data_transformed = transform(data_raw)
        load(data_transformed, "dim_employee_silver")
        
        print("-" * 30)
        print(f"ETL Selesai! {len(data_transformed)} baris diproses.")
        
    except Exception as e:
        print(f"ETL Failed: {e}")

if __name__ == "__main__":
    run_etl()