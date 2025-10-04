import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
import requests
from datetime import datetime

# Configuración de la base de datos
db_config = {
    'user': 'root',           
    'password': '12345', 
    'host': 'localhost',
    'database': 'ventas'
}


try:
    conn = mysql.connector.connect(
        user=db_config['user'],
        password=db_config['password'],
        host=db_config['host']
    )
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_config['database']}")
    cursor.execute(f"USE {db_config['database']}")
    conn.close()
except mysql.connector.Error as err:
    print(f"Error al conectar o crear la base de datos: {err}")
    exit()


engine = create_engine(f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}")

#Lectura de archivos CSV
products_df = pd.read_csv('C:/Users/El amor de tu vida/OneDrive/ITLA/2025 c3/BIG DATA/etl/ventas/data/Archivo CSV Análisis de Ventas-20250924/products.csv')
customers_df = pd.read_csv('C:/Users/El amor de tu vida/OneDrive/ITLA/2025 c3/BIG DATA/etl/ventas/data/Archivo CSV Análisis de Ventas-20250924/customers.csv')
orders_df = pd.read_csv('C:/Users/El amor de tu vida/OneDrive/ITLA/2025 c3/BIG DATA/etl/ventas/data/Archivo CSV Análisis de Ventas-20250924/orders.csv')
order_details_df = pd.read_csv('C:/Users/El amor de tu vida/OneDrive/ITLA/2025 c3/BIG DATA/etl/ventas/data/Archivo CSV Análisis de Ventas-20250924/order_details.csv')

#Integración de datos desde una API 
def fetch_external_data(product_ids):
    external_data = {pid: {'updated_price': 10.50} for pid in product_ids}  # Ejemplo ficticio
    return external_data

product_ids = products_df['ProductID'].tolist()
external_data = fetch_external_data(product_ids)
products_df['UpdatedPrice'] = products_df['ProductID'].map(lambda x: external_data.get(x, {}).get('updated_price', products_df.loc[products_df['ProductID'] == x, 'Price'].iloc[0]))

#Procesamiento de datos

# Limpieza de valores nulos
products_df = products_df.dropna()
customers_df = customers_df.dropna(subset=['Email'])
orders_df = orders_df.dropna()
order_details_df = order_details_df.dropna()

# Eliminación de duplicados
products_df = products_df.drop_duplicates(subset=['ProductID'])
customers_df = customers_df.drop_duplicates(subset=['Email'])
orders_df = orders_df.drop_duplicates(subset=['OrderID'])
order_details_df = order_details_df.drop_duplicates(subset=['OrderID', 'ProductID'])

# Normalización de formatos
products_df['Price'] = products_df['Price'].astype(float).round(2)
orders_df['OrderDate'] = pd.to_datetime(orders_df['OrderDate'], errors='coerce').dt.date
order_details_df['Quantity'] = order_details_df['Quantity'].astype(int)

# Normalización de la columna Phone
customers_df['Phone'] = customers_df['Phone'].str.replace(r'\s+', '', regex=True)  # Elimina espacios
customers_df['Phone'] = customers_df['Phone'].str.replace(r'[^\w\-\+]', '', regex=True) 
customers_df['Phone'] = customers_df['Phone'].str.slice(0, 50)  # Limita a 50 caracteres


valid_customers = customers_df['CustomerID'].tolist()
orders_df = orders_df[orders_df['CustomerID'].isin(valid_customers)]

valid_products = products_df['ProductID'].tolist()
order_details_df = order_details_df[order_details_df['ProductID'].isin(valid_products)]

# Unir order_details_df con products_df para obtener el precio
order_details_df = order_details_df.merge(products_df[['ProductID', 'Price', 'UpdatedPrice']], on='ProductID', how='left')

# Calcular campos 
order_details_df['TotalPrice'] = order_details_df['Quantity'] * order_details_df['UpdatedPrice'].fillna(order_details_df['Price'])

# 4. Carga de datos en la base de datos
products_df[['ProductID', 'ProductName', 'Category', 'Price', 'Stock']].to_sql('products', engine, if_exists='replace', index=False)
customers_df.to_sql('customers', engine, if_exists='replace', index=False)
orders_df.to_sql('orders', engine, if_exists='replace', index=False)
order_details_df[['OrderID', 'ProductID', 'Quantity', 'TotalPrice']].to_sql('order_details', engine, if_exists='replace', index=False)

print("Pipeline completado con éxito a las", datetime.now().strftime("%I:%M %p AST el %d/%m/%Y"))