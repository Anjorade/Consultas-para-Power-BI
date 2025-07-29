import os
import requests
import json
import time
from datetime import datetime
from urllib.parse import quote

# Configuración desde variables de entorno
TOKEN = os.getenv("API_TOKEN")
BASE_URL = os.getenv("API_BASE_URL")
HEADERS = {"token": TOKEN}
WAREHOUSES = json.loads(os.getenv("WAREHOUSES", '[]'))

# Configuración de ejecución
MAX_RETRIES = 2
REQUEST_DELAY = 30
RETRY_DELAY = 10

# Definición de consultas
QUERIES = [
    {
        "id": "Consulta_1",
        "output_file": "Consulta_1.json",
        "endpoint": "/System.MaterialTransactions.List.View1",
        "params": {
            "orderby": "ctxn_transaction_date desc",
            "take": "30000",
            "where": "ctxn_movement_type ilike '261%25%' and (ctxn_transaction_date > current_date - 120) and ctxn_warehouse_code ilike '{warehouse}' and ctxn_primary_uom_code ilike 'Und'"
        },
        "use_warehouse": True
    },
    {
        "id": "Consulta_2",
        "output_file": "Consulta_2.json",
        "endpoint": "/System.MaterialTransactions.List.View1",
        "params": {
            "orderby": "ctxn_transaction_date desc",
            "take": "30000",
            "where": "ctxn_movement_type ilike '261%25%' and (ctxn_transaction_date > current_date - 120) and ctxn_warehouse_code ilike '{warehouse}'"
        },
        "use_warehouse": True
    }
]

def build_url(query_config, warehouse=None):
    params = query_config["params"].copy()
    
    if query_config.get("use_warehouse", False) and warehouse:
        params["where"] = params["where"].format(warehouse=warehouse)
    
    return f"{BASE_URL}{query_config['endpoint']}?{'&'.join(f'{quote(k)}={quote(str(v))}' for k, v in params.items())}"

def execute_query(query_config, warehouse=None):
    url = build_url(query_config, warehouse)
    
    for attempt in range(MAX_RETRIES + 1):
        try:
            print(f"\n▶️ Ejecutando {query_config['id']} (Intento {attempt + 1})")
            response = requests.get(url, headers=HEADERS, timeout=45)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            if attempt == MAX_RETRIES:
                print(f"❌ Error en {query_config['id']}: {str(e)}")
                return None
            time.sleep(RETRY_DELAY)

def save_data(data, filename):
    if not data:
        return False
        
    try:
        with open(f"data/{filename}", 'w') as f:
            json.dump(data, f)
        print(f"💾 Datos guardados en data/{filename}")
        return True
    except Exception as e:
        print(f"❌ Error guardando datos: {str(e)}")
        return False

def main():
    print("\n🔷 INICIANDO CONSULTAS PARA POWER BI 🔷")
    start_time = time.time()
    
    try:
        for query in QUERIES:
            if not query.get("use_warehouse", False):
                data = execute_query(query)
                if data:
                    save_data(data, query["output_file"])
            else:
                for warehouse in WAREHOUSES:
                    data = execute_query(query, warehouse)
                    if data:
                        save_data(data, query["output_file"])
            
            if query != QUERIES[-1]:
                time.sleep(REQUEST_DELAY)
    
    finally:
        print(f"\n⏱️ Tiempo total: {(time.time() - start_time)/60:.2f} minutos")

if __name__ == "__main__":
    main()
