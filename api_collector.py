import os
import requests
import pandas as pd
import time
from datetime import datetime
from urllib.parse import quote

# Configuración segura desde secretos
TOKEN = os.getenv("API_TOKEN")
BASE_URL = os.getenv("API_BASE_URL")
HEADERS = {"token": TOKEN}

MAX_RETRIES = 1
REQUEST_DELAY = 20
RETRY_DELAY = 10

# ENDPOINTS como los del ejemplo original
ENDPOINTS = {
    "Consulta_1": "System.MaterialTransactions.List.View1",
    "Consulta_2": "System.MaterialTransactions.List.View1",
    "Consulta_3": "System.MaterialTransactions.List.View1"
}

# Configuración de las consultas (ya no parametrizadas por almacén)
QUERY_CONFIG = [
    {
        "name": "Consulta_1",
        "params": {
            "orderby": "ctxn_transaction_date desc",
            "take": "30000",
            "where": "ctxn_movement_type ilike '261%25%'"
        }
    },
    {
        "name": "Consulta_2",
        "params": {
            "orderby": "ctxn_transaction_date desc",
            "take": "30000",
            "where": "ctxn_item_code1 ilike '10007411'"
        }
    },
    {
        "name": "Consulta_3",
        "params": {
            "orderby": "ctxn_transaction_date desc",
            "take": "30000",
            "where": "ctxn_movement_type ilike '261%25%' and (ctxn_transaction_date > current_date - 120) and ctxn_warehouse_code ilike '1290'"
        }
    }
]

def build_url(endpoint, params):
    encoded_params = []
    for key, value in params.items():
        encoded_key = quote(str(key))
        encoded_value = quote(str(value))
        encoded_params.append(f"{encoded_key}={encoded_value}")
    return f"{BASE_URL}{endpoint}?{'&'.join(encoded_params)}"

def fetch_data(url, name):
    for attempt in range(MAX_RETRIES + 1):
        try:
            print(f"\n🔎 Consultando {name} (Intento {attempt + 1})")
            response = requests.get(url, headers=HEADERS, timeout=60)
            response.raise_for_status()
            data = response.json()
            if not data:
                print(f"⚠️  {name} no devolvió datos.")
                return None
            df = pd.json_normalize(data)
            df["load_timestamp"] = datetime.now().isoformat()
            return df
        except requests.exceptions.RequestException as e:
            print(f"❌ Error en {name}: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
            else:
                return None

def save_data(df, name):
    os.makedirs("data", exist_ok=True)
    path = f"data/{name}.json"
    df.to_json(path, orient="records", indent=2)
    print(f"💾 Guardado: {path} - {len(df)} registros")

def main():
    print("🚀 Iniciando consultas para Power BI")
    start_time = time.time()

    for query in QUERY_CONFIG:
        name = query["name"]
        url = build_url(ENDPOINTS[name], query["params"])
        df = fetch_data(url, name)
        if df is not None:
            save_data(df, name)
        time.sleep(REQUEST_DELAY)

    duration = time.time() - start_time
    print(f"✅ Proceso finalizado en {duration:.2f} segundos.")

if __name__ == "__main__":
    main()
