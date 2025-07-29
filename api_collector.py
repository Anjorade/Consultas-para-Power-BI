import os
import requests
import json
import time
from datetime import datetime
from urllib.parse import quote

# ======================================
# CONFIGURACIÓN (SEGURA CON VARIABLES DE ENTORNO)
# ======================================
TOKEN = os.getenv("API_TOKEN")  # Cambiado el nombre del secret
BASE_URL = os.getenv("API_BASE_URL")  # Cambiado el nombre del secret
HEADERS = {"token": TOKEN}

# Configuración de comportamiento
MAX_RETRIES = 2           # Reintentos por consulta fallida
REQUEST_DELAY = 30        # Espera entre consultas (segundos)
RETRY_DELAY = 10          # Espera entre reintentos (segundos)

# ======================================
# DEFINICIÓN DE ENDPOINTS
# ======================================
ENDPOINTS = {
    "Consulta_1": "System.MaterialTransactions.List.View1",
    "Consulta_2": "System.MaterialTransactions.List.View1",
    "Consulta_3": "System.MaterialTransactions.List.View1",
}

# ======================================
# CONFIGURACIÓN DE CONSULTAS (ACTUALIZADA)
# ======================================
QUERY_CONFIG = [
    {
        "name": "Consulta_1",
        "params": {
            "orderby": "ctxn_transaction_date desc",
            "take": "30000",
            "where": "ctxn_movement_type ilike '261%25%' and (ctxn_transaction_date > current_date - 120) and ctxn_warehouse_code ilike '1145' and not (ctxn_primary_uom_code ilike 'Und'"
        }
    },
    {
        "name": "Consulta_2",
        "params": {
            "orderby": "ctxn_transaction_date desc",
            "take": "30000",
            "where": "ctxn_movement_type ilike '261%25%' and (ctxn_transaction_date > current_date - 120) and ctxn_warehouse_code ilike '1145' and ctxn_primary_uom_code ilike 'Und'"
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

# ======================================
# FUNCIONES PRINCIPALES (ACTUALIZADAS)
# ======================================
def build_url(endpoint, params):
    """Construye URL con codificación segura"""
    encoded_params = []
    for key, value in params.items():
        str_key = str(key)
        str_value = str(value)
        encoded_key = quote(str_key)
        encoded_value = quote(str_value)
        encoded_params.append(f"{encoded_key}={encoded_value}")
    
    return f"{BASE_URL}{endpoint}?{'&'.join(encoded_params)}"

def fetch_api_data(url, query_name):
    """Obtiene datos con manejo robusto de errores"""
    for attempt in range(MAX_RETRIES + 1):
        try:
            print(f"\nℹ️  Consultando {query_name} (Intento {attempt + 1}/{MAX_RETRIES + 1})")
            response = requests.get(url, headers=HEADERS, timeout=60)
            response.raise_for_status()
            
            data = response.json()
            if not data:
                print(f"⚠️  {query_name} devolvió datos vacíos")
                return None
                
            # Convertir a lista de diccionarios (en lugar de DataFrame)
            for item in data:
                item['load_timestamp'] = datetime.now().isoformat()
                item['query_name'] = query_name
            
            print(f"✅ {query_name} - {len(data)} registros obtenidos")
            return data
            
        except requests.exceptions.RequestException as e:
            if attempt == MAX_RETRIES:
                print(f"❌ {query_name} falló después de {MAX_RETRIES} reintentos: {str(e)}")
                return None
            print(f"⏳ Esperando {RETRY_DELAY}s antes de reintentar...")
            time.sleep(RETRY_DELAY)

def process_queries():
    """Procesa todas las consultas"""
    print("\n🔍 PROCESANDO CONSULTAS")
    queries_data = {}
    
    for config in QUERY_CONFIG:
        url = build_url(ENDPOINTS[config["name"]], config["params"])
        data = fetch_api_data(url, config["name"])
        
        if data is not None:
            queries_data[config["name"]] = data
        
        if config != QUERY_CONFIG[-1]:
            print(f"⏳ Pausa de {REQUEST_DELAY}s entre consultas...")
            time.sleep(REQUEST_DELAY)
    
    return queries_data

def save_data(data):
    """Guarda los datos en archivos JSON"""
    if not data:
        print("❌ No hay datos para guardar")
        return False
    
    os.makedirs("data", exist_ok=True)
    success = True
    
    for name, records in data.items():
        filename = f"data/{name}.json"  # Cambiado a JSON
        try:
            with open(filename, 'w') as f:
                json.dump(records, f, indent=2)
            size_mb = os.path.getsize(filename) / (1024 * 1024)
            print(f"💾 {filename} - {len(records)} registros ({size_mb:.2f} MB)")
        except Exception as e:
            print(f"❌ Error guardando {filename}: {str(e)}")
            success = False
    
    return success

# ======================================
# EJECUCIÓN PRINCIPAL
# ======================================
def main():
    """Función principal con manejo estructurado de errores"""
    print("\n🚀 INICIANDO RECOLECTOR DE DATOS")
    start_time = time.time()
    
    try:
        queries_data = process_queries()
        save_data(queries_data)
            
    except Exception as e:
        print(f"\n💥 ERROR CRÍTICO: {str(e)}")
        raise
    
    finally:
        duration = time.time() - start_time
        print(f"\n⌛ PROCESO COMPLETADO EN {duration:.2f} SEGUNDOS")

if __name__ == "__main__":
    main()
