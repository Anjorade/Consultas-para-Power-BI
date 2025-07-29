# Consultas para Power BI

Repositorio para automatizar consultas API que alimentan informes de Power BI.

## Archivos principales
- `Consulta_1.json`: Datos actualizados de transacciones tipo 261 en unidades
- `Consulta_2.json`: Datos actualizados de transacciones tipo 261 general

## Configuración requerida
1. Secrets de GitHub:
   - `API_TOKEN`: Token de autenticación
   - `API_BASE_URL`: URL base de la API
2. Variable:
   - `WAREHOUSES`: Lista de almacenes como JSON (ej. '["1145", "1290"]')

## Programación
Ejecución diaria a las 3 AM (hora Colombia)
