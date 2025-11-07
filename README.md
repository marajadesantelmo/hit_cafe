# HIT Café – Pipeline de Actualización y Procesamiento de Datos

Este repositorio obtiene datos de la API de Fudo, actualiza datasets en formato CSV y genera archivos Excel procesados utilizados en BI.

## Qué contiene
- `update_data_api_fudo.py`: Descarga de forma incremental nuevas ventas/items desde Fudo y actualiza `data/*.csv`.
- `process_data.py`: Procesa los CSV y escribe los archivos Excel en `procesado/`.
- `main.py`: Orquestador de los dos pasos anteriores. También registra eventos en `logs/log.csv`.
- `logging_utils.py`: Pequeña utilidad para logs en CSV usada por el pipeline.
- `get_dataframe_fudo.py`: Script de una sola vez para descargar históricos completos.
- `utils.py`: Funciones de autenticación y utilidades de API.

## Cómo ejecutar
- Windows (batch):
  - Doble clic en `run_main.bat` o ejecútalo desde una terminal.
- Python directamente:
  - Desde la carpeta del repositorio:

```powershell
python .\main.py
```

El pipeline realizará:
1. Actualizar `data/ventas.csv` e `data/items.csv` con nuevos registros.
2. Generar `procesado/items.xlsx`, `procesado/ventas.xlsx` y `procesado/permanencia.xlsx`.
3. Agregar eventos y errores a `logs/log.csv`.

## Logs
- Archivo CSV de logs: `logs/log.csv` con columnas: `timestamp, level, source, message, error`.
- El archivo y la carpeta se crean automáticamente en la primera ejecución.

## Notas
- El código busca primero la ruta compartida `\\dc01\Usuarios\PowerBI\flastra\Documents\hit_cafe`. Si no existe, utiliza la ruta local del repositorio.
- Las credenciales de API se leen desde `tokens.py` mediante `utils.autenticar()`. Asegúrate de que esté configurado.

## Solución de problemas
- Si `main.py` falla, revisa `logs/log.csv` para ver el mensaje de error detallado.
- Asegúrate de que la carpeta `data/` contenga los CSV iniciales. Si no, ejecuta `get_dataframe_fudo.py` una vez para inicializar.
