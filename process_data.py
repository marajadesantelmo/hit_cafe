#%% Importo paquetes
import os
from datetime import datetime, timedelta
from tokens import url_supabase, key_supabase
from supabase import create_client, Client
supabase_client = create_client(url_supabase, key_supabase)
import pandas as pd
from logging_utils import log_event

def insert_table_data(table_name, data):
    for record in data:
        try:
            supabase_client.from_(table_name).insert(record).execute()
        except Exception as e:
            print(f"Error inserting record into {table_name}: {e}")

def _get_base_dir() -> str:
    """Return the preferred base directory for data files."""
    network = r"\\dc01\Usuarios\PowerBI\flastra\Documents\hit_cafe"
    local = r'C:\Users\facun\OneDrive\Documentos\GitHub\hit_cafe'
    server = r'\\dc01\Usuarios\PowerBI\flastra\Documents\hit_cafe'
    return network if os.path.exists(network) else local if os.path.exists(local) else server


def run_processing() -> dict:
    """Process CSVs under data/ and write Excel files under procesado/.

    Returns a small summary dict with basic counts.
    """
    base_dir = _get_base_dir()
    data_dir = os.path.join(base_dir, 'data')
    out_dir = os.path.join(base_dir, 'procesado')
    os.makedirs(out_dir, exist_ok=True)
    print('Abriendo bases y actualizando metricas')
    log_event("INFO", "process_data", "Abriendo bases y actualizando metricas")
    current_date = datetime.now()
    first_day_of_current_month = current_date.replace(day=1)
    last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
    # last_month = last_day_of_previous_month.strftime("%Y-%m")
    # this_month = first_day_of_current_month.strftime("%Y-%m")
    # this_year = current_date.strftime("%Y")

    # Abro bases
    ventas = pd.read_csv(os.path.join(data_dir, 'ventas.csv'))
    items = pd.read_csv(os.path.join(data_dir, 'items.csv'))
    all_products = pd.read_csv(os.path.join(data_dir, 'productos_categorias.csv'))
    all_products['product_name'] = all_products['product_name'].apply(
        lambda x: x[:20] + '...' if len(x) > 20 else x
    )

    # Formateo ventas
    ventas['createdAt'] = pd.to_datetime(ventas['createdAt'])
    ventas['closedAt'] = pd.to_datetime(ventas['closedAt'])
    # Convert to timezone-naive before creating period
    ventas['createdAt_naive'] = ventas['createdAt'].dt.tz_localize(None)
    ventas['mes'] = ventas['createdAt_naive'].dt.to_period('M')
    ventas['permanencia'] = ventas['closedAt'] - ventas['createdAt']
    ventas['sale_id'] = ventas['sale_id'].astype(str)

    # Formateo items - quito items cancelados
    items = items[items['canceled'] != True].copy()
    # Join de items con su sale_id y mes
    items['sale_id'] = items['sale_id'].astype(str)
    # Join por sale_id y Sucursal para evitar cruces entre sucursales
    if 'Sucursal' in items.columns and 'Sucursal' in ventas.columns:
        items = items.merge(ventas, on=['sale_id', 'Sucursal'], how='left').copy()
    else:
        # Backward compatibility si falta la columna
        items = items.merge(ventas, on='sale_id', how='left').copy()
    # Join de items con su nombre
    # Join con productos por product_id y Sucursal
    if 'Sucursal' in items.columns and 'Sucursal' in all_products.columns:
        items = items.merge(all_products, on=['product_id', 'Sucursal'], how='left')
    else:
        items = items.merge(all_products, on='product_id', how='left')

    # Permanencia promedio por mes
    permanencia = ventas[ventas['saleType'] == 'EAT-IN']
    permanencia = permanencia[
        (permanencia['permanencia'] >= pd.Timedelta(minutes=50)) &
        (permanencia['permanencia'] < pd.Timedelta(hours=4))
    ]
    permanencia['createdAt'] = permanencia['createdAt'].dt.tz_localize(None)
    permanencia['closedAt'] = permanencia['closedAt'].dt.tz_localize(None)

    # Convert datetime columns to be timezone-unaware
    items['createdAt'] = items['createdAt'].dt.tz_localize(None)
    items['closedAt'] = items['closedAt'].dt.tz_localize(None)
    ventas['createdAt'] = ventas['createdAt'].dt.tz_localize(None)
    ventas['closedAt'] = ventas['closedAt'].dt.tz_localize(None)

    # Keep only info on date in YYYY-MM-DD format
    items['createdAt'] = items['createdAt'].dt.date
    items['closedAt'] = items['closedAt'].dt.date
    ventas['createdAt'] = ventas['createdAt'].dt.date
    ventas['closedAt'] = ventas['closedAt'].dt.date
    permanencia['createdAt'] = permanencia['createdAt'].dt.date
    permanencia['closedAt'] = permanencia['closedAt'].dt.date
    # Guardado
    log_event('INFO', 'process_data', 'Guardando dataframes en excel')
    items.to_excel(os.path.join(out_dir, 'items.xlsx'), index=False)
    ventas.to_excel(os.path.join(out_dir, 'ventas.xlsx'), index=False)
    permanencia.to_excel(os.path.join(out_dir, 'permanencia.xlsx'), index=False)

    # Proceso datos y subo información a SQL en supabase
    # Ventas diarias
    ventas_diarias_gral = ventas.groupby(ventas['createdAt']).agg({'total': 'sum'}).reset_index()
    ventas_diarias_gral.columns = ['Fecha', 'Venta General']
    ventas_diarias_gral = ventas_diarias_gral[ventas_diarias_gral['Venta General'] > 0]
    ventas_diarias_gral['Fecha'] = ventas_diarias_gral['Fecha'].astype(str)
    ventas_diarias_gral['Venta General'] = ventas_diarias_gral['Venta General'].round(0).astype(int)

    # Ventas por sucursal
    ventas_por_sucursal = ventas.groupby(['Sucursal', ventas['createdAt']]).agg({'total': 'sum'}).reset_index()
    ventas_por_sucursal.columns = ['Sucursal', 'Fecha', 'Venta Sucursal']
    ventas_por_sucursal = ventas_por_sucursal[ventas_por_sucursal['Venta Sucursal'] > 0]
    ventas_por_sucursal['Fecha'] = ventas_por_sucursal['Fecha'].astype(str)
    ventas_por_sucursal['Venta Sucursal'] = ventas_por_sucursal['Venta Sucursal'].round(0).astype(float)

    # Metricas de producto
    items_por_dia = items.groupby(['Sucursal', 'createdAt', 'product_name']).agg({
        'quantity': 'sum',
        'total': 'sum'
    }).reset_index()
    items_por_dia.columns = ['Sucursal', 'Fecha', 'Producto', 'Cantidad Total', 'Venta Total']
    items_por_dia = items_por_dia[items_por_dia['Cantidad Total'] > 0]
    product_daily_avg = items_por_dia.groupby(['Sucursal', 'Producto']).agg({
        'Cantidad Total': 'mean',
        'Venta Total': 'mean'
    }).reset_index()

    product_daily_avg.columns = ['Sucursal', 'Producto', 'Promedio Cantidad Diaria', 'Promedio Venta Diaria']
    product_total_sales = items_por_dia.groupby(['Sucursal', 'Producto']).agg({
        'Cantidad Total': 'sum',
        'Venta Total': 'sum'
    }).reset_index()
    ventas_por_dia_y_producto = product_total_sales.merge(product_daily_avg, on=['Sucursal', 'Producto'], how='left')
    cols_to_round = ['Venta Total', 'Cantidad Total', 'Promedio Cantidad Diaria',  'Promedio Venta Diaria']
    ventas_por_dia_y_producto[cols_to_round] = (ventas_por_dia_y_producto[cols_to_round]
        .round(2)
        .fillna(0))

    # Metricas de categoria
    categorias_por_dia = items.groupby(['Sucursal', 'createdAt', 'product_category']).agg({
        'quantity': 'sum',
        'total': 'sum'
    }).reset_index()
    categorias_por_dia.columns = ['Sucursal', 'Fecha', 'Categoria', 'Cantidad Total', 'Venta Total']
    categorias_por_dia = categorias_por_dia[categorias_por_dia['Cantidad Total'] > 0]
    category_daily_avg = categorias_por_dia.groupby(['Sucursal', 'Categoria']).agg({
        'Cantidad Total': 'mean',
        'Venta Total': 'mean'
    }).reset_index()
    category_daily_avg.columns = ['Sucursal', 'Categoria', 'Promedio Cantidad Diaria', 'Promedio Venta Diaria']
    category_total_sales = categorias_por_dia.groupby(['Sucursal', 'Categoria']).agg({
        'Cantidad Total': 'sum',
        'Venta Total': 'sum'
    }).reset_index()

    ventas_por_dia_y_categoria = category_total_sales.merge(category_daily_avg, on=['Sucursal', 'Categoria'], how='left')
    # Upload to Supabase

    producto_categoria = items[['product_name', 'product_category']].drop_duplicates()
    producto_categoria.columns = ['Producto', 'Categoria']
    producto_categoria.dropna(inplace=True)

    ##Metricas generales

    #Total ventas mes actual
    ventas_mes_actual = ventas[ventas['mes'] == current_date.strftime("%Y-%m")]
    metrica_ventas_mes_actual = ventas_mes_actual['total'].sum()
    #Total ventas mes anterior
    ventas_mes_anterior = ventas[ventas['mes'] == last_day_of_previous_month.strftime("%Y-%m")]
    metrica_ventas_mes_anterior = ventas_mes_anterior['total'].sum()
    #Crecimiento mensual
    if metrica_ventas_mes_anterior != 0:
        crecimiento_mensual = ((metrica_ventas_mes_actual - metrica_ventas_mes_anterior) / metrica_ventas_mes_anterior) * 100
    else:
        crecimiento_mensual = 0
    
    #Promedio mensual
    if metrica_ventas_mes_actual != 0:
        dias_transcurridos = (current_date - first_day_of_current_month).days + 1
        promedio_mensual = metrica_ventas_mes_actual / dias_transcurridos
    else:
        promedio_mensual = 0
    ##Metricas diarias
    
    #Total ventas hoy
    ventas_hoy = ventas[ventas['createdAt'] == current_date.date()]
    metrica_ventas_hoy = ventas_hoy['total'].sum()

    #Promedio ventas diario
    if metrica_ventas_hoy != 0:
        dias_transcurridos = (current_date - first_day_of_current_month).days + 1
        promedio_diario = metrica_ventas_hoy / dias_transcurridos
    else:
        promedio_diario = 0

    # Métricas para Arguibel
    ventas_arguibel = ventas[ventas['Sucursal'] == 'Arguibel']
    ventas_arguibel_mes_actual = ventas_arguibel[ventas_arguibel['mes'] == current_date.strftime("%Y-%m")]
    metrica_ventas_arguibel_mes_actual = ventas_arguibel_mes_actual['total'].sum()
    ventas_arguibel_mes_anterior = ventas_arguibel[ventas_arguibel['mes'] == last_day_of_previous_month.strftime("%Y-%m")]
    metrica_ventas_arguibel_mes_anterior = ventas_arguibel_mes_anterior['total'].sum()
    
    if metrica_ventas_arguibel_mes_anterior != 0:
        crecimiento_mensual_arguibel = ((metrica_ventas_arguibel_mes_actual - metrica_ventas_arguibel_mes_anterior) / metrica_ventas_arguibel_mes_anterior) * 100
    else:
        crecimiento_mensual_arguibel = 0
    
    if metrica_ventas_arguibel_mes_actual != 0:
        promedio_mensual_arguibel = metrica_ventas_arguibel_mes_actual / dias_transcurridos
    else:
        promedio_mensual_arguibel = 0
    
    ventas_arguibel_hoy = ventas_arguibel[ventas_arguibel['createdAt'] == current_date.date()]
    metrica_ventas_arguibel_hoy = ventas_arguibel_hoy['total'].sum()
    
    if metrica_ventas_arguibel_hoy != 0:
        promedio_diario_arguibel = metrica_ventas_arguibel_hoy / dias_transcurridos
    else:
        promedio_diario_arguibel = 0

    # Métricas para Polo
    ventas_polo = ventas[ventas['Sucursal'] == 'Polo']
    ventas_polo_mes_actual = ventas_polo[ventas_polo['mes'] == current_date.strftime("%Y-%m")]
    metrica_ventas_polo_mes_actual = ventas_polo_mes_actual['total'].sum()
    ventas_polo_mes_anterior = ventas_polo[ventas_polo['mes'] == last_day_of_previous_month.strftime("%Y-%m")]
    metrica_ventas_polo_mes_anterior = ventas_polo_mes_anterior['total'].sum()
    
    if metrica_ventas_polo_mes_anterior != 0:
        crecimiento_mensual_polo = ((metrica_ventas_polo_mes_actual - metrica_ventas_polo_mes_anterior) / metrica_ventas_polo_mes_anterior) * 100
    else:
        crecimiento_mensual_polo = 0
    
    if metrica_ventas_polo_mes_actual != 0:
        promedio_mensual_polo = metrica_ventas_polo_mes_actual / dias_transcurridos
    else:
        promedio_mensual_polo = 0
    
    ventas_polo_hoy = ventas_polo[ventas_polo['createdAt'] == current_date.date()]
    metrica_ventas_polo_hoy = ventas_polo_hoy['total'].sum()
    
    if metrica_ventas_polo_hoy != 0:
        promedio_diario_polo = metrica_ventas_polo_hoy / dias_transcurridos
    else:
        promedio_diario_polo = 0

    # Combine all metrics
    # Create metrics dataframe with Sucursal as a column
    metricas_data = []
    
    # Metrics for all branches
    metricas_data.append({
        'Sucursal': 'Todas',
        'Total ventas mes': metrica_ventas_mes_actual,
        'Promedio mes': promedio_mensual,
        'Total ventas hoy': metrica_ventas_hoy,
        'Promedio venta diaria': promedio_diario
    })
    
    # Metrics for Arguibel
    metricas_data.append({
        'Sucursal': 'Arguibel',
        'Total ventas mes': metrica_ventas_arguibel_mes_actual,
        'Promedio mes': promedio_mensual_arguibel,
        'Total ventas hoy': metrica_ventas_arguibel_hoy,
        'Promedio venta diaria': promedio_diario_arguibel
    })
    
    # Metrics for Polo
    metricas_data.append({
        'Sucursal': 'Polo',
        'Total ventas mes': metrica_ventas_polo_mes_actual,
        'Promedio mes': promedio_mensual_polo,
        'Total ventas hoy': metrica_ventas_polo_hoy,
        'Promedio venta diaria': promedio_diario_polo
    })
    
    metricas_dataframe = pd.DataFrame(metricas_data)

    # Venta mensual general y por sucursal
    ventas_mensuales = ventas.groupby(['Sucursal', 'mes']).agg({'total': 'sum'}).reset_index()
    ventas_mensuales.columns = ['Sucursal', 'Mes', 'Venta Mensual']
    ventas_mensuales['Mes'] = ventas_mensuales['Mes'].astype(str)
    ventas_mensuales['Venta Mensual'] = ventas_mensuales['Venta Mensual'].round(0).astype(int)
    ventas_mensual_general = ventas_mensuales.groupby('Mes').agg({'Venta Mensual': 'sum'}).reset_index()
    ventas_mensual_general.columns = ['Mes', 'Venta Mensual']
    ventas_mensual_general['Mes'] = ventas_mensual_general['Mes'].astype(str)
    ventas_mensual_general['Sucursal'] = 'Todas'
    ventas_mensual_general['Venta Mensual'] = ventas_mensual_general['Venta Mensual'].round(0).astype(int)
    ventas_mensuales = pd.concat([ventas_mensuales, ventas_mensual_general], ignore_index=True)



    try: 
        print("Eliminando datos antiguos de hc_producto_categoria...")
        #supabase_client.table('hc_producto_categoria').delete().neq('Producto', '').execute()
        print(f"Insertando {len(producto_categoria)} registros en hc_producto_categoria...")
        insert_table_data('hc_producto_categoria', producto_categoria.to_dict(orient='records'))
        
        print("Eliminando datos antiguos de hc_venta_general_por_dia...")
        supabase_client.table('hc_venta_general_por_dia').delete().neq('Venta General', 0).execute()
        print(f"Insertando {len(ventas_diarias_gral)} registros en hc_venta_general_por_dia...")
        insert_table_data('hc_venta_general_por_dia', ventas_diarias_gral.to_dict(orient='records'))
        
        print("Eliminando datos antiguos de hc_ventas_por_sucursal...")
        supabase_client.table('hc_ventas_por_sucursal').delete().neq('Venta Sucursal', 0).execute()
        print(f"Insertando {len(ventas_por_sucursal)} registros en hc_ventas_por_sucursal...")
        insert_table_data('hc_ventas_por_sucursal', ventas_por_sucursal.to_dict(orient='records'))
        
        print("Eliminando datos antiguos de hc_ventas_por_dia_y_producto...")
        supabase_client.table('hc_ventas_por_dia_y_producto').delete().neq('Cantidad Total', 0).execute()
        print(f"Insertando {len(ventas_por_dia_y_producto)} registros en hc_ventas_por_dia_y_producto...")
        insert_table_data('hc_ventas_por_dia_y_producto', ventas_por_dia_y_producto.to_dict(orient='records'))
        
        print("Eliminando datos antiguos de hc_ventas_por_dia_y_categoria...")
        supabase_client.table('hc_ventas_por_dia_y_categoria').delete().neq('Cantidad Total', 0).execute()
        print(f"Insertando {len(ventas_por_dia_y_categoria)} registros en hc_ventas_por_dia_y_categoria...")
        insert_table_data('hc_ventas_por_dia_y_categoria', ventas_por_dia_y_categoria.to_dict(orient='records'))
        
        print("Eliminando datos antiguos de hc_metricas_generales...")
        supabase_client.table('hc_metricas_generales').delete().neq('Sucursal', '').execute()
        print(f"Insertando {len(metricas_dataframe)} registros en hc_metricas_generales...")
        insert_table_data('hc_metricas_generales', metricas_dataframe.to_dict(orient='records'))

        supabase_client.table('hc_venta_mensual').delete().neq('Venta Mensual', 0).execute()
        print(f"Insertando {len(ventas_mensuales)} registros en hc_ventas_mensuales...")
        insert_table_data('hc_venta_mensual', ventas_mensuales.to_dict(orient='records'))


        print("Carga de datos a Supabase completada exitosamente.")

    
    except Exception as e:
        log_event("ERROR", "process_data", "Error uploading data to Supabase", error=str(e))
    return {
        'ventas_rows': int(ventas.shape[0]),
        'items_rows': int(items.shape[0]),
        'permanencia_rows': int(permanencia.shape[0]),
        'generated': ['procesado/items.xlsx', 'procesado/ventas.xlsx', 'procesado/permanencia.xlsx']
    }


if __name__ == "__main__":
    try:
        res = run_processing()
        print(f"Processing finished: {res}")
    except Exception as e:
        log_event("ERROR", "process_data", "Unhandled error in standalone run", error=str(e))
        raise



