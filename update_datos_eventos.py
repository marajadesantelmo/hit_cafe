import gspread
import pandas as pd
import os
from tokens import url_supabase, key_supabase
from supabase import create_client, Client
supabase_client = create_client(url_supabase, key_supabase)
from datetime import datetime, timedelta

print("Iniciando actualización de datos de operaciones...")
if os.path.exists('\\\\dc01\\Usuarios\\PowerBI\\flastra\\Documents\\sgto_financiera\\credenciales_gsheets.json'):
    gc = gspread.service_account('\\\\dc01\\Usuarios\\PowerBI\\flastra\\Documents\\sgto_financiera\\credenciales_gsheets.json')
elif os.path.exists('credenciales_gsheets.json'):
    gc = gspread.service_account(filename='credenciales_gsheets.json')
#Operaciones
sheet_url = 'https://docs.google.com/spreadsheets/d/1Ysi1U2FUkbDe_9llLNely86AOsWi1oqx-i_2qtvCFyk'
sh = gc.open_by_url(sheet_url)
worksheet = sh.worksheet('Eventos cotizados')
header = worksheet.get('B1:T1')[0]  
all_rows = worksheet.get('B2:T900') 

eventos = pd.DataFrame(all_rows, columns=header)
eventos = eventos[eventos['Estado'] == 'Ganado'].copy()
eventos = eventos[['Fecha Evento', 'Cliente ', 'Sucursal', 'Tipo de menu', 'PAX',
       'Horario', 'valor persona (IVA NO INCLUIDO)', 'Total']]
eventos.rename(columns={'valor persona (IVA NO INCLUIDO)': 'Valor persona', 
                                'Cliente ': 'Cliente'}, inplace=True)

eventos['Fecha Evento'] = pd.to_datetime(eventos['Fecha Evento'], format='%d/%m/%Y', errors='coerce').dt.strftime('%Y-%m-%d')

def clean_currency(x):
    if isinstance(x, str):
        clean_str = x.replace('AR$', '').replace('.', '').replace(',', '.')
        try:
            return float(clean_str)
        except ValueError:
            return 0.0
    return x

eventos['Valor persona'] = eventos['Valor persona'].apply(clean_currency)
eventos['Total'] = eventos['Total'].apply(clean_currency)
eventos['Cliente'] = eventos['Cliente'].str.title()
eventos['PAX'] = pd.to_numeric(eventos['PAX'], errors='coerce').fillna(0).astype(int)
eventos['Mes'] = eventos['Fecha Evento'].str[:7]

eventos_sucursal_mensual = eventos.groupby(['Sucursal', 'Mes']).agg(
    Eventos=('Fecha Evento', 'count'),
    PAX_Total=('PAX', 'sum'),
    Ingresos_Total=('Total', 'sum')
).reset_index()

mes_anterior = (datetime.now() - timedelta(days=30)).strftime('%Y-%m')
mes_actual = datetime.now().strftime('%Y-%m')
proximo_mes = (datetime.now() + timedelta(days=30)).strftime('%Y-%m')

eventos_mes_actual = eventos[eventos['Mes'] == mes_actual]
eventos_mes_anterior = eventos[eventos['Mes'] == mes_anterior]
eventos_proximo_mes = eventos[eventos['Mes'] == proximo_mes]

eventos_sucursal_mes_anterior = eventos_mes_anterior.groupby(['Sucursal', 'Mes']).agg(
    Eventos=('Fecha Evento', 'count'),
    PAX_Total=('PAX', 'sum'),
    Ingresos_Total=('Total', 'sum')
).reset_index()
eventos_sucursal_mes_anterior.rename(columns={
    'Eventos': 'Eventos mes anterior',
    'PAX_Total': 'PAX mes anterior',
    'Ingresos_Total': 'Venta mes anterior'
}, inplace=True)

eventos_sucursal_mes_actual = eventos_mes_actual.groupby(['Sucursal', 'Mes']).agg(
    Eventos=('Fecha Evento', 'count'),
    PAX_Total=('PAX', 'sum'),
    Ingresos_Total=('Total', 'sum')
).reset_index()
eventos_sucursal_mes_actual.rename(columns={
    'Eventos': 'Eventos mes actual',
    'PAX_Total': 'PAX mes actual',
    'Ingresos_Total': 'Venta mes actual'
}, inplace=True)

eventos_sucursal_proximo_mes = eventos_proximo_mes.groupby(['Sucursal', 'Mes']).agg(
    Eventos=('Fecha Evento', 'count'),
    PAX_Total=('PAX', 'sum'),
    Ingresos_Total=('Total', 'sum')
).reset_index()
eventos_sucursal_proximo_mes.rename(columns={
    'Eventos': 'Eventos proximo mes',
    'PAX_Total': 'PAX proximo mes',
    'Ingresos_Total': 'Venta proximo mes'
}, inplace=True)

eventos_metricas_mensuales = eventos_sucursal_mensual.merge(
    eventos_sucursal_mes_anterior[['Sucursal', 'Eventos mes anterior', 'PAX mes anterior', 'Venta mes anterior']],
    on='Sucursal', how='left'
).merge(
    eventos_sucursal_mes_actual[['Sucursal', 'Eventos mes actual', 'PAX mes actual', 'Venta mes actual']],
    on='Sucursal', how='left'
).merge(
    eventos_sucursal_proximo_mes[['Sucursal', 'Eventos proximo mes', 'PAX proximo mes', 'Venta proximo mes']],
    on='Sucursal', how='left'
)

eventos_metricas_mensuales.fillna(0, inplace=True)

def insert_table_data(table_name, data):
    for record in data:
        try:
            supabase_client.from_(table_name).insert(record).execute()
        except Exception as e:
            print(f"Error inserting record into {table_name}: {e}")

supabase_client.table('hc_eventos').delete().neq('Total', 0).execute()
print(f"Insertando {len(eventos)} registros en hc_eventos...")
insert_table_data('hc_eventos', eventos.to_dict(orient='records'))

supabase_client.table('hc_eventos_metricas_mensuales').delete().neq('Ingresos_Total', 0).execute()
print(f"Insertando {len(eventos_metricas_mensuales)} registros en hc_eventos_metricas_mensuales...")
insert_table_data('hc_eventos_metricas_mensuales', eventos_metricas_mensuales.to_dict(orient='records'))