import os
import time
import requests
import pandas as pd

from utils import autenticar, get_branch_configs
from logging_utils import log_event

items = pd.read_excel('procesado/items.xlsx')
ventas = pd.read_excel('procesado/ventas.xlsx')

items_filtrado = items[(items['Sucursal'] == 'Arguibel') & (items['createdAt'] == '2025-11-03')]
ventas_filtrado = ventas[(ventas['Sucursal'] == 'Arguibel') & (ventas['createdAt'] == '2025-11-03')]

venta_items = items_filtrado.groupby('product_name')['quantity'].sum().reset_index().sort_values(by='quantity', ascending=False)

items['createdAt'] = pd.to_datetime(items['createdAt']).dt.date
semana_filtrada = items[(items['Sucursal'] == 'Arguibel') & (items['createdAt'] >= pd.to_datetime('2025-10-30').date()) & (items['createdAt'] <= pd.to_datetime('2025-11-05').date())]

ventas_diaria_semana_filtrada = semana_filtrada.groupby('createdAt')['price'].sum().reset_index()
ventas_diaria_semana_filtrada['price'].sum()

ventas['createdAt'] = pd.to_datetime(ventas['createdAt']).dt.date
semana_filtrada_ventas = ventas[(ventas['Sucursal'] == 'Arguibel') & (ventas['createdAt'] >= pd.to_datetime('2025-10-30').date()) & (ventas['createdAt'] <= pd.to_datetime('2025-11-05').date())]
ventas_diaria_semana_filtrada_ventas = semana_filtrada_ventas.groupby('createdAt')['total'].sum().reset_index()
ventas_diaria_semana_filtrada_ventas['total'].sum()

items_venta_mensual = items.groupby(items['createdAt'].dt.to_period('M'))['price'].sum().reset_index()