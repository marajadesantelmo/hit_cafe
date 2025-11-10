"""
Full bootstrap download from the Fudo API for all sucursales (Arguibel, Polo, Ugarte).
Generates unified CSVs in data/: ventas.csv, items.csv, pagos.csv, productos_categorias.csv
with an extra column 'Sucursal' on all of them.
"""

#%% Importo paquetes
import os
import time
import requests
import pandas as pd
from utils import autenticar, get_branch_configs


def _get_base_dir() -> str:
    network = "//dc01/Usuarios/PowerBI/flastra/Documents/hit_cafe"
    local = 'C:\\Users\\facun\\OneDrive\\Documentos\\GitHub\\hit_cafe'
    return network if os.path.exists(network) else local


base_dir = _get_base_dir()
data_dir = os.path.join(base_dir, 'data')
os.makedirs(data_dir, exist_ok=True)


#%% Obtengo datos de ventas e items de todas las sucursales
sales_dfs = []
items_dfs = []

print('Descargando ventas de Fudo API (todas las sucursales)')
for cfg in get_branch_configs():
    suc = cfg['name']
    headers = autenticar(cfg['apiKey'], cfg['apiSecret'])
    page = 0
    # Primer request para conocer si hay data
    url = f'https://api.fu.do/v1alpha1/sales?page[size]=500&page[number]={page}&include=items'
    response = requests.get(url, headers=headers)
    data_dict = response.json()
    while 'data' in data_dict and len(data_dict['data']) == 500:
        print(f'[{suc}] Descargando pagina {page}')
        url = f'https://api.fu.do/v1alpha1/sales?page[size]=500&page[number]={page}&include=items'
        response = requests.get(url, headers=headers)
        if response.status_code == 429 and 'Retry-After' in response.headers:
            retry_after = int(response.headers['Retry-After'])
            print(f"[{suc}] page {page} - Retry later, esperando {retry_after} seg...")
            time.sleep(retry_after)
            continue
        if response.status_code == 404:
            print(f"[{suc}] page {page} - Venta no encontrada. Detenemos.")
            break
        data_dict = response.json()
        if 'error' in data_dict:
            if data_dict['error'] == '404':
                break
            if data_dict['error'] == '429':
                print(f"[{suc}] Pagina {page} - Esperando respuesta de la API.")
                continue

        page_sales_data = data_dict.get('data', [])
        for data in page_sales_data:
            customer_data = data['relationships']['customer']['data']
            customer_id = customer_data['id'] if customer_data and 'id' in customer_data else None
            sale_row = pd.DataFrame([{
                'sale_id': data['id'],
                'total': data['attributes']['total'],
                'people': data['attributes']['people'],
                'saleType': data['attributes']['saleType'],
                'createdAt': data['attributes']['createdAt'],
                'closedAt': data['attributes']['closedAt'],
                'customer_id': customer_id,
                'Sucursal': suc,
            }])
            sales_dfs.append(sale_row)

        page_items_data = data_dict.get('included', [])
        for item in page_items_data:
            page_items_df = pd.DataFrame([{
                'sale_id': item['relationships']['sale']['data']['id'],
                'product_id': item['relationships']['product']['data']['id'],
                'price': item['attributes']['price'],
                'quantity': item['attributes']['quantity'],
                'canceled': item['attributes']['canceled'],
                'Sucursal': suc,
            }])
            items_dfs.append(page_items_df)

        page += 1

sales_df = pd.concat(sales_dfs, ignore_index=True) if sales_dfs else pd.DataFrame()
items_df = pd.concat(items_dfs, ignore_index=True) if items_dfs else pd.DataFrame()

items_df.to_csv(os.path.join(data_dir, 'items.csv'), index=False)
sales_df.to_csv(os.path.join(data_dir, 'ventas.csv'), index=False)


#%% Obtengo datos de pagos de todas las sucursales
print('Descargando pagos de Fudo API (todas las sucursales)')
payments_dfs = []
for cfg in get_branch_configs():
    suc = cfg['name']
    headers = autenticar(cfg['apiKey'], cfg['apiSecret'])
    page = 1
    # First request to check if there's data
    url = f'https://api.fu.do/v1alpha1/payments?page[size]=500&page[number]={page}'
    response = requests.get(url, headers=headers)
    data_dict = response.json()
    
    while 'data' in data_dict and len(data_dict['data']) > 0:
        print(f'[{suc}] Descargando pagina {page} (pagos)')
        if response.status_code == 429 and 'Retry-After' in response.headers:
            retry_after = int(response.headers['Retry-After'])
            print(f"[{suc}] Pagina {page} - Retry later, esperando {retry_after} seg...")
            time.sleep(retry_after)
            # Retry the same page
            response = requests.get(url, headers=headers)
            data_dict = response.json()
            continue
            
        if response.status_code == 404:
            print(f"[{suc}] Pagina {page} - Pago no encontrado. Detenemos.")
            break
            
        if 'error' in data_dict:
            if data_dict['error'] == '404':
                break
            if data_dict['error'] == '429':
                print(f"[{suc}] Pagina {page} - Esperando respuesta de la API.")
                continue

        page_payments_data = data_dict.get('data', [])
        for payment_data in page_payments_data:
            payment_row = pd.DataFrame({
                'payment_id': [payment_data['id']],
                'amount': [payment_data['attributes']['amount']],
                'createdAt': [payment_data['attributes']['createdAt']],
                'paymentMethod': [payment_data['relationships']['paymentMethod']['data']['id']],
                # 'sale_id': [payment_data['relationships']['sale']['data']['id']],
                'canceled': [payment_data['attributes']['canceled']],
                'Sucursal': [suc],
            })
            payments_dfs.append(payment_row)
            
        # Move to next page
        page += 1
        url = f'https://api.fu.do/v1alpha1/payments?page[size]=500&page[number]={page}'
        response = requests.get(url, headers=headers)
        data_dict = response.json()

payments_df = pd.concat(payments_dfs, ignore_index=True) if payments_dfs else pd.DataFrame()
payments_df.to_csv(os.path.join(data_dir, 'pagos.csv'), index=False)


# %% Obtengo datos de productos y categorias por sucursal
print('Descargando productos y categorias de Fudo API (todas las sucursales)')
productos_final_all = []
for cfg in get_branch_configs():
    suc = cfg['name']
    headers = autenticar(cfg['apiKey'], cfg['apiSecret'])
    # Categorias de productos
    url = 'https://api.fu.do/v1alpha1/product-categories?sort=id&include=products'
    response = requests.get(url, headers=headers)
    json_data = response.json().get('data', [])

    processed_data = []
    for item in json_data:
        category_name = item['attributes']['name']
        category_id = item['id']
        for product in item['relationships']['products']['data']:
            product_id = product['id']
            processed_data.append([category_name, category_id, product_id])

    categorias = pd.DataFrame(processed_data, columns=['product_category', 'category_id', 'product_id'])

    # Productos
    product_data = []
    url = f'https://api.fu.do/v1alpha1/products?page[size]=500&page[number]=0'
    response = requests.get(url, headers=headers)
    if response.status_code == 429 and 'Retry-After' in response.headers:
        retry_after = int(response.headers['Retry-After'])
        print(f"[{suc}] Products - Retry later, esperando {retry_after} seg...")
        time.sleep(retry_after)
        response = requests.get(url, headers=headers)
    if response.status_code == 200:
        json_data = response.json()
        for item in json_data.get('data', []):
            product_id = item['id']
            product_name = item['attributes']['name']
            product_data.append([product_id, product_name])
        print(f"[{suc}] Se han descargado {len(product_data)} productos")
    else:
        print(f"[{suc}] Failed to retrieve products, status code: {response.status_code}")

    productos = pd.DataFrame(product_data, columns=['product_id', 'product_name'])

    # Merge productos y categorias
    productos['product_id'] = productos['product_id'].astype(str)
    categorias['product_id'] = categorias['product_id'].astype(str)
    productos_final = productos.merge(categorias, on='product_id')
    productos_final = productos_final[['product_id', 'product_name', 'product_category']]
    productos_final['Sucursal'] = suc

    productos_final_all.append(productos_final)

productos_final_all_df = pd.concat(productos_final_all, ignore_index=True) if productos_final_all else pd.DataFrame()
productos_final_all_df.to_csv(os.path.join(data_dir, 'productos_categorias.csv'), index=False)
