#%% Importo paquetes
import os
import time
import requests
import pandas as pd

from utils import autenticar, get_branch_configs
from logging_utils import log_event


def _get_base_dir() -> str:
    """Return the preferred base directory for data files."""
    network = r"\\dc01\Usuarios\PowerBI\flastra\Documents\hit_cafe"
    local = r'C:\Users\facun\OneDrive\Documentos\GitHub\hit_cafe'
    server = r'\\dc01\Usuarios\PowerBI\flastra\Documents\hit_cafe'
    return network if os.path.exists(network) else local if os.path.exists(local) else server

#%% Funciones para actualizar bases de datos
def get_ventas_dataframes(headers, start_sale_id: int, sucursal: str):
    sales_dfs = []
    items_dfs = []
    last_sales_df = pd.DataFrame()
    last_items_df = pd.DataFrame()
    sale_id = start_sale_id
    while True:
        url = f'https://api.fu.do/v1alpha1/sales/{sale_id}?include=items'
        response = requests.get(url, headers=headers)
        if response.status_code == 429 and 'Retry-After' in response.headers:
            retry_after = int(response.headers['Retry-After'])
            print(f"Sale {sale_id} - Retry later, waiting {retry_after} seconds...")
            time.sleep(retry_after)
        if response.status_code == 404:
            print(f"Sale {sale_id} - Venta no encontrada. Dejamos de buscar ventas.")
        try:
            data_dict = response.json()      
            sale_data = data_dict['data']
        except Exception:
            break                  
        customer_data = sale_data['relationships']['customer']['data']
        customer_id = customer_data['id'] if customer_data and 'id' in customer_data else None
        sales_df = pd.DataFrame({
            'sale_id': [sale_id],
            'total': [sale_data['attributes']['total']],         
            'people': [sale_data['attributes']['people']],
            'saleType': [sale_data['attributes']['saleType']],
            'createdAt': [sale_data['attributes']['createdAt']],
            'closedAt': [sale_data['attributes']['closedAt']], 
            'customer_id': customer_id,
            'Sucursal': sucursal,
        })
        sales_dfs.append(sales_df)
        if 'included' in data_dict:
            items_data = data_dict['included']
            items_list = []
            for item in items_data:
                product_id = item['relationships']['product']['data']['id']
                price = item['attributes']['price']
                quantity = item['attributes']['quantity']
                canceled = item['attributes']['canceled']
                items_list.append({
                    'sale_id': sale_id,
                    'product_id': product_id,
                    'price': price,       #tener en cuenta que es precio total no unitario
                    'quantity': quantity,
                    'canceled': canceled })
            items_df = pd.DataFrame(items_list)
            items_df['Sucursal'] = sucursal
            items_dfs.append(items_df)
        print(f"Venta {sale_id} - Descargada")
        sale_id += 1
    if len(sales_dfs) != 0:
        last_sales_df = pd.concat(sales_dfs, ignore_index=True)
    if len(items_dfs) != 0:
        last_items_df = pd.concat(items_dfs, ignore_index=True)
    return last_sales_df , last_items_df 

# Note: Payment-related functions have been removed as they were unused in the
# update flow. They can be restored from version control if needed.

def update_productos_categorias():
    """Update productos_categorias.csv with any new products from all branches."""
    base_dir = _get_base_dir()
    productos_path = os.path.join(base_dir, 'data', 'productos_categorias.csv')
    
    # Load existing products
    try:
        existing_products = pd.read_csv(productos_path)
        if 'Sucursal' not in existing_products.columns:
            existing_products['Sucursal'] = None
    except FileNotFoundError:
        existing_products = pd.DataFrame(columns=['product_id', 'product_name', 'product_category', 'Sucursal'])
    
    all_new_products = []
    
    for cfg in get_branch_configs():
        suc = cfg['name']
        headers = autenticar(cfg['apiKey'], cfg['apiSecret'])
        
        log_event("INFO", "update_data_api_fudo", f"[{suc}] Actualizando catálogo de productos")
        
        # Get product categories
        try:
            url = 'https://api.fu.do/v1alpha1/product-categories?sort=id&include=products'
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                json_data = response.json().get('data', [])
                
                processed_data = []
                for item in json_data:
                    category_name = item['attributes']['name']
                    for product in item['relationships']['products']['data']:
                        product_id = product['id']
                        processed_data.append([category_name, product_id])
                
                categorias = pd.DataFrame(processed_data, columns=['product_category', 'product_id'])
                
                # Get product names
                url_products = 'https://api.fu.do/v1alpha1/products?page[size]=500&page[number]=0'
                response = requests.get(url_products, headers=headers)
                if response.status_code == 200:
                    product_data = []
                    json_data = response.json()
                    for item in json_data.get('data', []):
                        product_id = item['id']
                        product_name = item['attributes']['name']
                        product_data.append([product_id, product_name])
                    
                    productos = pd.DataFrame(product_data, columns=['product_id', 'product_name'])
                    
                    # Merge and add Sucursal
                    productos['product_id'] = productos['product_id'].astype(str)
                    categorias['product_id'] = categorias['product_id'].astype(str)
                    productos_final = productos.merge(categorias, on='product_id', how='left')
                    productos_final = productos_final[['product_id', 'product_name', 'product_category']]
                    productos_final['Sucursal'] = suc
                    
                    all_new_products.append(productos_final)
                    
        except Exception as e:
            log_event("ERROR", "update_data_api_fudo", f"[{suc}] Error updating product catalog", error=str(e))
    
    if all_new_products:
        # Combine all new products
        new_products_df = pd.concat(all_new_products, ignore_index=True)
        
        # Remove duplicates and merge with existing
        all_products = pd.concat([existing_products, new_products_df], ignore_index=True)
        all_products = all_products.drop_duplicates(subset=['product_id', 'Sucursal'], keep='last')
        
        # Save updated catalog
        all_products.to_csv(productos_path, index=False)
        log_event("INFO", "update_data_api_fudo", f"Catálogo actualizado: {len(all_products)} productos totales")
        
        return len(new_products_df)
    
    return 0

def run_update():
    """Update ventas.csv and items.csv from the latest sale id onward.

    Returns a small summary dict with counts.
    """
    base_dir = _get_base_dir()
    ventas_path = os.path.join(base_dir, 'data', 'ventas.csv')
    items_path = os.path.join(base_dir, 'data', 'items.csv')

    log_event("INFO", "update_data_api_fudo", "Cargando dataframes")
    ventas = pd.read_csv(ventas_path)
    items = pd.read_csv(items_path)

    if 'Sucursal' not in ventas.columns:
        ventas['Sucursal'] = None
    if 'Sucursal' not in items.columns:
        items['Sucursal'] = None

    # Limpio la base de posibles ventas no cerradas y saco ultimas 20 ventas por las dudas
    ventas = ventas.dropna(subset=['closedAt'])
    if len(ventas) > 20:
        ventas = ventas.iloc[:-20]
    updated_counts = {"ventas_added": 0, "items_added": 0}

    ventas_updates_all = []
    items_updates_all = []

    for cfg in get_branch_configs():
        suc = cfg['name']
        # Compute last sale id per sucursal
        v_suc = ventas[ventas['Sucursal'] == suc]
        if not v_suc.empty:
            try:
                last_sale_id = pd.to_numeric(v_suc['sale_id'], errors='coerce').dropna().astype(int).max()
            except Exception:
                last_sale_id = 0
        else:
            last_sale_id = 0

        log_event("INFO", "update_data_api_fudo", f"[{suc}] Descargando desde sale_id {last_sale_id + 1}")
        headers = autenticar(cfg['apiKey'], cfg['apiSecret'])
        v_u, i_u = get_ventas_dataframes(headers, last_sale_id + 1, suc)
        if isinstance(v_u, pd.DataFrame) and not v_u.empty:
            ventas_updates_all.append(v_u)
            updated_counts["ventas_added"] += int(v_u.shape[0])
        if isinstance(i_u, pd.DataFrame) and not i_u.empty:
            items_updates_all.append(i_u)
            updated_counts["items_added"] += int(i_u.shape[0])

    if items_updates_all:
        log_event("INFO", "update_data_api_fudo", "Actualizando items.csv (todas las sucursales)")
        items_new = pd.concat([items] + items_updates_all, ignore_index=True)
        items_new.to_csv(items_path, index=False)
        time.sleep(1)

    if ventas_updates_all:
        log_event("INFO", "update_data_api_fudo", "Actualizando ventas.csv (todas las sucursales)")
        ventas_new = pd.concat([ventas] + ventas_updates_all, ignore_index=True)
        ventas_new.to_csv(ventas_path, index=False)
        time.sleep(1)

    # Update product catalog if we have new items
    if items_updates_all:
        log_event("INFO", "update_data_api_fudo", "Actualizando catálogo de productos")
        productos_added = update_productos_categorias()
        updated_counts["productos_added"] = productos_added

    return updated_counts


if __name__ == "__main__":
    # Allow running as a standalone script
    try:
        result = run_update()
        print(f"Update finished: {result}")
    except Exception as e:
        log_event("ERROR", "update_data_api_fudo", "Unhandled error in standalone run", error=str(e))
        raise
