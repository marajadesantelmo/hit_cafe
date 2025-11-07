import pandas as pd
import os
from datetime import datetime
from supabase import create_client, Client
import time
from typing import Optional, Dict

url_supabase = os.getenv("url_supabase")
key_supabase = os.getenv("key_supabase")

supabase_client = create_client(url_supabase, key_supabase)

def login_user(email: str, password: str) -> Optional[Dict]:
    try:
        auth_response = supabase_client.auth.sign_in_with_password({
            "email": email,
            "password": password
        })
        if auth_response.user:
            return auth_response.user
        return None
    except Exception as e:
        print(f"Error during login: {e}")
        return None

def get_user_session():
    try:
        session = supabase_client.auth.get_session()
        if session and session.user:
            return session
        return None
    except Exception as e:
        print(f"Error getting session: {e}")
        return None

def logout_user():
    try:
        supabase_client.auth.sign_out()
        return True
    except Exception as e:
        print(f"Error during logout: {e}")
        return False

def fetch_table_data(table_name, retries=3, delay=5):
    for attempt in range(retries):
        try:
            query = (
                supabase_client
                .from_(table_name)
                .select('*')
                .execute()
            )
            df = pd.DataFrame(query.data)
            if df.empty:
                print(f"Warning: No data found in {table_name}")
            return df
        except Exception as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                print(f"Failed to fetch data from {table_name} after {retries} attempts")
                return pd.DataFrame()

