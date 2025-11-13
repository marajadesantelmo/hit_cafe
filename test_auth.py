"""
Test script to validate authentication improvements
"""

from utils import autenticar, get_branch_configs
import time

def test_authentication():
    print("Testing authentication for all branches...")
    
    for i, cfg in enumerate(get_branch_configs()):
        suc = cfg['name']
        print(f"\nTesting authentication for {suc}...")
        
        try:
            # Add delay between different branches to avoid rate limiting
            if i > 0:
                print(f'Waiting 10 seconds before testing {suc}...')
                time.sleep(10)
            
            headers = autenticar(cfg['apiKey'], cfg['apiSecret'])
            print(f"✅ Authentication successful for {suc}")
            print(f"Headers: {headers}")
            
        except Exception as e:
            print(f"❌ Authentication failed for {suc}: {e}")
            return False
    
    print("\n✅ All authentications successful!")
    return True

if __name__ == "__main__":
    test_authentication()