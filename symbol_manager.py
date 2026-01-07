import pandas as pd
import requests
import os
import threading
import gc
from datetime import datetime

class SymbolManager:
    def __init__(self, filename="data/instruments.csv"):
        self.filename = filename
        self.url = "https://images.dhan.co/api-data/api-scrip-master.csv"
        self.df = None
        self.is_ready = False # Flag to check status
        
        # Ensure data directory exists
        folder = os.path.dirname(filename)
        if folder and not os.path.exists(folder):
            os.makedirs(folder)
        
        # START BACKGROUND LOADING (Prevents App Timeout)
        self.loader_thread = threading.Thread(target=self._background_init)
        self.loader_thread.daemon = True
        self.loader_thread.start()

    def _background_init(self):
        """Runs in background so App starts immediately."""
        print("⏳ Symbol Manager: Initializing in background...")
        if not os.path.exists(self.filename):
            print("⬇️ Downloading Scrip Master...")
            if not self.download_scrips():
                print("❌ Download Failed.")
                return
        
        self.load_instruments()

    def download_scrips(self):
        try:
            response = requests.get(self.url, timeout=30)
            with open(self.filename, 'wb') as f:
                f.write(response.content)
            return True
        except Exception as e:
            print(f"Error downloading: {e}")
            return False

    def load_instruments(self):
        """Loads CSV with Memory Optimization."""
        if os.path.exists(self.filename):
            try:
                # 1. Load ONLY needed columns
                use_cols = [
                    'SEM_EXM_EXCH_ID', 'SEM_SMST_SECURITY_ID', 
                    'SEM_TRADING_SYMBOL', 'SEM_INSTRUMENT_NAME',
                    'SEM_EXPIRY_DATE', 'SEM_STRIKE_PRICE', 
                    'SEM_OPTION_TYPE'
                ]
                
                # 2. Specify Types to save RAM
                dtype_map = {
                    'SEM_SMST_SECURITY_ID': 'str',
                    'SEM_TRADING_SYMBOL': 'str',
                    'SEM_INSTRUMENT_NAME': 'category', # Big memory saver
                    'SEM_EXM_EXCH_ID': 'category',     # Big memory saver
                    'SEM_OPTION_TYPE': 'category',
                    'SEM_STRIKE_PRICE': 'float32'
                }

                # Load Data
                self.df = pd.read_csv(self.filename, usecols=use_cols, dtype=dtype_map, low_memory=False)
                
                # Filter (In-Place to save memory)
                valid_exchanges = ['NSE', 'BSE', 'MCX']
                self.df = self.df[self.df['SEM_EXM_EXCH_ID'].isin(valid_exchanges)]
                
                valid_instruments = [
                    'EQUITY', 'INDEX', 'FUTIDX', 'FUTSTK', 'FUTCOM', 
                    'OPTIDX', 'OPTSTK', 'OPTCOM'
                ]
                self.df = self.df[self.df['SEM_INSTRUMENT_NAME'].isin(valid_instruments)]

                # Pre-calculate Display Name and Search Key
                # We use vectorization for speed instead of apply()
                self.df['SEARCH_KEY'] = self.df['SEM_TRADING_SYMBOL'].str.upper()
                
                # Create a simple display column
                self.df['DISPLAY'] = self.df['SEM_TRADING_SYMBOL'] + " (" + self.df['SEM_INSTRUMENT_NAME'].astype(str) + ")"

                # Convert Dates
                self.df['EXPIRY_DT'] = pd.to_datetime(self.df['SEM_EXPIRY_DATE'], errors='coerce')

                # Cleanup
                self.is_ready = True
                print(f"✅ Symbol Manager Ready: {len(self.df)} instruments loaded.")
                gc.collect() # Force free memory

            except Exception as e:
                print(f"⚠️ Error loading CSV: {e}")

    def search(self, query):
        """Returns empty list if not ready yet."""
        if not self.is_ready or self.df is None:
            return []
        
        try:
            query = query.upper().strip()
            mask = self.df['SEARCH_KEY'].str.contains(query, na=False)
            results = self.df[mask].head(15) # Limit to 15 to be fast
            
            output = []
            for _, row in results.iterrows():
                output.append({
                    "symbol": row['SEM_TRADING_SYMBOL'],
                    "id": row['SEM_SMST_SECURITY_ID'],
                    "exchange": row['SEM_EXM_EXCH_ID'],
                    "display": row['DISPLAY'],
                    "type": row['SEM_INSTRUMENT_NAME']
                })
            return output
        except:
            return []

    def get_atm_security(self, index_symbol, spot_price, direction):
        if not self.is_ready or self.df is None: return None, None
        
        try:
            step = 100 if "BANK" in index_symbol.upper() else 50
            strike = round(spot_price / step) * step
            opt_type = "CE" if direction == "BUY" else "PE"

            mask = (
                (self.df['SEM_INSTRUMENT_NAME'].isin(['OPTIDX', 'OPTSTK'])) &
                (self.df['SEARCH_KEY'].str.contains(index_symbol.upper())) &
                (self.df['SEM_STRIKE_PRICE'] == strike) &
                (self.df['SEM_OPTION_TYPE'] == opt_type) &
                (self.df['EXPIRY_DT'] >= datetime.now())
            )
            
            matches = self.df[mask].sort_values('EXPIRY_DT')
            
            if not matches.empty:
                row = matches.iloc[0]
                return str(row['SEM_SMST_SECURITY_ID']), row['DISPLAY']
        except:
            pass
        return None, None
