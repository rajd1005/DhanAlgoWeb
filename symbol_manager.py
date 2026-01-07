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
        self.is_ready = False # Flag to check if data is loaded
        
        # Ensure data directory exists
        folder = os.path.dirname(filename)
        if folder and not os.path.exists(folder):
            os.makedirs(folder)
        
        # START BACKGROUND LOADING
        # This prevents the "Application failed to respond" error on Railway/Heroku
        self.loader_thread = threading.Thread(target=self._background_init)
        self.loader_thread.daemon = True
        self.loader_thread.start()

    def _background_init(self):
        """Runs in background so App starts immediately."""
        print("⏳ Symbol Manager: Initializing in background...")
        
        # Download if missing
        if not os.path.exists(self.filename):
            print("⬇️ Downloading Scrip Master...")
            if not self.download_scrips():
                print("❌ Download Failed. Retrying next restart.")
                return
        
        # Load Data
        self.load_instruments()

    def download_scrips(self):
        """Downloads the comprehensive Scrip Master from Dhan."""
        try:
            # 30 second timeout to prevent hanging
            response = requests.get(self.url, timeout=30)
            with open(self.filename, 'wb') as f:
                f.write(response.content)
            return True
        except Exception as e:
            print(f"❌ Error downloading CSV: {e}")
            return False

    def load_instruments(self):
        """
        Loads CSV with Strict Memory Optimization.
        Filters for NSE, BSE, MCX and all segments (Eq, F&O, Comm, Index).
        """
        if os.path.exists(self.filename):
            try:
                # 1. Define Columns to Load (Save Memory)
                use_cols = [
                    'SEM_EXM_EXCH_ID',      # Exchange
                    'SEM_SMST_SECURITY_ID', # Security ID
                    'SEM_TRADING_SYMBOL',   # Symbol
                    'SEM_INSTRUMENT_NAME',  # Type (EQUITY, FUTIDX, etc)
                    'SEM_EXPIRY_DATE',      # Expiry
                    'SEM_STRIKE_PRICE',     # Strike
                    'SEM_OPTION_TYPE'       # CE/PE
                ]
                
                # 2. Specify Types to save RAM (Crucial for Cloud Free Tiers)
                dtype_map = {
                    'SEM_SMST_SECURITY_ID': 'str',
                    'SEM_TRADING_SYMBOL': 'str',
                    'SEM_INSTRUMENT_NAME': 'category', # Big memory saver
                    'SEM_EXM_EXCH_ID': 'category',     # Big memory saver
                    'SEM_OPTION_TYPE': 'category',
                    'SEM_STRIKE_PRICE': 'float32'
                }

                # 3. Load Data
                self.df = pd.read_csv(self.filename, usecols=use_cols, dtype=dtype_map, low_memory=False)
                
                # 4. FILTERING (Keep only what we need)
                valid_exchanges = ['NSE', 'BSE', 'MCX']
                self.df = self.df[self.df['SEM_EXM_EXCH_ID'].isin(valid_exchanges)]
                
                valid_instruments = [
                    'EQUITY', 'INDEX',             # Cash
                    'FUTIDX', 'FUTSTK', 'FUTCOM',  # Futures
                    'OPTIDX', 'OPTSTK', 'OPTCOM'   # Options
                ]
                self.df = self.df[self.df['SEM_INSTRUMENT_NAME'].isin(valid_instruments)]

                # 5. Pre-calculate Search Key (Upper case for case-insensitive search)
                self.df['SEARCH_KEY'] = self.df['SEM_TRADING_SYMBOL'].str.upper()
                
                # 6. Create Display Name Column (Vectorized for speed)
                # Format: "SYMBOL (TYPE)" -> e.g. "NIFTY (INDEX)", "RELIANCE (EQUITY)"
                self.df['DISPLAY'] = self.df['SEM_TRADING_SYMBOL'] + " (" + self.df['SEM_INSTRUMENT_NAME'].astype(str) + ")"

                # 7. Convert Dates
                self.df['EXPIRY_DT'] = pd.to_datetime(self.df['SEM_EXPIRY_DATE'], errors='coerce')

                # 8. Cleanup & Flag Ready
                self.is_ready = True
                print(f"✅ Symbol Manager Ready: {len(self.df)} instruments loaded.")
                
                # Force Garbage Collection to free up RAM used during loading
                gc.collect()

            except Exception as e:
                print(f"⚠️ Error loading CSV: {e}")

    def search(self, query):
        """
        Returns top 15 matches. Returns empty if loading not complete.
        """
        if not self.is_ready or self.df is None:
            return []
        
        try:
            query = query.upper().strip()
            
            # Filter matches
            mask = self.df['SEARCH_KEY'].str.contains(query, na=False)
            results = self.df[mask].head(15) # Limit results for speed
            
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
        except Exception as e:
            print(f"Search Error: {e}")
            return []

    def get_atm_security(self, index_symbol, spot_price, direction):
        """
        Finds ATM Option ID. Used by Algo Engine.
        """
        if not self.is_ready or self.df is None: return None, None
        
        try:
            # 1. Determine Step Size
            step = 100 if "BANK" in index_symbol.upper() else 50
            strike = round(spot_price / step) * step
            opt_type = "CE" if direction == "BUY" else "PE"

            # 2. Filter (Using Vectorized String Search)
            mask = (
                (self.df['SEM_INSTRUMENT_NAME'].isin(['OPTIDX', 'OPTSTK'])) &
                (self.df['SEARCH_KEY'].str.contains(index_symbol.upper())) &
                (self.df['SEM_STRIKE_PRICE'] == strike) &
                (self.df['SEM_OPTION_TYPE'] == opt_type) &
                (self.df['EXPIRY_DT'] >= datetime.now())
            )
            
            # 3. Find nearest expiry
            matches = self.df[mask].sort_values('EXPIRY_DT')
            
            if not matches.empty:
                row = matches.iloc[0]
                return str(row['SEM_SMST_SECURITY_ID']), row['DISPLAY']
        except Exception as e:
            print(f"ATM Fetch Error: {e}")
            
        return None, None
