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
        self.is_ready = False 
        
        folder = os.path.dirname(filename)
        if folder and not os.path.exists(folder):
            os.makedirs(folder)
        
        self.loader_thread = threading.Thread(target=self._background_init)
        self.loader_thread.daemon = True
        self.loader_thread.start()

    def _background_init(self):
        print("⏳ Symbol Manager: Initializing in background...")
        if not os.path.exists(self.filename):
            print("⬇️ Downloading Scrip Master...")
            if not self.download_scrips():
                print("❌ Download Failed.")
                return
        self.load_instruments()

    def download_scrips(self):
        try:
            response = requests.get(self.url, timeout=60)
            with open(self.filename, 'wb') as f:
                f.write(response.content)
            return True
        except Exception as e:
            print(f"❌ Error downloading CSV: {e}")
            return False

    def load_instruments(self):
        if os.path.exists(self.filename):
            try:
                # Load necessary columns
                use_cols = [
                    'SEM_EXM_EXCH_ID', 'SEM_SMST_SECURITY_ID', 
                    'SEM_TRADING_SYMBOL', 'SEM_INSTRUMENT_NAME', 
                    'SEM_EXPIRY_DATE', 'SEM_STRIKE_PRICE', 'SEM_OPTION_TYPE',
                    'SEM_CUSTOM_SYMBOL' # Description (Fixes "Nifty 50" Search)
                ]
                dtype_map = {
                    'SEM_SMST_SECURITY_ID': 'str',
                    'SEM_TRADING_SYMBOL': 'str',
                    'SEM_CUSTOM_SYMBOL': 'str',
                    'SEM_INSTRUMENT_NAME': 'category',
                    'SEM_EXM_EXCH_ID': 'category',
                    'SEM_OPTION_TYPE': 'category',
                    'SEM_STRIKE_PRICE': 'float32'
                }

                self.df = pd.read_csv(self.filename, usecols=use_cols, dtype=dtype_map, low_memory=False)
                
                # Filter Valid Data
                self.df = self.df[self.df['SEM_EXM_EXCH_ID'].isin(['NSE', 'BSE', 'MCX'])]
                valid_instruments = ['EQUITY', 'INDEX', 'FUTIDX', 'FUTSTK', 'FUTCOM', 'OPTIDX', 'OPTSTK', 'OPTCOM']
                self.df = self.df[self.df['SEM_INSTRUMENT_NAME'].isin(valid_instruments)]

                # Setup Search Keys
                self.df['SEARCH_KEY'] = self.df['SEM_TRADING_SYMBOL'].str.upper()
                self.df['DESC_KEY'] = self.df['SEM_CUSTOM_SYMBOL'].str.upper().fillna("") # Search description
                self.df['DISPLAY'] = self.df['SEM_TRADING_SYMBOL'] + " (" + self.df['SEM_INSTRUMENT_NAME'].astype(str) + ")"
                self.df['EXPIRY_DT'] = pd.to_datetime(self.df['SEM_EXPIRY_DATE'], errors='coerce')

                # API Segment Mapping
                def get_segment(row):
                    exch = row['SEM_EXM_EXCH_ID']
                    instr = row['SEM_INSTRUMENT_NAME']
                    if instr == 'INDEX': return 'IDX_I'
                    if exch == 'MCX': return 'MCX_COMM'
                    if exch == 'NSE': return 'NSE_EQ' if instr == 'EQUITY' else 'NSE_FNO'
                    if exch == 'BSE': return 'BSE_EQ' if instr == 'EQUITY' else 'BSE_FNO'
                    return 'NSE_EQ'

                self.df['API_SEGMENT'] = self.df.apply(get_segment, axis=1)

                self.is_ready = True
                print(f"✅ Symbol Manager Ready: {len(self.df)} instruments loaded.")
                gc.collect()

            except Exception as e:
                print(f"⚠️ Error loading CSV: {e}")

    def search(self, query):
        if not self.is_ready or self.df is None: return []
        
        try:
            query = query.upper().strip()
            
            # 1. Match Symbol OR Description
            mask_sym = self.df['SEARCH_KEY'].str.contains(query, na=False)
            mask_desc = self.df['DESC_KEY'].str.contains(query, na=False)
            mask_query = mask_sym | mask_desc
            
            # 2. Restrict Search Box to relevant types
            allowed_types = ['INDEX', 'EQUITY', 'FUTIDX', 'FUTSTK', 'FUTCOM']
            mask_type = self.df['SEM_INSTRUMENT_NAME'].isin(allowed_types)
            
            results = self.df[mask_query & mask_type].copy()
            
            # 3. Sort: Index First, then Shortest Name
            results['is_index'] = results['SEM_INSTRUMENT_NAME'] == 'INDEX'
            results['len'] = results['SEM_TRADING_SYMBOL'].str.len()
            
            results = results.sort_values(
                by=['is_index', 'len', 'SEM_TRADING_SYMBOL'], 
                ascending=[False, True, True]
            ).head(20)
            
            output = []
            for _, row in results.iterrows():
                output.append({
                    "symbol": row['SEM_TRADING_SYMBOL'],
                    "id": row['SEM_SMST_SECURITY_ID'],
                    "exchange": row['SEM_EXM_EXCH_ID'],
                    "display": row['DISPLAY'],
                    "segment": row['API_SEGMENT'],
                    "type": row['SEM_INSTRUMENT_NAME']
                })
            return output
        except Exception as e:
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
        except: pass
        return None, None
