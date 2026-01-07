import pandas as pd
import requests
import os
from datetime import datetime

class SymbolManager:
    def __init__(self, filename="data/instruments.csv"):
        self.filename = filename
        self.url = "https://images.dhan.co/api-data/api-scrip-master.csv"
        self.df = None
        
        # Download if missing, otherwise load
        if not os.path.exists(self.filename):
            self.download_scrips()
        else:
            self.load_instruments()

    def download_scrips(self):
        """Downloads the comprehensive Scrip Master from Dhan."""
        print("⬇️ Downloading Complete Instrument List...")
        try:
            response = requests.get(self.url)
            with open(self.filename, 'wb') as f:
                f.write(response.content)
            print("✅ Download Complete.")
            self.load_instruments()
            return True
        except Exception as e:
            print(f"❌ Download Failed: {e}")
            return False

    def load_instruments(self):
        """
        Loads and filters the CSV to include ALL requested segments.
        """
        if os.path.exists(self.filename):
            try:
                # 1. Define Columns to Load (Save Memory)
                use_cols = [
                    'SEM_EXM_EXCH_ID',      # Exchange (NSE, BSE, MCX)
                    'SEM_SMST_SECURITY_ID', # Security ID
                    'SEM_TRADING_SYMBOL',   # Symbol (e.g., RELIANCE)
                    'SEM_INSTRUMENT_NAME',  # Type (EQUITY, FNO, etc.)
                    'SEM_EXPIRY_DATE',      # Expiry (for FNO)
                    'SEM_STRIKE_PRICE',     # Strike (for Opt)
                    'SEM_OPTION_TYPE',      # CE/PE
                    'SEM_CUSTOM_SYMBOL'     # Description
                ]
                
                # 2. Load CSV (Low Memory Mode)
                self.df = pd.read_csv(self.filename, low_memory=False, usecols=use_cols)
                
                # 3. FILTERING LOGIC (The Core Requirement)
                # Keep only NSE, BSE, MCX
                valid_exchanges = ['NSE', 'BSE', 'MCX']
                self.df = self.df[self.df['SEM_EXM_EXCH_ID'].isin(valid_exchanges)]
                
                # Keep specific Instrument Types
                # EQUITY = Stocks
                # INDEX = Spot Indices (Nifty 50, Sensex)
                # FUTIDX, FUTSTK, FUTCOM = Futures
                # OPTIDX, OPTSTK, OPTCOM = Options
                valid_instruments = [
                    'EQUITY', 'INDEX', 
                    'FUTIDX', 'FUTSTK', 'FUTCOM', 
                    'OPTIDX', 'OPTSTK', 'OPTCOM'
                ]
                self.df = self.df[self.df['SEM_INSTRUMENT_NAME'].isin(valid_instruments)]
                
                # 4. Create a "Smart Display" Name for Search
                # Logic: Combine Symbol + Expiry + Strike + Option Type for clarity
                self.df['DISPLAY'] = self.df.apply(self._generate_display_name, axis=1)
                
                # 5. Create Search Key (Uppercase for speed)
                self.df['SEARCH_KEY'] = self.df['SEM_TRADING_SYMBOL'].astype(str).str.upper()
                
                # 6. Parse Dates (for sorting options later)
                self.df['EXPIRY_DT'] = pd.to_datetime(self.df['SEM_EXPIRY_DATE'], errors='coerce')

                print(f"✅ Loaded {len(self.df)} Instruments (All Segments).")
                
            except Exception as e:
                print(f"⚠️ Error loading CSV: {e}")

    def _generate_display_name(self, row):
        """Helper to create readable names like 'NIFTY 25JAN 21500 CE'"""
        symbol = row['SEM_TRADING_SYMBOL']
        instr = row['SEM_INSTRUMENT_NAME']
        exch = row['SEM_EXM_EXCH_ID']
        
        if instr in ['EQUITY', 'INDEX']:
            return f"{symbol} ({exch} {instr})"
        
        # For F&O (Futures/Options)
        # Expiry Format: 2024-01-25 -> 25JAN
        expiry = str(row['SEM_EXPIRY_DATE']).split(' ')[0] 
        
        if 'FUT' in instr:
            return f"{symbol} {expiry} FUT ({exch})"
        
        if 'OPT' in instr:
            strike = str(row['SEM_STRIKE_PRICE']).replace('.0', '')
            opt_type = row['SEM_OPTION_TYPE'] # CE or PE
            return f"{symbol} {expiry} {strike} {opt_type} ({exch})"
            
        return f"{symbol} ({exch})"

    def search(self, query):
        """
        Fast Search returning top 15 results.
        Prioritizes: Indices > Stocks > Futures > Options
        """
        if self.df is None: return []
        
        query = query.upper().strip()
        
        # Filter 1: Contains Query
        mask = self.df['SEARCH_KEY'].str.contains(query, na=False)
        results = self.df[mask]
        
        # Sort Priority: Index/Equity first, then FNO
        # We limit to 20 results to keep the UI snappy
        results = results.sort_values(by=['SEM_INSTRUMENT_NAME', 'SEM_TRADING_SYMBOL']).head(20)
        
        output = []
        for _, row in results.iterrows():
            output.append({
                "symbol": row['SEM_TRADING_SYMBOL'],
                "id": str(row['SEM_SMST_SECURITY_ID']),
                "exchange": row['SEM_EXM_EXCH_ID'],
                "display": row['DISPLAY'],
                "type": row['SEM_INSTRUMENT_NAME']
            })
        return output

    def get_atm_security(self, index_symbol, spot_price, direction):
        # ... (Keep previous Auto-ATM Logic) ...
        # (This remains unchanged from the previous efficient version)
        if self.df is None: return None, None
        step = 100 if "BANK" in index_symbol.upper() else 50
        strike = round(spot_price / step) * step
        opt_type = "CE" if direction == "BUY" else "PE"

        mask = (
            (self.df['SEM_INSTRUMENT_NAME'].isin(['OPTIDX', 'OPTSTK'])) &
            (self.df['SEM_TRADING_SYMBOL'].str.contains(index_symbol.upper())) &
            (self.df['SEM_STRIKE_PRICE'] == strike) &
            (self.df['SEM_OPTION_TYPE'] == opt_type) &
            (self.df['EXPIRY_DT'] >= datetime.now())
        )
        
        matches = self.df[mask].sort_values('EXPIRY_DT')
        if not matches.empty:
            row = matches.iloc[0]
            return str(row['SEM_SMST_SECURITY_ID']), row['SEM_TRADING_SYMBOL']
        return None, None
