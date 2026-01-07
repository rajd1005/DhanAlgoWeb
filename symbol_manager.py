import pandas as pd
import requests
import os
from datetime import datetime

class SymbolManager:
    def __init__(self, filename="data/instruments.csv"):
        self.filename = filename
        # Official Dhan Scrip Master URL
        self.url = "https://images.dhan.co/api-data/api-scrip-master.csv"
        self.df = None
        
        # Ensure data directory exists
        folder = os.path.dirname(filename)
        if folder and not os.path.exists(folder):
            os.makedirs(folder)
        
        # Download if missing, otherwise load
        if not os.path.exists(self.filename):
            print("⚠️ Scrip Master not found. Downloading now...")
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
                    'SEM_CUSTOM_SYMBOL',    # Description
                    'SEM_LOT_UNITS'         # Lot Size (Useful for display)
                ]
                
                # 2. Load CSV (Low Memory Mode)
                # We use specific dtypes to optimize speed
                dtype_map = {
                    'SEM_SMST_SECURITY_ID': str,
                    'SEM_STRIKE_PRICE': float
                }
                self.df = pd.read_csv(self.filename, low_memory=False, usecols=use_cols, dtype=dtype_map)
                
                # 3. FILTERING LOGIC
                # Keep only NSE, BSE, MCX
                valid_exchanges = ['NSE', 'BSE', 'MCX']
                self.df = self.df[self.df['SEM_EXM_EXCH_ID'].isin(valid_exchanges)]
                
                # Keep specific Instrument Types
                valid_instruments = [
                    'EQUITY', 'INDEX',       # Cash / Spot
                    'FUTIDX', 'FUTSTK', 'FUTCOM',  # Futures
                    'OPTIDX', 'OPTSTK', 'OPTCOM'   # Options
                ]
                self.df = self.df[self.df['SEM_INSTRUMENT_NAME'].isin(valid_instruments)]
                
                # 4. Create a "Smart Display" Name for Search
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
        
        # Clean up Symbol Name (remove spaces)
        symbol = str(symbol).strip()

        if instr == 'EQUITY':
            return f"{symbol} (EQ)"
        if instr == 'INDEX':
            return f"{symbol} (INDEX)"
        
        # For F&O (Futures/Options)
        # Parse Expiry: "2024-01-25 00:00:00" -> "25JAN"
        try:
            exp_date = pd.to_datetime(row['SEM_EXPIRY_DATE'])
            expiry_str = exp_date.strftime("%d%b").upper()
        except:
            expiry_str = ""
        
        if 'FUT' in instr:
            return f"{symbol} {expiry_str} FUT ({exch})"
        
        if 'OPT' in instr:
            # Format Strike: 21500.0 -> 21500
            strike = f"{row['SEM_STRIKE_PRICE']:.0f}"
            opt_type = row['SEM_OPTION_TYPE'] # CE or PE
            return f"{symbol} {expiry_str} {strike} {opt_type} ({exch})"
            
        return f"{symbol} ({exch})"

    def search(self, query):
        """
        Fast Search returning top 20 results.
        Prioritizes: Indices > Stocks > Futures > Options
        """
        if self.df is None: return []
        
        query = query.upper().strip()
        
        # Filter: Contains Query
        mask = self.df['SEARCH_KEY'].str.contains(query, na=False)
        results = self.df[mask]
        
        # Sort Priority: 
        # 1. Exact match starts with query (e.g. "NIFTY" matches "NIFTY 50" before "BANKNIFTY")
        # 2. Instrument Type (INDEX > EQUITY > FUT > OPT)
        
        # Simple Sort by Name and Type
        results = results.sort_values(by=['SEM_TRADING_SYMBOL', 'SEM_INSTRUMENT_NAME']).head(20)
        
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
        """
        Finds the nearest ATM Option Security ID for Algo Trading.
        """
        if self.df is None: return None, None
        
        # Determine Step Size
        step = 100 if "BANK" in index_symbol.upper() else 50
        strike = round(spot_price / step) * step
        opt_type = "CE" if direction == "BUY" else "PE"

        # Filter for Options
        mask = (
            (self.df['SEM_INSTRUMENT_NAME'].isin(['OPTIDX', 'OPTSTK'])) &
            (self.df['SEM_TRADING_SYMBOL'].str.contains(index_symbol.upper())) &
            (self.df['SEM_STRIKE_PRICE'] == strike) &
            (self.df['SEM_OPTION_TYPE'] == opt_type) &
            (self.df['EXPIRY_DT'] >= datetime.now())
        )
        
        matches = self.df[mask].sort_values('EXPIRY_DT')
        
        if not matches.empty:
            row = matches.iloc[0] # Get nearest expiry
            return str(row['SEM_SMST_SECURITY_ID']), row['DISPLAY']
            
        return None, None
