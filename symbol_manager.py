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
        
        # Download if missing
        if not os.path.exists(self.filename):
            self.download_scrips()
        else:
            self.load_instruments()

    def download_scrips(self):
        """Downloads the latest Scrip Master from Dhan."""
        print("⬇️ Downloading Instrument List...")
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
        """Loads CSV into Memory for fast searching."""
        if os.path.exists(self.filename):
            try:
                # Load specific columns to save memory
                use_cols = [
                    'SEM_EXM_EXCH_ID', 'SEM_SMST_SECURITY_ID', 
                    'SEM_TRADING_SYMBOL', 'SEM_INSTRUMENT_NAME',
                    'SEM_EXPIRY_DATE', 'SEM_STRIKE_PRICE', 'SEM_OPTION_TYPE'
                ]
                self.df = pd.read_csv(self.filename, low_memory=False, usecols=use_cols)
                
                # Filter for NSE/BSE/MCX only
                self.df = self.df[self.df['SEM_EXM_EXCH_ID'].isin(['NSE', 'BSE', 'MCX'])]
                
                # Create a search key
                self.df['SEARCH_KEY'] = self.df['SEM_TRADING_SYMBOL'].astype(str).str.upper()
                
                # Convert Expiry to datetime for sorting
                self.df['EXPIRY_DT'] = pd.to_datetime(self.df['SEM_EXPIRY_DATE'], errors='coerce')
                
                print(f"✅ Loaded {len(self.df)} Instruments.")
            except Exception as e:
                print(f"⚠️ Error loading CSV: {e}")

    def search(self, query):
        """Returns top 10 matches for the dashboard search."""
        if self.df is None: return []
        
        query = query.upper()
        mask = self.df['SEARCH_KEY'].str.contains(query, na=False)
        results = self.df[mask].head(10)
        
        output = []
        for _, row in results.iterrows():
            output.append({
                "symbol": row['SEM_TRADING_SYMBOL'],
                "id": str(row['SEM_SMST_SECURITY_ID']),
                "exchange": row['SEM_EXM_EXCH_ID'],
                "display": f"{row['SEM_TRADING_SYMBOL']} ({row['SEM_EXM_EXCH_ID']})"
            })
        return output

    def get_atm_security(self, index_symbol, spot_price, direction):
        """
        Finds the ATM Option Security ID.
        1. Round Spot to nearest strike.
        2. Find nearest Expiry.
        3. Match Strike & Option Type (CE/PE).
        """
        if self.df is None: return None, None

        # 1. Determine Strike (Assuming NIFTY/BANKNIFTY steps)
        step = 100 if "BANK" in index_symbol.upper() else 50
        strike = round(spot_price / step) * step
        opt_type = "CE" if direction == "BUY" else "PE"

        # 2. Filter for Options of this Index
        # Assuming index_symbol like "NIFTY" maps to "OPTIDX"
        # We search roughly by symbol name in trading symbol
        mask = (
            (self.df['SEM_INSTRUMENT_NAME'] == 'OPTIDX') &
            (self.df['SEM_TRADING_SYMBOL'].str.contains(index_symbol.upper())) &
            (self.df['SEM_STRIKE_PRICE'] == strike) &
            (self.df['SEM_OPTION_TYPE'] == opt_type) &
            (self.df['EXPIRY_DT'] >= datetime.now())
        )
        
        matches = self.df[mask].sort_values('EXPIRY_DT')
        
        if not matches.empty:
            # Return the nearest expiry match
            row = matches.iloc[0]
            return str(row['SEM_SMST_SECURITY_ID']), row['SEM_TRADING_SYMBOL']
        
        return None, None
