import pandas as pd
import requests
import os
import time

class SymbolManager:
    def __init__(self, filename="data/instruments.csv"):
        self.filename = filename
        self.url = "https://images.dhan.co/api-data/api-scrip-master.csv"
        self.df = None
        self.load_instruments()

    def download_scrips(self):
        """Downloads the latest Scrip Master from Dhan"""
        print("⬇️ Downloading Instrument List (This happens once)...")
        try:
            # Download CSV
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
        """Loads CSV into Memory for fast searching"""
        if os.path.exists(self.filename):
            # Load only necessary columns to save memory
            # Columns: SEM_EXM_EXCH_ID, SEM_SMST_SECURITY_ID, SEM_TRADING_SYMBOL, SEM_CUSTOM_SYMBOL
            try:
                self.df = pd.read_csv(self.filename, low_memory=False)
                
                # Filter for useful exchanges only
                self.df = self.df[self.df['SEM_EXM_EXCH_ID'].isin(['NSE', 'BSE', 'MCX'])]
                
                # Create a search column
                self.df['SEARCH_KEY'] = self.df['SEM_TRADING_SYMBOL'].astype(str).str.upper()
                print(f"✅ Loaded {len(self.df)} Instruments.")
            except Exception as e:
                print(f"⚠️ Error loading CSV: {e}")

    def search(self, query):
        """Returns top 10 matches for the query"""
        if self.df is None:
            return []
        
        query = query.upper()
        # Filter: Symbol starts with query
        mask = self.df['SEARCH_KEY'].str.contains(query, na=False)
        results = self.df[mask].head(10)
        
        # Format for Frontend
        output = []
        for _, row in results.iterrows():
            output.append({
                "symbol": row['SEM_TRADING_SYMBOL'],
                "id": str(row['SEM_SMST_SECURITY_ID']),
                "exchange": row['SEM_EXM_EXCH_ID'],
                "display": f"{row['SEM_TRADING_SYMBOL']} ({row['SEM_EXM_EXCH_ID']})"
            })
        return output
