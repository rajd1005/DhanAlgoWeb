import asyncio
import websockets
import json
import struct
import logging
from datetime import datetime

# --- SHARED MEMORY (Simple Storage) ---
# Your Web App can import this dictionary to get the latest prices
live_data = {} 

class DhanFeedService:
    def __init__(self, client_id, access_token, instruments):
        self.client_id = client_id
        self.access_token = access_token
        self.instruments = instruments
        self.url = f"wss://api-feed.dhan.co?version=2&token={access_token}&clientId={client_id}&authType=2"

    async def run(self):
        """Main Infinite Loop"""
        while True:
            try:
                print("Connecting to Dhan Feed...")
                async with websockets.connect(self.url) as ws:
                    print("✅ Connected!")
                    
                    # Subscribe
                    req = {
                        "RequestCode": 15,
                        "InstrumentCount": len(self.instruments),
                        "InstrumentList": self.instruments
                    }
                    await ws.send(json.dumps(req))

                    # Listen
                    while True:
                        res = await ws.recv()
                        if isinstance(res, bytes):
                            self._process_binary(res)
            except Exception as e:
                print(f"⚠️ Connection Error: {e}. Retrying in 5s...")
                await asyncio.sleep(5)

    def _process_binary(self, data):
        """Unpacks data and updates the global 'live_data' dictionary"""
        if len(data) < 8: return
        
        # Header: Code (1B), Len (2B), Segment (1B), ID (4B)
        header = struct.unpack('<BHB I', data[:8])
        packet_code = header[0]
        security_id = header[3]

        # Ticker Packet (LTP)
        if packet_code == 2:
            # LTP (4B Float), Time (4B Int)
            payload = struct.unpack('<f I', data[8:16])
            price = round(payload[0], 2)
            
            # UPDATE SHARED STORAGE
            live_data[security_id] = price
            
            # Optional: Print to console to verify
            # print(f"Update: ID {security_id} -> ₹{price}")

# --- RUNNER ---
if __name__ == "__main__":
    # Test Configuration
    CLIENT_ID = "YOUR_CLIENT_ID"
    ACCESS_TOKEN = "YOUR_JWT_TOKEN"
    
    # Nifty 50 (13) & Bank Nifty (25)
    WATCHLIST = [
        {"ExchangeSegment": "IDX_I", "SecurityId": "13"},
        {"ExchangeSegment": "IDX_I", "SecurityId": "25"}
    ]

    service = DhanFeedService(CLIENT_ID, ACCESS_TOKEN, WATCHLIST)
    asyncio.run(service.run())
