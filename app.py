import asyncio
import json
import struct
import logging
from fastapi import FastAPI, HTTPException, Query
import uvicorn
import websockets

# --- CONFIGURATION ---
CLIENT_ID = "YOUR_CLIENT_ID"       # REPLACE THIS
ACCESS_TOKEN = "YOUR_ACCESS_TOKEN" # REPLACE THIS

# Instruments to Subscribe (Add more as needed)
# 13 = Nifty 50, 25 = Nifty Bank
WATCHLIST = [
    {"ExchangeSegment": "IDX_I", "SecurityId": "13"},
    {"ExchangeSegment": "IDX_I", "SecurityId": "25"}
]

# --- SHARED MEMORY ---
# This dictionary stores real-time prices: { "13": 21500.50, "25": 48000.00 }
live_market_data = {}

# --- LOGGING SETUP ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DhanFeed")

# --- WEBSOCKET SERVICE ---
class DhanWebSocketService:
    def __init__(self):
        self.url = f"wss://api-feed.dhan.co?version=2&token={ACCESS_TOKEN}&clientId={CLIENT_ID}&authType=2"
        self.running = True

    async def connect_and_listen(self):
        """Main loop that keeps the connection alive"""
        while self.running:
            try:
                logger.info("⏳ Connecting to Dhan WebSocket...")
                async with websockets.connect(self.url) as ws:
                    logger.info("✅ Connected to DhanHQ!")
                    
                    # 1. Send Subscription Request
                    req = {
                        "RequestCode": 15,  # Ticker Data (LTP)
                        "InstrumentCount": len(WATCHLIST),
                        "InstrumentList": WATCHLIST
                    }
                    await ws.send(json.dumps(req))
                    logger.info("📡 Subscription sent for Nifty & Bank Nifty")

                    # 2. Listen for Binary Data
                    while self.running:
                        response = await ws.recv()
                        
                        if isinstance(response, bytes):
                            self.parse_binary(response)
                        else:
                            # Handle heartbeat/text messages
                            pass
                            
            except Exception as e:
                logger.error(f"❌ Connection Error: {e}. Reconnecting in 3s...")
                await asyncio.sleep(3)

    def parse_binary(self, data):
        """Unpacks binary data and updates global dictionary"""
        try:
            # Header is 8 bytes
            if len(data) < 8: return

            # Unpack Header: < (Little Endian), B (Code), H (Len), B (Seg), I (ID)
            header = struct.unpack('<BHB I', data[:8])
            packet_code = header[0]
            security_id = str(header[3]) # Convert ID to string for dict key

            # Packet Code 2 = Ticker Packet (LTP)
            if packet_code == 2:
                # Payload starts at byte 8: < f (Float LTP), I (Int Time)
                payload = struct.unpack('<f I', data[8:16])
                ltp = round(payload[0], 2)

                # UPDATE SHARED MEMORY
                live_market_data[security_id] = ltp
                
                # Optional: Debug Print (Uncomment to see stream)
                # print(f"⚡ Update: {security_id} -> {ltp}")
                
        except Exception as e:
            logger.error(f"Parse Error: {e}")

# --- WEB SERVER (FastAPI) ---
app = FastAPI(title="DhanAlgoWeb Feed")

@app.on_event("startup")
async def startup_event():
    """Start the WebSocket in the background when server starts"""
    feed = DhanWebSocketService()
    # Run in background without blocking the server
    asyncio.create_task(feed.connect_and_listen())

@app.get("/")
async def root():
    """Dashboard to see all data"""
    return {
        "status": "Live",
        "data": live_market_data
    }

@app.get("/api/ltp")
async def get_ltp(id: str = Query(...), segment: str = Query(None)):
    """
    Simulates the endpoint your old system was calling.
    Example: /api/ltp?id=13&segment=IDX_I
    """
    price = live_market_data.get(id)
    
    if price is None:
        # If data hasn't arrived yet
        return {"status": "waiting", "message": f"No data for ID {id} yet"}
    
    # Return formatted exactly like a standard API response
    return {
        "security_id": id,
        "segment": segment,
        "ltp": price
    }

# --- RUNNER ---
if __name__ == "__main__":
    # Runs the server on port 8000
    uvicorn.run(app, host="0.0.0.0", port=8000)
