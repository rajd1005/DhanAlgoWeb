from dhanhq import DhanContext, dhanhq
import threading
import time
import json
import os
from datetime import datetime

class TradingEngine:
    # FIXED LINE: We replaced '...' with the real arguments
    def __init__(self, config_manager, notifier, filename="data/active_trades.json"):
        self.cfg = config_manager
        self.notify = notifier
        self.filename = filename
        
        # Auto-create the data folder if it doesn't exist
        folder = os.path.dirname(filename)
        if folder and not os.path.exists(folder):
            os.makedirs(folder)

        self.dhan = None
        self.active_trades = self.load_trades() # Load saved trades
        self.is_running = False
        self.connect_api()

    def load_trades(self):
        """Loads trades from the JSON file so they persist after restart."""
        if os.path.exists(self.filename):
            try:
                with open(self.filename, 'r') as f:
                    return json.load(f)
            except:
                return {}
        return {}

    def save_trades(self):
        """Saves current trades to file."""
        with open(self.filename, 'w') as f:
            json.dump(self.active_trades, f, indent=4)

    def connect_api(self):
        creds = self.cfg.config['dhan_creds']
        if creds['client_id'] and creds['access_token']:
            try:
                ctx = DhanContext(creds['client_id'], creds['access_token'])
                self.dhan = dhanhq(ctx)
                return True
            except:
                return False
        return False

    def place_trade(self, symbol, direction, quantity, channel, mode="PAPER"):
        """Places a trade and logs it."""
        # 1. Apply Channel Restrictions
        target_channel, forced = self.cfg.get_target_channel(channel)
        
        # 2. Logic for LIVE vs PAPER
        trade_id = f"{symbol}_{int(time.time())}"
        
        status_msg = ""
        
        if mode == "LIVE":
            if not self.dhan: return "API Not Connected"
            try:
                # Live Execution
                txn = self.dhan.BUY if direction == "BUY" else self.dhan.SELL
                self.dhan.place_order(
                    security_id=symbol, # Ensure this is ID
                    exchange_segment=self.dhan.NSE_FNO,
                    transaction_type=txn,
                    quantity=quantity,
                    order_type=self.dhan.MARKET,
                    product_type=self.dhan.INTRA,
                    price=0
                )
                status_msg = "✅ Order Sent to Exchange"
            except Exception as e:
                return f"Execution Failed: {e}"
        else:
            status_msg = "📝 Paper Trade Logged"

        # 3. Save Trade Data
        self.active_trades[trade_id] = {
            "id": trade_id,
            "symbol": symbol,
            "direction": direction,
            "qty": quantity,
            "mode": mode,
            "entry_time": str(datetime.now().time())[:8],
            "pnl": 0.0,
            "status": "OPEN"
        }
        self.save_trades() # <--- SAVE TO FILE

        # 4. Notify
        msg = f"Symbol: {symbol}\nAction: {direction}\nMode: {mode}\n{status_msg}"
        if forced: msg += "\n(Switched to VIP due to daily limit)"
        
        self.notify.send_alert(target_channel, "New Trade Executed", msg, trade_id if mode=="PAPER" else None)
        self.cfg.increment_trade_count(target_channel)
        
        return "Trade Placed Successfully"

    def convert_to_live(self, trade_id):
        """Converts a Paper Trade to Live."""
        trade = self.active_trades.get(trade_id)
        if not trade or trade['mode'] == "LIVE":
            return "Invalid Trade"

        # Execute Live
        res = self.place_trade(trade['symbol'], trade['direction'], trade['qty'], "VIP Channel", mode="LIVE")
        
        # Update Record
        trade['mode'] = "LIVE"
        trade['status'] = "CONVERTED"
        self.save_trades() # <--- SAVE TO FILE
        return res
