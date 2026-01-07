import threading
import time
import json
import os
import pandas as pd
from datetime import datetime
import pytz
from dhanhq import DhanContext, dhanhq

class TradingEngine:
    def __init__(self, config, notifier, filename="data/trades.json"):
        self.cfg = config
        self.notify = notifier
        self.filename = filename
        self.active_trades = self.load_trades()
        self.dhan = None
        self.is_connected = False
        
        # Start Background Thread for Monitoring & Time Strategy
        self.stop_event = threading.Event()
        self.monitor_thread = threading.Thread(target=self.run_loop)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
        
        self.connect_api()

    def connect_api(self):
        c = self.cfg.config['dhan_creds']
        if c['client_id'] and c['access_token']:
            try:
                self.dhan = dhanhq(c['client_id'], c['access_token'])
                self.is_connected = True
                return True
            except:
                self.is_connected = False
        return False

    def load_trades(self):
        if os.path.exists(self.filename):
            try:
                with open(self.filename, 'r') as f: return json.load(f)
            except: return {}
        return {}

    def save_trades(self):
        with open(self.filename, 'w') as f: json.dump(self.active_trades, f, indent=4)

    # --- 1. SMART HELPERS (Auto-ATM & Expiry) ---
    def get_atm_contract(self, underlying_symbol, direction):
        """Mock logic for finding ATM (Replace with real Option Chain fetch if needed)"""
        # In a full system, you'd fetch the underlying price first.
        # For this example, we assume NIFTY is ~21500
        spot = 21500 # This should be fetched via self.dhan.get_quote()
        strike = round(spot / 50) * 50
        opt_type = "CE" if direction == "BUY" else "PE"
        # Return a dummy string or fetch real Security ID from CSV/API
        return f"{underlying_symbol} {strike} {opt_type}", strike

    def calculate_targets(self, entry, sl_points, direction):
        """Auto 5 Targets"""
        targets = {}
        factor = 1 if direction == "BUY" else -1
        multipliers = [0.5, 1.0, 1.5, 2.0, 3.0]
        
        sl_price = entry - (sl_points * factor)
        
        for i, m in enumerate(multipliers):
            targets[f"T{i+1}"] = entry + (sl_points * m * factor)
            
        return sl_price, targets

    # --- 2. TRADE EXECUTION ---
    def place_trade(self, symbol, direction, qty, channel, sl_points, trail_pts=0, mode="PAPER"):
        target_channel, forced = self.cfg.get_target_channel(channel)
        
        # Auto-Select ATM if symbol is just index name
        if symbol in ["NIFTY", "BANKNIFTY", "FINNIFTY"]:
            symbol, strike = self.get_atm_contract(symbol, direction)
            
        # Get Entry Price (Mock or Real)
        entry_price = 100.0 # Replace with self.dhan.get_quote(...)['last_price']
        
        if mode == "LIVE" and self.is_connected:
            try:
                # self.dhan.place_order(...) # Call real API
                pass
            except Exception as e: return f"API Error: {e}"

        # Risk Calc
        sl_price, targets = self.calculate_targets(entry_price, sl_points, direction)
        
        trade_id = f"{symbol}_{int(time.time())}"
        self.active_trades[trade_id] = {
            "id": trade_id, "symbol": symbol, "direction": direction, "qty": qty,
            "entry_price": entry_price, "sl_price": sl_price, "targets": targets,
            "channel": target_channel, "trail_pts": trail_pts, "mode": mode,
            "max_price": entry_price, "t1_hit": False, "status": "ACTIVE"
        }
        self.save_trades()
        self.cfg.increment_trade_count(target_channel)

        # Notify
        self.notify.notify_add(target_channel, symbol, direction, mode)
        self.notify.notify_active(target_channel, self.active_trades[trade_id])
        
        return "Trade Executed"

    def convert_to_live(self, trade_id):
        t = self.active_trades.get(trade_id)
        if t and t['mode'] == "PAPER":
            # Execute Real Order Logic Here
            t['mode'] = "LIVE"
            self.save_trades()
            self.notify.notify_update(t['channel'], t['symbol'], "🚨 *Switched to LIVE Execution*")
            return "Converted to Live"
        return "Invalid Trade"

    # --- 3. BACKGROUND MONITOR (Risk, Time, Trailing) ---
    def run_loop(self):
        while not self.stop_event.is_set():
            # A. Time Based Strategy Check (e.g. 09:54)
            now = datetime.now(pytz.timezone('Asia/Kolkata'))
            if now.strftime("%H:%M:%S") == "09:54:00":
                # Auto-Execute Morning Strategy
                self.place_trade("NIFTY", "BUY", 25, "VIP Channel", sl_points=20, mode="PAPER")
                time.sleep(1) 

            # B. Monitor Active Trades
            # In production, use self.dhan.quote() for real LTPs
            # Here we simulate random movement for demonstration
            for tid, t in list(self.active_trades.items()):
                if t['status'] != "ACTIVE": continue

                # Mock LTP (Replace with real API fetch)
                import random
                current_ltp = t['entry_price'] + random.uniform(-5, 10) 
                
                # 1. Update Max High
                if t['direction'] == "BUY":
                    if current_ltp > t['max_price']: t['max_price'] = current_ltp
                else:
                    if current_ltp < t['max_price']: t['max_price'] = current_ltp

                # 2. Check T1 (Safe Guard) -> Suppress SL Notify
                t1 = t['targets']['T1']
                if not t['t1_hit']:
                    if (t['direction'] == "BUY" and current_ltp >= t1) or \
                       (t['direction'] == "SELL" and current_ltp <= t1):
                        t['t1_hit'] = True
                        t['sl_price'] = t['entry_price'] # Move SL to Cost
                        self.notify.notify_update(t['channel'], t['symbol'], "Target 1 Hit! SL Moved to Cost.")

                # 3. Trailing SL (Points)
                if t['trail_pts'] > 0:
                    # Logic: If price moves X pts, move SL X pts.
                    pass # (Implement detailed step logic here)

                # 4. Check Exit (SL or T5)
                reason = None
                if t['direction'] == "BUY":
                    if current_ltp <= t['sl_price']: reason = "SL Hit"
                    elif current_ltp >= t['targets']['T5']: reason = "Target 5 Hit"
                
                if reason:
                    # Execute Exit Order
                    self.notify.notify_exit(t['channel'], t, reason, current_ltp)
                    del self.active_trades[tid]
                    self.save_trades()

            time.sleep(1)
