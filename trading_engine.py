import threading
import time
import json
import os
import pandas as pd
from datetime import datetime
import pytz
from dhanhq import DhanContext, dhanhq

class TradingEngine:
    def __init__(self, config, notifier, symbol_manager, filename="data/trades.json"):
        self.cfg = config
        self.notify = notifier
        self.sym_mgr = symbol_manager
        self.filename = filename
        
        self.active_trades = self.load_trades()
        self.dhan = None
        self.is_connected = False
        
        # Start Background Thread
        self.stop_event = threading.Event()
        self.monitor_thread = threading.Thread(target=self.run_loop)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
        
        self.connect_api()

    def connect_api(self):
        c = self.cfg.config['dhan_creds']
        if c['client_id'] and c['access_token']:
            try:
                # Initialize Dhan V2
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

    def calculate_targets(self, entry, sl_points, direction):
        """Auto-Calculate 5 Targets based on Risk."""
        targets = {}
        factor = 1 if direction == "BUY" else -1
        
        # Levels: 0.5x, 1x, 1.5x, 2x, 3x
        multipliers = [0.5, 1.0, 1.5, 2.0, 3.0]
        
        sl_price = entry - (sl_points * factor)
        
        for i, m in enumerate(multipliers):
            targets[f"T{i+1}"] = entry + (sl_points * m * factor)
            
        return sl_price, targets

    def place_trade(self, symbol, security_id, direction, qty, channel, sl_points, mode="PAPER"):
        """Main execution function."""
        target_channel, forced = self.cfg.get_target_channel(channel)
        
        # 1. Get Live Price (Mock if Paper/Disconnected, Real if Connected)
        entry_price = 0.0
        if self.is_connected and security_id:
            try:
                # Fetch Quote
                quote = self.dhan.get_quote(self.dhan.NSE, security_id)
                entry_price = float(quote['data']['last_price'])
            except:
                entry_price = 100.0 # Fallback
        else:
            entry_price = 100.0 # Paper Mock
            
        # 2. Execute Live Order
        if mode == "LIVE" and self.is_connected:
            try:
                self.dhan.place_order(
                    security_id=security_id,
                    exchange_segment=self.dhan.NSE_FNO,
                    transaction_type=self.dhan.BUY if direction == "BUY" else self.dhan.SELL,
                    quantity=qty,
                    order_type=self.dhan.MARKET,
                    product_type=self.dhan.INTRA,
                    price=0
                )
            except Exception as e:
                return f"API Error: {e}"

        # 3. Risk Calculation
        sl_price, targets = self.calculate_targets(entry_price, sl_points, direction)
        
        trade_id = f"{symbol}_{int(time.time())}"
        self.active_trades[trade_id] = {
            "id": trade_id, "symbol": symbol, "sec_id": security_id,
            "direction": direction, "qty": qty, "entry_price": entry_price,
            "sl_price": sl_price, "targets": targets, "channel": target_channel,
            "mode": mode, "max_price": entry_price, "t1_hit": False, "status": "ACTIVE"
        }
        self.save_trades()
        self.cfg.increment_trade_count(target_channel)

        # 4. Notify
        self.notify.notify_add(target_channel, symbol, direction, mode)
        self.notify.notify_active(target_channel, self.active_trades[trade_id])
        
        return "Trade Executed Successfully"

    def convert_to_live(self, trade_id):
        t = self.active_trades.get(trade_id)
        if t and t['mode'] == "PAPER":
            # Execute Real Order logic reused
            res = self.place_trade(t['symbol'], t['sec_id'], t['direction'], t['qty'], "VIP Channel", 20, "LIVE")
            
            # Update old paper record to closed/converted
            t['status'] = "CONVERTED_TO_LIVE"
            self.save_trades()
            return f"Converted: {res}"
        return "Invalid Trade"

    def run_loop(self):
        """Background Monitor: Time Strategy + Risk Manager"""
        while not self.stop_event.is_set():
            # A. TIME BASED STRATEGY (09:54)
            now = datetime.now(pytz.timezone('Asia/Kolkata'))
            if now.strftime("%H:%M:%S") == "09:54:00":
                # 1. Get Spot Price for NIFTY (Mock 21500 if no data)
                spot = 21500 
                if self.is_connected:
                    try:
                        q = self.dhan.get_quote(self.dhan.NSE, "13") # 13 is NIFTY 50 ID
                        spot = float(q['data']['last_price'])
                    except: pass
                
                # 2. Find ATM Security ID
                sec_id, sym_name = self.sym_mgr.get_atm_security("NIFTY", spot, "BUY")
                
                if sec_id:
                    print(f"⏰ Executing Time Strategy: {sym_name}")
                    self.place_trade(sym_name, sec_id, "BUY", 50, "VIP Channel", 20, "PAPER")
                    time.sleep(1.5) # Prevent double trigger

            # B. RISK MANAGEMENT (Monitor active trades)
            for tid, t in list(self.active_trades.items()):
                if t['status'] != "ACTIVE": continue

                # Get LTP
                ltp = t['entry_price'] # Default
                if self.is_connected and t['sec_id']:
                    try:
                        q = self.dhan.get_quote(self.dhan.NSE_FNO, t['sec_id'])
                        ltp = float(q['data']['last_price'])
                    except: 
                        import random
                        ltp += random.uniform(-2, 5) # Mock movement

                # 1. Update Max High
                if t['direction'] == "BUY":
                    if ltp > t['max_price']: t['max_price'] = ltp
                else:
                    if ltp < t['max_price']: t['max_price'] = ltp

                # 2. Check Target 1 (Safe Mode)
                t1 = t['targets']['T1']
                if not t['t1_hit']:
                    if (t['direction'] == "BUY" and ltp >= t1) or (t['direction'] == "SELL" and ltp <= t1):
                        t['t1_hit'] = True
                        t['sl_price'] = t['entry_price'] # SL to Cost
                        self.notify.notify_update(t['channel'], t['symbol'], "Target 1 Hit! SL Moved to Cost.")

                # 3. Check Exit (SL or Target 5)
                reason = None
                if t['direction'] == "BUY":
                    if ltp <= t['sl_price']: reason = "SL Hit"
                    elif ltp >= t['targets']['T5']: reason = "Target 5 Hit"
                else:
                    if ltp >= t['sl_price']: reason = "SL Hit"
                    elif ltp <= t['targets']['T5']: reason = "Target 5 Hit"
                
                if reason:
                    self.notify.notify_exit(t['channel'], t, reason, ltp)
                    del self.active_trades[tid]
                    self.save_trades()

            time.sleep(1)
