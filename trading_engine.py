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

    # --- NEW HELPERS FOR FRONTEND ---
    
    def get_latest_price(self, security_id, exchange_segment=None):
        """Fetches Live LTP from Dhan."""
        if not self.is_connected: return 0.0
        
        # Default to NSE Equity if not specified
        if not exchange_segment: exchange_segment = self.dhan.NSE
            
        try:
            # Exchange Segment mapping: NSE=1, FNO=2, etc. (Check Dhan Docs)
            # Using basic try-fetch
            quote = self.dhan.get_quote(exchange_segment, security_id)
            if quote and 'data' in quote:
                return float(quote['data']['last_price'])
        except Exception as e:
            print(f"LTP Fetch Error: {e}")
        return 0.0

    def get_option_chain_data(self, symbol, spot_price):
        """
        Generates a small Option Chain (ATM +/- 5 strikes) for the dropdown.
        """
        # 1. Determine Step Size (NIFTY=50, BANKNIFTY=100, Others=Variable)
        step = 100 if "BANK" in symbol.upper() else 50
        
        # 2. Calculate ATM Strike
        atm_strike = round(spot_price / step) * step
        
        # 3. Generate Strike List (5 below, ATM, 5 above)
        strikes = []
        for i in range(-5, 6):
            s = atm_strike + (i * step)
            strikes.append(s)
            
        return strikes, atm_strike

    # --- EXISTING LOGIC ---

    def calculate_targets(self, entry, sl_points, direction):
        targets = {}
        factor = 1 if direction == "BUY" else -1
        multipliers = [0.5, 1.0, 1.5, 2.0, 3.0]
        sl_price = entry - (sl_points * factor)
        for i, m in enumerate(multipliers):
            targets[f"T{i+1}"] = entry + (sl_points * m * factor)
        return sl_price, targets

    def place_trade(self, symbol, security_id, direction, qty, channel, sl_points, mode="PAPER"):
        target_channel, forced = self.cfg.get_target_channel(channel)
        
        # Fetch Real Entry Price if possible
        entry_price = self.get_latest_price(security_id, self.dhan.NSE_FNO)
        if entry_price == 0: entry_price = 100.0 # Fallback
            
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

        self.notify.notify_add(target_channel, symbol, direction, mode)
        self.notify.notify_active(target_channel, self.active_trades[trade_id])
        return "Trade Executed Successfully"

    def convert_to_live(self, trade_id):
        t = self.active_trades.get(trade_id)
        if t and t['mode'] == "PAPER":
            res = self.place_trade(t['symbol'], t['sec_id'], t['direction'], t['qty'], "VIP Channel", 20, "LIVE")
            t['status'] = "CONVERTED_TO_LIVE"
            self.save_trades()
            return f"Converted: {res}"
        return "Invalid Trade"

    def run_loop(self):
        while not self.stop_event.is_set():
            # (Time Strategy Logic - kept concise for brevity)
            # ...
            # (Risk Manager Logic)
            for tid, t in list(self.active_trades.items()):
                if t['status'] != "ACTIVE": continue

                ltp = self.get_latest_price(t['sec_id'], self.dhan.NSE_FNO)
                if ltp == 0: ltp = t['entry_price'] # Mock if fetch fails

                # Max High
                if t['direction'] == "BUY":
                    if ltp > t['max_price']: t['max_price'] = ltp
                else:
                    if ltp < t['max_price']: t['max_price'] = ltp

                # Target 1
                t1 = t['targets']['T1']
                if not t['t1_hit']:
                    if (t['direction'] == "BUY" and ltp >= t1) or (t['direction'] == "SELL" and ltp <= t1):
                        t['t1_hit'] = True
                        t['sl_price'] = t['entry_price']
                        self.notify.notify_update(t['channel'], t['symbol'], "Target 1 Hit! SL Moved to Cost.")

                # Exits
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
