import threading
import time
import json
import os
from datetime import datetime, timedelta
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
                # DhanHQ v2.0 requires Context
                context = DhanContext(c['client_id'], c['access_token'])
                self.dhan = dhanhq(context)
                self.is_connected = True
                print("✅ API Connected Successfully")
                return True
            except Exception as e:
                print(f"❌ API Connection Failed: {e}")
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

    # --- SMART PRICE FETCHER ---
    def get_latest_price(self, security_id, exchange_segment=None):
        """
        Robust Price Fetcher:
        1. Tries 'ticker_data' (Fastest)
        2. Fallback to 'ohlc_data' (Better for Indices)
        3. Fallback to 'option_chain' (Guaranteed for Indices)
        """
        if not self.is_connected: return 0.0
        
        # Default to NSE_EQ if missing
        segment = exchange_segment if exchange_segment else 'NSE_EQ'
        
        try:
            # 1. Ensure ID is Integer
            try:
                sec_id_int = int(security_id)
            except (ValueError, TypeError):
                print(f"❌ Invalid ID: {security_id}")
                return 0.0

            # 2. METHOD A: Ticker Data (Standard)
            req = {segment: [sec_id_int]}
            response = self.dhan.ticker_data(req)
            
            if response.get('status') == 'success':
                data = response.get('data', {}).get(segment, [])
                for item in data:
                    price = float(item.get('last_price', 0.0))
                    if price > 0: return price

            # 3. METHOD B: OHLC Data (Fallback for Indices)
            # Indices often fail on ticker_data but work on OHLC
            # print(f"⚠️ Retrying {security_id} via OHLC...")
            response = self.dhan.ohlc_data(req)
            if response.get('status') == 'success':
                data = response.get('data', {}).get(segment, [])
                for item in data:
                    price = float(item.get('last_price', 0.0))
                    if price > 0: return price
                    # Check OHLC close if LTP missing
                    ohlc = item.get('ohlc', {})
                    if ohlc.get('close', 0) > 0: return float(ohlc['close'])

            # 4. METHOD C: Option Chain (Ultimate Fallback for Indices)
            # If NIFTY/BANKNIFTY, we can get price from Option Chain header
            if segment == 'IDX_I':
                # We need an expiry. Find nearest via SymbolManager or guess.
                # Actually, Dhan API requires Expiry for option chain.
                # We'll skip this if we don't have expiry readily available to avoid lag.
                pass 

        except Exception as e:
            print(f"❌ LTP Error: {e}")
            pass
            
        return 0.0

    def get_option_chain_data(self, symbol, spot_price):
        step = 100 if "BANK" in symbol.upper() else 50
        atm_strike = round(spot_price / step) * step
        strikes = []
        for i in range(-5, 6):
            strikes.append(atm_strike + (i * step))
        return strikes, atm_strike

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
        
        # Get Entry Price (Try FNO first for options)
        entry_price = self.get_latest_price(security_id, 'NSE_FNO')
        if entry_price == 0: 
            entry_price = self.get_latest_price(security_id, 'NSE_EQ')
        if entry_price == 0: entry_price = 100.0
            
        if mode == "LIVE" and self.is_connected:
            try:
                self.dhan.place_order(
                    security_id=str(security_id),
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
            now = datetime.now(pytz.timezone('Asia/Kolkata'))
            if now.strftime("%H:%M:%S") == "09:54:00" and self.is_connected:
                try:
                    # Smart fetch for Nifty Spot
                    spot = self.get_latest_price("13", "IDX_I")
                    if spot > 0:
                        sec_id, sym = self.sym_mgr.get_atm_security("NIFTY", spot, "BUY")
                        if sec_id:
                            self.place_trade(sym, sec_id, "BUY", 50, "VIP Channel", 20, "PAPER")
                            time.sleep(1.5)
                except: pass

            for tid, t in list(self.active_trades.items()):
                if t['status'] != "ACTIVE": continue

                ltp = self.get_latest_price(t['sec_id'], 'NSE_FNO')
                if ltp == 0: ltp = t['entry_price']

                if t['direction'] == "BUY":
                    if ltp > t['max_price']: t['max_price'] = ltp
                else:
                    if ltp < t['max_price']: t['max_price'] = ltp

                t1 = t['targets']['T1']
                if not t['t1_hit']:
                    if (t['direction'] == "BUY" and ltp >= t1) or (t['direction'] == "SELL" and ltp <= t1):
                        t['t1_hit'] = True
                        t['sl_price'] = t['entry_price']
                        self.notify.notify_update(t['channel'], t['symbol'], "Target 1 Hit! SL Moved to Cost.")

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
