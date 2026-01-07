import json
import os
from datetime import datetime
import pytz

class ConfigManager:
    def __init__(self, filename="data/config.json"):
        self.filename = filename
        # Ensure data directory exists
        folder = os.path.dirname(filename)
        if folder and not os.path.exists(folder):
            os.makedirs(folder)
            
        self.config = self.load_config()

    def load_config(self):
        default = {
            "dhan_creds": {"client_id": "", "access_token": ""},
            "telegram": {
                "bot_token": "",
                "channels": {
                    "Free Group": "-100xxxxxxx", 
                    "VIP Channel": "-100yyyyyyy"
                }
            },
            "trading_mode": "PAPER", # PAPER or LIVE
            "lot_sizes": {"NIFTY": 25, "BANKNIFTY": 15},
            "daily_stats": {"date": str(datetime.now().date()), "free_count": 0}
        }
        
        if os.path.exists(self.filename):
            try:
                with open(self.filename, 'r') as f:
                    saved = json.load(f)
                    # Merge defaults
                    for k, v in default.items():
                        if k not in saved: saved[k] = v
                    return saved
            except:
                pass
        return default

    def save_config(self):
        with open(self.filename, 'w') as f:
            json.dump(self.config, f, indent=4)

    def get_target_channel(self, requested_channel):
        """Restriction Logic: 1 Free trade/day -> Switch to VIP"""
        today = str(datetime.now().date())
        stats = self.config['daily_stats']

        if stats['date'] != today:
            stats['date'] = today
            stats['free_count'] = 0
            self.save_config()

        if requested_channel == "Free Group" and stats['free_count'] >= 1:
            return "VIP Channel", True # True = Forced Switch
            
        return requested_channel, False

    def increment_trade_count(self, channel):
        if channel == "Free Group":
            self.config['daily_stats']['free_count'] += 1
            self.save_config()
