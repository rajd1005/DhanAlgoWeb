import json
import os
from datetime import datetime

class ConfigManager:
    def __init__(self, filename="data/config.json"):
        self.filename = filename
        # Auto-create data folder
        folder = os.path.dirname(filename)
        if folder and not os.path.exists(folder):
            os.makedirs(folder)
            
        self.config = self.load_config()

    def load_config(self):
        default = {
            "dhan_creds": {
                "client_id": "",
                "access_token": ""
            },
            "telegram": {
                "bot_token": "",
                "channels": {
                    "Free Group": "", 
                    "VIP Channel": ""
                }
            },
            "trading_mode": "PAPER", # Options: PAPER, LIVE
            "daily_stats": {
                "date": str(datetime.now().date()), 
                "free_count": 0
            }
        }
        
        if os.path.exists(self.filename):
            try:
                with open(self.filename, 'r') as f:
                    saved = json.load(f)
                    # Merge defaults to ensure no missing keys
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
        """
        Restriction Logic: 
        If 'Free Group' is selected and 1 trade is already done today,
        Force switch to 'VIP Channel'.
        """
        today = str(datetime.now().date())
        stats = self.config['daily_stats']

        # Reset counters if new day
        if stats['date'] != today:
            stats['date'] = today
            stats['free_count'] = 0
            self.save_config()

        # Check Limit
        if requested_channel == "Free Group" and stats['free_count'] >= 1:
            return "VIP Channel", True # True indicates "Forced Switch"
            
        return requested_channel, False

    def increment_trade_count(self, channel):
        if channel == "Free Group":
            self.config['daily_stats']['free_count'] += 1
            self.save_config()
