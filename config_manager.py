import json
import os
from datetime import datetime

class ConfigManager:
    def __init__(self, filename="config.json"):
        self.filename = filename
        self.config = self.load_config()

    def load_config(self):
        default = {
            "dhan_creds": {"client_id": "", "access_token": ""},
            "telegram": {
                "bot_token": "",
                "channels": {
                    "Free Group": "-1001111111", 
                    "VIP Channel": "-1002222222"
                }
            },
            "trading_mode": "PAPER", # PAPER or LIVE
            "daily_stats": {"date": str(datetime.now().date()), "free_count": 0}
        }
        
        if os.path.exists(self.filename):
            with open(self.filename, 'r') as f:
                saved = json.load(f)
                # Merge with default to ensure all keys exist
                for key in default:
                    if key not in saved: saved[key] = default[key]
                return saved
        return default

    def save_config(self):
        with open(self.filename, 'w') as f:
            json.dump(self.config, f, indent=4)

    def get_target_channel(self, requested_channel):
        """Restrictions: 1 Free trade per day, then force VIP."""
        today = str(datetime.now().date())
        stats = self.config['daily_stats']

        if stats['date'] != today:
            stats['date'] = today
            stats['free_count'] = 0
            self.save_config()

        if requested_channel == "Free Group" and stats['free_count'] >= 1:
            return "VIP Channel", True # True indicates "Forced Switch"
            
        return requested_channel, False

    def increment_trade_count(self, channel):
        if channel == "Free Group":
            self.config['daily_stats']['free_count'] += 1
            self.save_config()
