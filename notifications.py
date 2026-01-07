import requests
import json

class TelegramBot:
    def __init__(self, config_manager):
        self.cfg = config_manager

    def send_alert(self, channel_name, title, message, trade_id=None):
        """Sends formatted alerts to the specific channel."""
        conf = self.cfg.config
        bot_token = conf['telegram']['bot_token']
        chat_id = conf['telegram']['channels'].get(channel_name)
        
        if not bot_token or not chat_id:
            return "Telegram Not Configured"

        text = f"🔔 *{title}*\n{message}"
        
        # Add 'Execute Live' Button if it's a Paper Trade
        reply_markup = None
        if trade_id:
            reply_markup = {
                "inline_keyboard": [[
                    {"text": "⚡ EXECUTE LIVE", "callback_data": f"live_{trade_id}"}
                ]]
            }

        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {
            "chat_id": chat_id, 
            "text": text, 
            "parse_mode": "Markdown",
        }
        if reply_markup:
            payload["reply_markup"] = json.dumps(reply_markup)

        try:
            requests.post(url, json=payload, timeout=2)
        except Exception as e:
            print(f"Telegram Error: {e}")
