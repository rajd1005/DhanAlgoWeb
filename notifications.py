import requests
import json
from datetime import datetime

class TelegramBot:
    def __init__(self, config_manager):
        self.cfg = config_manager

    def send_msg(self, channel_name, text, buttons=None):
        conf = self.cfg.config
        token = conf['telegram']['bot_token']
        chat_id = conf['telegram']['channels'].get(channel_name)
        
        if not token or not chat_id: return

        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True
        }
        if buttons:
            payload["reply_markup"] = json.dumps(buttons)

        try:
            requests.post(f"https://api.telegram.org/bot{token}/sendMessage", json=payload, timeout=3)
        except Exception as e:
            print(f"Telegram Fail: {e}")

    def notify_add(self, channel, symbol, direction, mode):
        """Trade Added Notification"""
        msg = f"🛠 *Trade Added ({mode})*\nSymbol: {symbol}\nSide: {direction}\nWaiting for Entry..."
        self.send_msg(channel, msg)

    def notify_active(self, channel, trade_data):
        """Detailed Active Trade Alert with 5 Targets"""
        t = trade_data
        targets = "\n".join([f"🎯 {k}: {v:.2f}" for k, v in t['targets'].items()])
        msg = (
            f"🚀 *TRADE ACTIVE*\n"
            f"Symbol: {t['symbol']} @ {t['entry_price']}\n"
            f"🛡 SL: {t['sl_price']:.2f}\n"
            f"------------------\n"
            f"{targets}\n"
            f"------------------"
        )
        self.send_msg(channel, msg)

    def notify_exit(self, channel, trade_data, reason, exit_price):
        """Exit Alert with Made High & PnL"""
        t = trade_data
        pnl = (exit_price - t['entry_price']) * t['qty']
        if t['direction'] == "SELL": pnl *= -1
        
        icon = "✅" if pnl >= 0 else "❌"
        msg = (
            f"{icon} *TRADE CLOSED*\n"
            f"Symbol: {t['symbol']}\n"
            f"Reason: {reason}\n"
            f"Exit: {exit_price} | P&L: {pnl:.2f}\n"
            f"📈 *Made High:* {t['max_price']} (Highest since entry)"
        )
        self.send_msg(channel, msg)

    def notify_update(self, channel, symbol, msg):
        self.send_msg(channel, f"🔔 *{symbol} Update:* {msg}")
