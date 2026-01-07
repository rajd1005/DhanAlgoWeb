import os
from flask import Flask, render_template, request, redirect, url_for, flash
from config_manager import ConfigManager
from notifications import TelegramBot
from trading_engine import TradingEngine

app = Flask(__name__)
# SECURITY NOTE: In production, it's better to hide this key, 
# but for your private bot, this is fine.
app.secret_key = 'super_secret_key_change_this'

# --- INITIALIZATION ---
# 1. Load Config
config = ConfigManager()

# 2. Setup Telegram
bot = TelegramBot(config)

# 3. Start Trading Engine
# Note: We don't pass 'filename' here because TradingEngine uses its default
engine = TradingEngine(config, bot)

# --- ROUTES ---

@app.route('/')
def index():
    """Main Dashboard showing Active Trades."""
    return render_template('index.html', 
                           trades=engine.active_trades, 
                           mode=config.config['trading_mode'])

@app.route('/trade', methods=['POST'])
def trade():
    """Handles Manual Trade Placement from UI."""
    symbol = request.form.get('symbol')
    direction = request.form.get('direction')
    
    # Safety check for quantity
    try:
        qty = int(request.form.get('quantity'))
    except ValueError:
        qty = 0
        
    channel = request.form.get('channel')
    
    # Use current system mode (Paper/Live)
    current_mode = config.config['trading_mode']
    
    if qty > 0 and symbol:
        msg = engine.place_trade(symbol, direction, qty, channel, current_mode)
        flash(msg)
    else:
        flash("❌ Error: Invalid Quantity or Symbol")
        
    return redirect(url_for('index'))

@app.route('/convert/<trade_id>')
def convert(trade_id):
    """Button Action: Convert Paper Trade to Live."""
    msg = engine.convert_to_live(trade_id)
    flash(msg)
    return redirect(url_for('index'))

@app.route('/settings', methods=['GET', 'POST'])
def settings():
    """Settings Page to Edit Credentials."""
    if request.method == 'POST':
        # Update Config Object
        c = config.config
        c['dhan_creds']['client_id'] = request.form.get('client_id')
        c['dhan_creds']['access_token'] = request.form.get('access_token')
        c['telegram']['bot_token'] = request.form.get('bot_token')
        c['trading_mode'] = request.form.get('mode')
        
        # Save to JSON
        config.save_config()
        
        # Try to Reconnect API with new keys
        if engine.connect_api():
            flash("✅ Settings Saved & API Connected!")
        else:
            flash("⚠️ Settings Saved but API Connection Failed. Check Tokens.")
            
        return redirect(url_for('settings'))
        
    return render_template('settings.html', config=config.config)

# --- SERVER EXECUTION (RAILWAY FIX) ---
if __name__ == '__main__':
    # RAILWAY REQUIREMENT:
    # 1. Get the PORT environment variable (Railway assigns this dynamically)
    # 2. Listen on '0.0.0.0' (Public Interface), NOT '127.0.0.1'
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
