import os
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from config_manager import ConfigManager
from notifications import TelegramBot
from symbol_manager import SymbolManager
from trading_engine import TradingEngine

app = Flask(__name__)
# SECURITY NOTE: In a real production app, use a random environment variable for this key
app.secret_key = 'algo_secure_key_change_this_in_production'

# --- INITIALIZATION ---
# 1. Load Configuration (Creates data folder if missing)
cfg = ConfigManager()

# 2. Initialize Telegram Bot
bot = TelegramBot(cfg)

# 3. Initialize Symbol Manager (Starts background loading immediately)
sym_mgr = SymbolManager()

# 4. Initialize Trading Engine (Starts background monitoring thread)
# Note: Connects to Dhan API using credentials from Config
engine = TradingEngine(cfg, bot, sym_mgr)

# --- GLOBAL CONTEXT ---
@app.context_processor
def inject_globals():
    """Injects variables available to all HTML templates."""
    return dict(
        channels=cfg.config['telegram']['channels'].keys(),
        mode=cfg.config['trading_mode'],
        system_ready=sym_mgr.is_ready # Used to show loading state in UI
    )

# --- WEB ROUTES ---

@app.route('/')
def index():
    """Main Dashboard showing Active Trades."""
    return render_template('index.html', trades=engine.active_trades)

@app.route('/settings', methods=['GET', 'POST'])
def settings():
    """Settings Page for API Keys and Config."""
    if request.method == 'POST':
        c = cfg.config
        
        # Update Credentials
        c['dhan_creds']['client_id'] = request.form.get('client_id')
        c['dhan_creds']['access_token'] = request.form.get('access_token')
        
        # Update Telegram
        c['telegram']['bot_token'] = request.form.get('bot_token')
        
        # Update Mode
        c['trading_mode'] = request.form.get('mode')
        
        cfg.save_config()
        
        # Try Reconnect
        if engine.connect_api():
            flash("✅ Settings Saved & API Connected!")
        else:
            flash("⚠️ Settings Saved, but API Connection Failed. Check Token.")
            
        return redirect(url_for('settings'))
        
    return render_template('settings.html', config=cfg.config)

@app.route('/trade', methods=['POST'])
def trade():
    """Handle Manual Trade Execution from Dashboard."""
    # Inputs
    sec_id = request.form.get('final_security_id')
    symbol = request.form.get('final_symbol_name')
    direction = request.form.get('direction')
    chan = request.form.get('channel')
    
    try:
        qty = int(request.form.get('qty', 0))
        sl = float(request.form.get('sl', 0))
    except ValueError:
        qty = 0
        sl = 0

    current_mode = cfg.config['trading_mode']
    
    # Validation
    if qty > 0 and sec_id:
        msg = engine.place_trade(symbol, sec_id, direction, qty, chan, sl, current_mode)
        flash(msg)
    else:
        flash("❌ Error: Invalid Option/Symbol or Quantity.")
        
    return redirect(url_for('index'))

@app.route('/convert/<tid>')
def convert(tid):
    """Button Action: Convert Paper Trade to Live."""
    msg = engine.convert_to_live(tid)
    flash(msg)
    return redirect(url_for('index'))

@app.route('/sync-scrips')
def sync_scrips():
    """Manual Button to Re-Download Scrip Master."""
    if sym_mgr.download_scrips():
        flash("✅ Scrip Master Updated Successfully!")
    else:
        flash("❌ Update Failed. Check Logs.")
    return redirect(url_for('settings'))

# --- API ROUTES (AJAX) ---

@app.route('/api/status')
def api_status():
    """Returns System Health for Dashboard Badge."""
    return jsonify({
        "connected": engine.is_connected,
        "mode": cfg.config['trading_mode'],
        "data_ready": sym_mgr.is_ready
    })

@app.route('/api/search')
def search():
    """Symbol Auto-Complete API."""
    if not sym_mgr.is_ready:
        return jsonify([{"display": "⏳ System Loading Data...", "symbol": "", "id": ""}])
        
    q = request.args.get('q', '')
    if len(q) < 2: return jsonify([])
    
    return jsonify(sym_mgr.search(q))

@app.route('/api/ltp')
def get_ltp():
    """Fetch Live Price for a Security ID."""
    sec_id = request.args.get('id')
    if not sec_id: return jsonify({"ltp": 0.0})
    
    price = engine.get_latest_price(sec_id)
    return jsonify({"ltp": price})

@app.route('/api/options')
def get_options():
    """Fetch Option Chain (ATM +/- 5) for Dropdown."""
    symbol = request.args.get('symbol')
    try:
        spot_price = float(request.args.get('spot', 0))
    except:
        return jsonify([])

    direction = request.args.get('direction', 'BUY')
    
    if not symbol or spot_price == 0:
        return jsonify([])
    
    # Get calculated strikes
    strikes, atm = engine.get_option_chain_data(symbol, spot_price)
    
    option_list = []
    opt_type = "CE" if direction == "BUY" else "PE"
    
    for stk in strikes:
        # Resolve Security ID for each strike
        sec_id, name = sym_mgr.get_atm_security(symbol, stk, direction)
        
        option_list.append({
            "strike": stk,
            "is_atm": (stk == atm),
            "id": sec_id if sec_id else "",
            "name": name if name else f"{symbol} {stk} {opt_type}"
        })
        
    return jsonify(option_list)

@app.route('/api/option-ltp')
def get_option_ltp():
    """Fetch Live Price specifically for Options."""
    sec_id = request.args.get('id')
    if not sec_id: return jsonify({"ltp": 0.0})
    
    # Force FNO segment search
    price = engine.get_latest_price(sec_id, 'NSE_FNO')
    return jsonify({"ltp": price})

# --- SERVER STARTUP ---
if __name__ == '__main__':
    # Railway Configuration
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
