import os
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from config_manager import ConfigManager
from notifications import TelegramBot
from symbol_manager import SymbolManager
from trading_engine import TradingEngine

app = Flask(__name__)
app.secret_key = 'algo_secure_key'

cfg = ConfigManager()
bot = TelegramBot(cfg)
sym_mgr = SymbolManager()
engine = TradingEngine(cfg, bot, sym_mgr)

@app.context_processor
def inject_globals():
    return dict(
        channels=cfg.config['telegram']['channels'].keys(),
        mode=cfg.config['trading_mode']
    )

@app.route('/')
def index():
    return render_template('index.html', trades=engine.active_trades)

# --- NEW STATUS ROUTE ---
@app.route('/api/status')
def api_status():
    """Checks if Dhan API is connected."""
    return jsonify({
        "connected": engine.is_connected,
        "mode": cfg.config['trading_mode']
    })
# ------------------------

@app.route('/api/search')
def search():
    q = request.args.get('q', '')
    if len(q) < 2: return jsonify([])
    return jsonify(sym_mgr.search(q))

@app.route('/api/ltp')
def get_ltp():
    sec_id = request.args.get('id')
    price = engine.get_latest_price(sec_id) # Uses the smart fetcher
    return jsonify({"ltp": price})

@app.route('/api/options')
def get_options():
    symbol = request.args.get('symbol')
    spot_price = float(request.args.get('spot', 0))
    direction = request.args.get('direction', 'BUY')
    
    strikes, atm = engine.get_option_chain_data(symbol, spot_price)
    
    option_list = []
    opt_type = "CE" if direction == "BUY" else "PE"
    
    for stk in strikes:
        # Use SymbolManager to find the specific Option Security ID
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
    sec_id = request.args.get('id')
    # Try fetching option price
    price = engine.get_latest_price(sec_id, engine.dhan.NSE_FNO)
    return jsonify({"ltp": price})

@app.route('/trade', methods=['POST'])
def trade():
    sec_id = request.form.get('final_security_id') 
    symbol = request.form.get('final_symbol_name')
    direction = request.form.get('direction')
    qty = int(request.form.get('qty', 0))
    sl = float(request.form.get('sl', 0))
    chan = request.form.get('channel')
    current_mode = cfg.config['trading_mode']
    
    if qty > 0 and sec_id:
        msg = engine.place_trade(symbol, sec_id, direction, qty, chan, sl, current_mode)
        flash(msg)
    else:
        flash("❌ Error: Invalid Option Selected")
        
    return redirect(url_for('index'))

@app.route('/convert/<tid>')
def convert(tid):
    msg = engine.convert_to_live(tid)
    flash(msg)
    return redirect(url_for('index'))

@app.route('/sync-scrips')
def sync():
    if sym_mgr.download_scrips(): flash("✅ Scrip Master Updated!")
    else: flash("❌ Update Failed")
    return redirect(url_for('settings'))

@app.route('/settings', methods=['GET', 'POST'])
def settings():
    if request.method == 'POST':
        c = cfg.config
        c['dhan_creds']['client_id'] = request.form.get('client_id')
        c['dhan_creds']['access_token'] = request.form.get('access_token')
        c['telegram']['bot_token'] = request.form.get('bot_token')
        c['trading_mode'] = request.form.get('mode')
        cfg.save_config()
        if engine.connect_api(): flash("✅ Connected & Saved!")
        else: flash("⚠️ Saved, but API Connection Failed")
        return redirect(url_for('settings'))
    return render_template('settings.html', config=cfg.config)

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
