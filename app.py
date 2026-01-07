import os
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from config_manager import ConfigManager
from notifications import TelegramBot
from symbol_manager import SymbolManager
from trading_engine import TradingEngine

app = Flask(__name__)
app.secret_key = 'algo_secure_key_prod'

cfg = ConfigManager()
bot = TelegramBot(cfg)
sym_mgr = SymbolManager()
engine = TradingEngine(cfg, bot, sym_mgr)

@app.context_processor
def inject_globals():
    return dict(
        channels=cfg.config['telegram']['channels'].keys(),
        mode=cfg.config['trading_mode'],
        system_ready=sym_mgr.is_ready
    )

@app.route('/')
def index():
    return render_template('index.html', trades=engine.active_trades)

@app.route('/settings', methods=['GET', 'POST'])
def settings():
    if request.method == 'POST':
        c = cfg.config
        c['dhan_creds']['client_id'] = request.form.get('client_id')
        c['dhan_creds']['access_token'] = request.form.get('access_token')
        c['telegram']['bot_token'] = request.form.get('bot_token')
        c['trading_mode'] = request.form.get('mode')
        cfg.save_config()
        if engine.connect_api(): flash("✅ Saved & Connected!")
        else: flash("⚠️ Saved, but API Failed.")
        return redirect(url_for('settings'))
    return render_template('settings.html', config=cfg.config)

@app.route('/trade', methods=['POST'])
def trade():
    sec_id = request.form.get('final_security_id')
    symbol = request.form.get('final_symbol_name')
    direction = request.form.get('direction')
    chan = request.form.get('channel')
    try:
        qty = int(request.form.get('qty', 0))
        sl = float(request.form.get('sl', 0))
    except: qty=0; sl=0

    current_mode = cfg.config['trading_mode']
    if qty > 0 and sec_id:
        msg = engine.place_trade(symbol, sec_id, direction, qty, chan, sl, current_mode)
        flash(msg)
    else:
        flash("❌ Error: Invalid Selection")
    return redirect(url_for('index'))

@app.route('/convert/<tid>')
def convert(tid):
    msg = engine.convert_to_live(tid)
    flash(msg)
    return redirect(url_for('index'))

@app.route('/sync-scrips')
def sync_scrips():
    if sym_mgr.download_scrips(): flash("✅ Updated!")
    else: flash("❌ Update Failed")
    return redirect(url_for('settings'))

# --- API ROUTES ---

@app.route('/api/status')
def api_status():
    return jsonify({
        "connected": engine.is_connected,
        "mode": cfg.config['trading_mode'],
        "data_ready": sym_mgr.is_ready
    })

@app.route('/api/search')
def search():
    if not sym_mgr.is_ready:
        return jsonify([{"display": "⏳ Loading Data...", "symbol": "", "id": ""}])
    q = request.args.get('q', '')
    if len(q) < 2: return jsonify([])
    return jsonify(sym_mgr.search(q))

@app.route('/api/ltp')
def get_ltp():
    """Fetch Live Price with Segment Support"""
    sec_id = request.args.get('id')
    segment = request.args.get('segment') # <--- NEW PARAM
    
    if not sec_id: return jsonify({"ltp": 0.0})
    
    # Pass segment explicitly to engine
    price = engine.get_latest_price(sec_id, segment)
    return jsonify({"ltp": price})

@app.route('/api/options')
def get_options():
    symbol = request.args.get('symbol')
    try: spot_price = float(request.args.get('spot', 0))
    except: return jsonify([])
    direction = request.args.get('direction', 'BUY')
    
    if not symbol or spot_price == 0: return jsonify([])
    
    strikes, atm = engine.get_option_chain_data(symbol, spot_price)
    option_list = []
    opt_type = "CE" if direction == "BUY" else "PE"
    
    for stk in strikes:
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
    if not sec_id: return jsonify({"ltp": 0.0})
    # Options are always NSE_FNO (usually)
    price = engine.get_latest_price(sec_id, 'NSE_FNO')
    return jsonify({"ltp": price})

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
