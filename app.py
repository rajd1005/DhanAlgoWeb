import os
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from config_manager import ConfigManager
from notifications import TelegramBot
from symbol_manager import SymbolManager
from trading_engine import TradingEngine

app = Flask(__name__)
app.secret_key = 'algo_secure_key'

# --- INIT (Safe Mode) ---
cfg = ConfigManager()
bot = TelegramBot(cfg)
sym_mgr = SymbolManager() # Starts loading in background
engine = TradingEngine(cfg, bot, sym_mgr)

@app.context_processor
def inject_globals():
    return dict(
        channels=cfg.config['telegram']['channels'].keys(),
        mode=cfg.config['trading_mode'],
        # Pass system status to UI
        system_ready=sym_mgr.is_ready 
    )

@app.route('/')
def index():
    return render_template('index.html', trades=engine.active_trades)

@app.route('/api/status')
def api_status():
    return jsonify({
        "connected": engine.is_connected,
        "mode": cfg.config['trading_mode'],
        "data_ready": sym_mgr.is_ready # Tell UI if data is loaded
    })

@app.route('/api/search')
def search():
    # If still loading, return empty or specific message
    if not sym_mgr.is_ready:
        return jsonify([{"display": "⚠️ System Loading Data...", "symbol": "", "id": ""}])
        
    q = request.args.get('q', '')
    if len(q) < 2: return jsonify([])
    return jsonify(sym_mgr.search(q))

# ... (Rest of the app.py routes remain exactly the same as before) ...
# ... (get_ltp, get_options, get_option_ltp, trade, convert, sync, settings) ...

# Keep the Server Execution Block
if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
