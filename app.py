import os
from flask import Flask, render_template, request, redirect, url_for, flash
from config_manager import ConfigManager
from notifications import TelegramBot
from trading_engine import TradingEngine

app = Flask(__name__)
app.secret_key = 'algo_secure_key'

cfg = ConfigManager()
bot = TelegramBot(cfg)
engine = TradingEngine(cfg, bot)

@app.context_processor
def inject_config():
    return dict(channels=cfg.config['telegram']['channels'].keys())

@app.route('/')
def index():
    return render_template('index.html', trades=engine.active_trades, mode=cfg.config['trading_mode'])

@app.route('/trade', methods=['POST'])
def trade():
    sym = request.form.get('symbol')
    direction = request.form.get('direction')
    qty = int(request.form.get('qty', 0))
    sl = float(request.form.get('sl', 0))
    chan = request.form.get('channel')
    mode = cfg.config['trading_mode']
    
    if qty > 0:
        msg = engine.place_trade(sym, direction, qty, chan, sl, mode=mode)
        flash(msg)
    return redirect(url_for('index'))

@app.route('/convert/<tid>')
def convert(tid):
    msg = engine.convert_to_live(tid)
    flash(msg)
    return redirect(url_for('index'))

@app.route('/settings', methods=['GET', 'POST'])
def settings():
    if request.method == 'POST':
        c = cfg.config
        c['dhan_creds']['client_id'] = request.form.get('client_id')
        c['dhan_creds']['access_token'] = request.form.get('access_token')
        c['telegram']['bot_token'] = request.form.get('bot_token')
        c['trading_mode'] = request.form.get('mode')
        cfg.save_config()
        if engine.connect_api(): flash("Connected & Saved!")
        else: flash("Saved, but API Connection Failed")
        return redirect(url_for('settings'))
    return render_template('settings.html', config=cfg.config)

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
