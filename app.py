from flask import Flask, render_template, request, redirect, url_for, flash
from config_manager import ConfigManager
from notifications import TelegramBot
from trading_engine import TradingEngine

app = Flask(__name__)
app.secret_key = 'super_secret_key'

# Initialize Components
config = ConfigManager()
bot = TelegramBot(config)
engine = TradingEngine(config, bot)

@app.route('/')
def index():
    # Show Dashboard
    return render_template('index.html', 
                           trades=engine.active_trades, 
                           mode=config.config['trading_mode'])

@app.route('/trade', methods=['POST'])
def trade():
    # Handle Manual Trade Form
    symbol = request.form.get('symbol')
    direction = request.form.get('direction')
    qty = int(request.form.get('quantity'))
    channel = request.form.get('channel')
    
    # Use current system mode (Paper/Live)
    current_mode = config.config['trading_mode']
    
    msg = engine.place_trade(symbol, direction, qty, channel, current_mode)
    flash(msg)
    return redirect(url_for('index'))

@app.route('/convert/<trade_id>')
def convert(trade_id):
    # Paper -> Live Button Logic
    msg = engine.convert_to_live(trade_id)
    flash(msg)
    return redirect(url_for('index'))

@app.route('/settings', methods=['GET', 'POST'])
def settings():
    if request.method == 'POST':
        # Update Config
        c = config.config
        c['dhan_creds']['client_id'] = request.form.get('client_id')
        c['dhan_creds']['access_token'] = request.form.get('access_token')
        c['telegram']['bot_token'] = request.form.get('bot_token')
        c['trading_mode'] = request.form.get('mode')
        config.save_config()
        
        # Reconnect API
        if engine.connect_api():
            flash("Settings Saved & API Connected!")
        else:
            flash("Settings Saved but API Connection Failed.")
        return redirect(url_for('settings'))
        
    return render_template('settings.html', config=config.config)

if __name__ == '__main__':
    app.run(debug=True, port=5000)
