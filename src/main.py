import eventlet
eventlet.monkey_patch()

import asyncio
import threading
import logging
import os
from datetime import datetime
from pathlib import Path

from flask import Flask, send_from_directory
from flask_socketio import SocketIO, emit
from flask_cors import CORS

from signals import SignalGenerator
from fetch import BinanceWebSocketClient

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

app = Flask(__name__, static_folder='.')

app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'wickr-trading-dashboard-secret-key-change-in-production')
app.config['JSON_SORT_KEYS'] = False
app.config['PROPAGATE_EXCEPTIONS'] = True

CORS(app)
socketio = SocketIO(
    app, 
    cors_allowed_origins="*", 
    async_mode='eventlet', 
    logger=False,
    engineio_logger=False,
    ping_timeout=60,
    ping_interval=25
)

class AppState:
    def __init__(self):
        self.current_signal = "NEUTRAL"
        self.signal_data = None
        self.symbol = "BTCUSDT"
        self.binance_client = None
        self.connected_clients = 0
        
    def broadcast(self, event, data):
        socketio.emit(event, data)

state = AppState()

config_path = Path(__file__).parent.parent / "config.json"
signal_generator = SignalGenerator(config_path=str(config_path))

binance_client = BinanceWebSocketClient(
    symbol=state.symbol,
    interval="1s",
    buffer_size=35
)
state.binance_client = binance_client

async def on_price_update(price, timestamp, is_closed):
    try:
        state.broadcast('price_update', {
            'price': price,
            'timestamp': timestamp.isoformat(),
            'is_closed': is_closed
        })
    except Exception as e:
        logger.error(f"Error broadcasting price update: {e}")

async def on_candle_closed(candle, buffer):
    try:
        if len(buffer) >= 35:
            await check_for_signals()
    except Exception as e:
        logger.error(f"Error in candle closed handler: {e}")

async def check_for_signals():
    try:
        df = binance_client.get_buffer_as_dataframe()
        if not df.empty and len(df) >= 35:
            signals = signal_generator.generate_signals(df, symbol=state.symbol)
            if signals:
                latest_signal = signals[-1]
                signal_type = latest_signal.get('signal', 'NEUTRAL')
                if signal_type != state.current_signal:
                    logger.info(f"Signal changed: {state.current_signal} -> {signal_type}")
                    state.current_signal = signal_type
                    state.signal_data = latest_signal
                    state.broadcast('signal', {
                        'signal': signal_type,
                        'data': latest_signal
                    })
    except Exception as e:
        logger.error(f"Error checking for signals: {e}")

@app.route('/')
def index():
    try:
        html_file = Path(__file__).parent / "index.html"
        if html_file.exists():
            return html_file.read_text()
        else:
            logger.error("Dashboard HTML file not found")
            return "<h1>Dashboard HTML file not found</h1>", 404
    except Exception as e:
        logger.error(f"Error serving index: {e}")
        return "<h1>Internal Server Error</h1>", 500

@app.route('/style.css')
def serve_css():
    try:
        return send_from_directory(Path(__file__).parent, 'style.css')
    except Exception as e:
        logger.error(f"Error serving CSS: {e}")
        return "/* CSS not found */", 404

@app.route('/health')
def health_check():
    """Health check endpoint for monitoring"""
    return {
        'status': 'healthy',
        'connected_clients': state.connected_clients,
        'current_signal': state.current_signal,
        'timestamp': datetime.now().isoformat()
    }, 200

@app.errorhandler(404)
def not_found(error):
    return {'error': 'Not found'}, 404

@app.errorhandler(500)
def internal_error(error):
    logger.error(f"Internal server error: {error}")
    return {'error': 'Internal server error'}, 500

@socketio.on('connect')
def handle_connect():
    try:
        state.connected_clients += 1
        logger.info(f"Client connected. Total clients: {state.connected_clients}")
        emit('connection_status', {'status': 'connected'})
        
        if state.signal_data:
            emit('signal', {
                'signal': state.current_signal,
                'data': state.signal_data
            })
        
        if binance_client.latest_price:
            emit('price_update', {
                'price': binance_client.latest_price,
                'timestamp': datetime.now().isoformat(),
                'is_closed': False
            })
    except Exception as e:
        logger.error(f"Error in connect handler: {e}")

@socketio.on('disconnect')
def handle_disconnect():
    try:
        state.connected_clients -= 1
        logger.info(f"Client disconnected. Total clients: {state.connected_clients}")
    except Exception as e:
        logger.error(f"Error in disconnect handler: {e}")

@socketio.on('ping')
def handle_ping():
    try:
        emit('pong', {'timestamp': datetime.now().isoformat()})
    except Exception as e:
        logger.error(f"Error in ping handler: {e}")

@socketio.on_error_default
def default_error_handler(e):
    logger.error(f"SocketIO error: {e}")
    return False

def run_binance_websocket():
    try:
        logger.info("Starting Binance WebSocket connection...")
        binance_client.register_callback('on_price_update', on_price_update)
        binance_client.register_callback('on_candle_closed', on_candle_closed)
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(binance_client.fetch_initial_candles())
            logger.info("Initial candles fetched successfully")
            loop.run_until_complete(binance_client.connect_and_stream())
        except Exception as e:
            logger.error(f"Error in WebSocket event loop: {e}")
        finally:
            loop.close()
    except Exception as e:
        logger.error(f"Fatal error in Binance WebSocket thread: {e}")

ws_thread = threading.Thread(target=run_binance_websocket, daemon=True)
ws_thread.start()
logger.info("Binance WebSocket thread started")

if __name__ == '__main__':
    import os
    
    app.config['DEBUG'] = False
    app.config['TESTING'] = False
    
    host = os.getenv('HOST', '0.0.0.0')
    port = int(os.getenv('PORT', 5000))
    
    print(f"Starting Wickr Trading Dashboard in PRODUCTION mode")
    print(f"Server: http://{host}:{port}")
    print(f"WebSocket: ws://{host}:{port}/socket.io/")
    
    socketio.run(
        app, 
        host=host, 
        port=port, 
        debug=False,
        use_reloader=False,
        log_output=True
    )
