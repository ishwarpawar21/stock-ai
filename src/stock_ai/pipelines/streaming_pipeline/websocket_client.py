import json
import logging
import websocket
from google.protobuf.json_format import MessageToDict
# from .market_data import MarketDataFeed_pb2

class UpstoxWebSocketClient:
    def __init__(self, access_token, client_id, instrument_keys):
        self.access_token = access_token
        self.client_id = client_id
        self.instrument_keys = instrument_keys
        self.ws = None
        self.is_streaming = False
    
    def on_message(self, message):
        """Handle incoming messages."""
        # logging.debug(f"Received message: {message}")
        # Deserialize the message if necessary
        # message = MessageToDict(MarketDataFeed_pb2.YourMessageType.FromString(message))
        print(message)  # Just for debugging purposes
        self.is_streaming = True  # Mark as streaming data

    def on_error(self, error):
        """Handle WebSocket errors."""
        logging.error(f"WebSocket error: {error}")

    def on_close(self):
        """Handle WebSocket closure."""
        # logging.debug("WebSocket connection closed.")

    def on_open(self):
        """Handle WebSocket connection opening."""
        # logging.debug("WebSocket connection opened.")
        # Subscribe to stock symbols
        self.ws.send(json.dumps({"type": "subscribe", "symbols": self.instrument_keys}))

    def run(self):
        """Connect to WebSocket and start the client."""
        self.ws = websocket.WebSocketApp(
            "wss://your-websocket-url",  # Replace with actual WebSocket URL
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )
        self.ws.on_open = self.on_open
        self.ws.run_forever()

    def is_data_streaming(self):
        """Check if data is streaming."""
        return self.is_streaming
