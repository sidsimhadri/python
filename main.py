
from dotenv import load_dotenv
import os
load_dotenv()
import asyncio
import websockets
import json
import traceback
import msgpack

API_KEY = os.getenv('ALPACA_KEY')
API_SECRET = os.getenv('ALPACA_SECRET')
SOCKET_URI = "wss://stream.data.alpaca.markets/v1beta1"


def handle_exception(e):
    if isinstance(e, websockets.exceptions.ConnectionClosed):
        print(f"Connection closed: {e}")
    elif isinstance(e, json.JSONDecodeError):
        print(f"Error decoding JSON: {e}")
    else:
        print(f"Unexpected error: {e}")
    traceback.print_exc()
    
    
async def authenticate(ws):
    try:
        auth_data = {
            "action": "auth",
            "key": API_KEY,
            "secret": API_SECRET
        }
        await ws.send(json.dumps(auth_data))
        response = await ws.recv()
        return response
    except Exception as e:
        handle_exception(e)
        
async def subscribe(ws, ticker):
    try:
        subscribe_data = {
            "action": "subscribe",
            "symbols": [ticker]
        }
        await ws.send(json.dumps(subscribe_data))
        response = await ws.recv()
        return response
    except Exception as e:
        handle_exception(e)
        
        
async def stream_data(uri, ticker):
    try:
        async with websockets.connect(uri) as ws:
            auth_response = await authenticate(ws)
            print(msgpack.unpackb(auth_response))
            if auth_response[0]['msg'] == 'connected':
                await subscribe(ws, ticker)
                while True:
                    message = await ws.recv()
                    print(message)
    except Exception as e:
        handle_exception(e)

async def main(ticker):
    try:
        await asyncio.gather(
            stream_data(f'{SOCKET_URI}/news', ticker),
            stream_data(f'{SOCKET_URI}/indicative', ticker)
        )
    except Exception as e:
        handle_exception(e)

asyncio.run(main("AAPL"))