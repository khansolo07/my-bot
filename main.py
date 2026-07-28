from fastapi import FastAPI, HTTPException, Request
import ccxt
import uvicorn
import os
import json

app = FastAPI(title="TradingView to MEXC Futures")

MEXC_API_KEY = os.getenv("MEXC_API_KEY")
MEXC_SECRET_KEY = os.getenv("MEXC_SECRET_KEY")
SECRET_PASSPHRASE = os.getenv("WEBHOOK_PASSPHRASE", "MySuperSecretKey123") 

FUTURES_LEVERAGE = 10 

@app.get("/")
def home():
    return {"status": "MEXC Bridge is running!"}

@app.post("/webhook")
async def receive_webhook(request: Request):
    try:
        body_bytes = await request.body()
        if not body_bytes:
            raise HTTPException(status_code=400, detail="Empty payload")
        
        data = json.loads(body_bytes.decode('utf-8'))
        
        if SECRET_PASSPHRASE and data.get("passphrase") != SECRET_PASSPHRASE:
            raise HTTPException(status_code=403, detail="Forbidden")

        action = data.get("action")              # "buy" или "sell"
        raw_symbol = data.get("symbol", "").replace(".P", "").replace("USDT", "") # BTCUSDT -> BTC
        amount = float(data.get("amount", 0.0))

        if not action or not raw_symbol or amount <= 0.0:
            raise HTTPException(status_code=400, detail="Missing params")

        # Настройка фьючерсов MEXC
        exchange = ccxt.mexc({
            'apiKey': MEXC_API_KEY,
            'secret': MEXC_SECRET_KEY,
            'options': {'defaultType': 'swap'},
            'enableRateLimit': True,
        })
        
        symbol_futures = f"{raw_symbol}/USDT:USDT"

        try:
            exchange.set_leverage(FUTURES_LEVERAGE, symbol_futures)
        except Exception as e:
            pass

        if action == "buy":
            order = exchange.create_market_buy_order(symbol_futures, amount)
        elif action == "sell":
            order = exchange.create_market_sell_order(symbol_futures, amount)
        else:
            raise HTTPException(status_code=400, detail="Invalid action")

        return {"status": "success", "order_id": order['id']}

    except Exception as e:
        print(f"Error: {str(e)}")
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

