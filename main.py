from fastapi import FastAPI, HTTPException, Request, BackgroundTasks
import ccxt.async_support as ccxt
import uvicorn
import os
import json
import asyncio

app = FastAPI(title="Safe Single-Position MEXC Bridge")

MEXC_API_KEY = os.getenv("MEXC_API_KEY")
MEXC_SECRET_KEY = os.getenv("MEXC_SECRET_KEY")
SECRET_PASSPHRASE = os.getenv("WEBHOOK_PASSPHRASE", "MYSUPERSECRETKEY123") 

# Настройки Риска и Тейка
STOP_LOSS_PCT = 0.01      # 1% Риск (1 R)
TAKE_PROFIT_PCT = 0.031   # 3.1% Тейк-Профит (3.1 R)
DELAY_BETWEEN_ORDERS = 1.5 # Задержка в секундах между обработкой алертов

execution_lock = asyncio.Lock()

async def process_mexc_order(data: dict):
    """Функция обработки ордера strictly solo (1 позиция на весь депо)"""
    async with execution_lock:
        exchange = None
        try:
            action = data.get("action")
            raw_symbol = data.get("symbol", "").replace(".P", "").replace("USDT", "")
            symbol_futures = f"{raw_symbol}/USDT:USDT"

            exchange = ccxt.mexc({
                'apiKey': MEXC_API_KEY,
                'secret': MEXC_SECRET_KEY,
                'options': {'defaultType': 'swap'},
                'enableRateLimit': True,
            })

            # 1. ПРОВЕРКА: Есть ли УЖЕ открытые позиции по аккаунту?
            # Получаем абсолютно все активные позиции
            all_positions = await exchange.fetch_positions()
            
            # Фильтруем те, где объем больше нуля (реально открытые сделки)
            active_positions = [
                p for p in all_positions 
                if float(p.get('contracts', 0)) > 0 or float(p.get('initialMargin', 0)) > 0
            ]

            if len(active_positions) > 0:
                print(f"[{raw_symbol}] ПРОПУСК: Депозит уже занят (открыта позиция по {active_positions[0]['symbol']})")
                await exchange.close()
                await asyncio.sleep(DELAY_BETWEEN_ORDERS)
                return

            # 2. Если открытых позиций нет — проверяем баланс
            balance = await exchange.fetch_balance()
            free_usdt = float(balance['USDT']['free'])

            if free_usdt < 5:  # Меньше $5 — считать свободный депо нулевым
                print(f"[{raw_symbol}] ПРОПУСК: Недостаточно свободного USDT ({free_usdt} USDT)")
                await exchange.close()
                await asyncio.sleep(DELAY_BETWEEN_ORDERS)
                return

            # 3. Получаем актуальную цену
            ticker = await exchange.fetch_ticker(symbol_futures)
            entry_price = float(ticker['last'])

            # Расчет объема на весь свободный депозит с плечом 10x
            leverage = 10 
            position_size_usdt = free_usdt * 0.95 * leverage
            amount = round(position_size_usdt / entry_price, 2)

            # 4. Отправляем 1 единый ордер (Вход + TP + SL)
            if action == "buy":
                sl_price = round(entry_price * (1 - STOP_LOSS_PCT), 4)
                tp_price = round(entry_price * (1 + TAKE_PROFIT_PCT), 4)
                
                params = {
                    'stopLoss': {'triggerPrice': sl_price, 'type': 'stop_market'},
                    'takeProfit': {'triggerPrice': tp_price, 'type': 'take_profit_market'}
                }

                order = await exchange.create_market_buy_order(symbol_futures, amount, params=params)
                print(f"[{raw_symbol}] BUY УСПЕШНО ОТКРЫТ | Цена: {entry_price} | TP: {tp_price} | SL: {sl_price}")

            elif action == "sell":
                sl_price = round(entry_price * (1 + STOP_LOSS_PCT), 4)
                tp_price = round(entry_price * (1 - TAKE_PROFIT_PCT), 4)
                
                params = {
                    'stopLoss': {'triggerPrice': sl_price, 'type': 'stop_market'},
                    'takeProfit': {'triggerPrice': tp_price, 'type': 'take_profit_market'}
                }

                order = await exchange.create_market_sell_order(symbol_futures, amount, params=params)
                print(f"[{raw_symbol}] SELL УСПЕШНО ОТКРЫТ | Цена: {entry_price} | TP: {tp_price} | SL: {sl_price}")

            await exchange.close()

        except Exception as e:
            if exchange:
                await exchange.close()
            print(f"[{data.get('symbol')}] Ошибка исполнение сделки: {str(e)}")

        # Пауза перед проверкой следующего алерта
        await asyncio.sleep(DELAY_BETWEEN_ORDERS)


@app.get("/")
def home():
    return {"status": "MEXC Single-Position Bridge is running!"}


@app.post("/webhook")
async def receive_webhook(request: Request, background_tasks: BackgroundTasks):
    try:
        body_bytes = await request.body()
        if not body_bytes:
            raise HTTPException(status_code=400, detail="Empty payload")
        
        data = json.loads(body_bytes.decode('utf-8'))
        
        if SECRET_PASSPHRASE and data.get("passphrase") != SECRET_PASSPHRASE:
            raise HTTPException(status_code=403, detail="Forbidden")

        action = data.get("action")
        raw_symbol = data.get("symbol", "")

        if not action or not raw_symbol:
            raise HTTPException(status_code=400, detail="Missing params")

        # Добавляем в асинхронную очередь
        background_tasks.add_task(process_mexc_order, data)

        return {"status": "queued", "message": "Alert received"}

    except Exception as e:
        print(f"Error in webhook: {str(e)}")
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

