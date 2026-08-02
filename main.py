import asyncio
import json
import os
import ccxt.async_support as ccxt
from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
import uvicorn

app = FastAPI(title="Safe Single-Position MEXC Bridge")

@app.get("/")
@app.head("/")
async def root():
    return {"status": "MEXC Single-Position Bridge is running!"}

MEXC_API_KEY = os.getenv("MEXC_API_KEY")
MEXC_SECRET_KEY = os.getenv("MEXC_SECRET_KEY")
SECRET_PASSPHRASE = os.getenv("WEBHOOK_PASSPHRASE", "MYSUPERSECRETKEY123") 

MARGIN_PCT = 0.97          # 97% от свободного баланса
LEVERAGE = 8               # Плечо 8x
TARGET_MARGIN_RISK = 0.01  # 1% риск от маржи
TARGET_MARGIN_TP = 0.031   # 3.1% тейк от маржи
DELAY_BETWEEN_ORDERS = 1.5

execution_lock = asyncio.Lock()

def pos_info_len(p):
    try:
        return float(p.get('info', {}).get('holdVol', 0))
    except Exception:
        return 0

async def process_mexc_order(data: dict):
    """Функция обработки ордера strictly solo с исполнением через GUARANTEED MAKER (postOnly = True)"""
    async with execution_lock:
        exchange = None
        try:
            action = data.get("action")
            raw_symbol = data.get("symbol", "").replace(".P", "").replace("USDT", "")
            symbol_futures = f"{raw_symbol}/USDT:USDT"

            exchange = ccxt.mexc({
                'apiKey': MEXC_API_KEY,
                'secret': MEXC_SECRET_KEY,
                'options': {
                    'defaultType': 'swap',
                },
                'enableRateLimit': True,
            })

            await exchange.load_markets()

            # 1. ПРОВЕРКА: Есть ли УЖЕ открытые позиции?
            all_positions = await exchange.fetch_positions()
            active_positions = [
                p for p in all_positions 
                if float(p.get('contracts', 0) or pos_info_len(p)) > 0
            ]

            if len(active_positions) > 0:
                print(f"[{raw_symbol}] ПРОПУСК: Депозит уже занят (открыта позиция)")
                await exchange.close()
                await asyncio.sleep(DELAY_BETWEEN_ORDERS)
                return

            # 2. Получение свободного баланса USDT
            balance = await exchange.fetch_balance({'type': 'swap'})
            usdt_data = balance.get('USDT', {})
            free_usdt = float(usdt_data.get('free') or usdt_data.get('total') or 0)

            if free_usdt < 5:
                print(f"[{raw_symbol}] ПРОПУСК: Мало USDT на балансе ({free_usdt} USDT)")
                await exchange.close()
                await asyncio.sleep(DELAY_BETWEEN_ORDERS)
                return

            # 3. Запрашиваем стакан цен (Orderbook) для определения цены Maker
            orderbook = await exchange.fetch_order_book(symbol_futures, limit=5)
            
            if action == "buy":
                # Для покупки берем лучший Bid (покупатель)
                limit_entry_price = float(orderbook['bids'][0][0])
                side = 'buy'
            elif action == "sell":
                # Для продажи берем лучший Ask (продавец)
                limit_entry_price = float(orderbook['asks'][0][0])
                side = 'sell'
            else:
                print(f"[{raw_symbol}] Неизвестное действие: {action}")
                await exchange.close()
                return

            market = exchange.market(symbol_futures)
            contract_size = float(market.get('contractSize', 1.0))

            margin_usdt = free_usdt * MARGIN_PCT
            position_size_usdt = margin_usdt * LEVERAGE

            raw_contracts = position_size_usdt / (limit_entry_price * contract_size)
            amount = float(exchange.amount_to_precision(symbol_futures, raw_contracts))

            # Расчет TP/SL от цены лимитного входа
            chart_sl_pct = TARGET_MARGIN_RISK / LEVERAGE
            chart_tp_pct = TARGET_MARGIN_TP / LEVERAGE

            if side == "buy":
                sl_price = float(exchange.price_to_precision(symbol_futures, limit_entry_price * (1 - chart_sl_pct)))
                tp_price = float(exchange.price_to_precision(symbol_futures, limit_entry_price * (1 + chart_tp_pct)))
            else:
                sl_price = float(exchange.price_to_precision(symbol_futures, limit_entry_price * (1 + chart_sl_pct)))
                tp_price = float(exchange.price_to_precision(symbol_futures, limit_entry_price * (1 - chart_tp_pct)))

            print(f"[{raw_symbol}] Открываем POST-ONLY LIMIT {side.upper()} по {limit_entry_price} | Контрактов: {amount}")

            # Устанавливаем плечо
            try:
                await exchange.set_leverage(LEVERAGE, symbol_futures)
            except Exception:
                pass

            # 4. Отправка LIMIT Post-Only ордера (Гарантированный Мейкер / 0% комиссии)
            main_order = await exchange.create_order(
                symbol=symbol_futures,
                type='limit',
                side=side,
                amount=amount,
                price=limit_entry_price,
                params={
                    'postOnly': True,  # Принудительно исполнять как Maker (0% Fee)
                    'stopLossPrice': sl_price,
                    'takeProfitPrice': tp_price
                }
            )
            print(f"[{raw_symbol}] Лимитный Post-Only ордер размещен по цене {limit_entry_price} (TP: {tp_price}, SL: {sl_price})!")

            await exchange.close()

        except Exception as e:
            if exchange:
                await exchange.close()
            print(f"[{data.get('symbol')}] Ошибка исполнения сделки: {str(e)}")

        await asyncio.sleep(DELAY_BETWEEN_ORDERS)


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

        background_tasks.add_task(process_mexc_order, data)
        return {"status": "queued", "message": "Alert received"}

    except Exception as e:
        print(f"Error in webhook: {str(e)}")
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

