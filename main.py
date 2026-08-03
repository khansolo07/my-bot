import asyncio
import json
import os
import ccxt.async_support as ccxt
from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
import uvicorn

app = FastAPI(title="Safe Single-Position MEXC Offset Bridge")

@app.get("/")
@app.head("/")
async def root():
    return {"status": "MEXC Offset Bridge is running!"}

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
    """Обработка ордера с жестким отступом цены в стакане"""
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
                print(f"[{raw_symbol}] ПРОПУСК: Позиция уже открыта")
                await exchange.close()
                await asyncio.sleep(DELAY_BETWEEN_ORDERS)
                return

            # 2. Баланс
            balance = await exchange.fetch_balance({'type': 'swap'})
            usdt_data = balance.get('USDT', {})
            free_usdt = float(usdt_data.get('free') or usdt_data.get('total') or 0)

            if free_usdt < 5:
                print(f"[{raw_symbol}] ПРОПУСК: Недостаточно USDT ({free_usdt})")
                await exchange.close()
                await asyncio.sleep(DELAY_BETWEEN_ORDERS)
                return

            # 3. Запрос стакана и ДОБАВЛЕНИЕ ОТСТУПА
            orderbook = await exchange.fetch_order_book(symbol_futures, limit=5)
            market = exchange.market(symbol_futures)
            
            # Шаг цены инструмента (например, 0.01 для SOL)
            precision_price = market.get('precision', {}).get('price', 2)
            tick_size = 10 ** (-precision_price) if isinstance(precision_price, int) else float(precision_price)

            if action == "buy":
                # Берем лучший Bid и отступаем НИЖЕ (на 1-2 шага), чтобы встать в очередь
                base_price = float(orderbook['bids'][0][0])
                limit_entry_price = float(exchange.price_to_precision(symbol_futures, base_price - tick_size))
                entry_side = 'buy'
                exit_side = 'sell'
            elif action == "sell":
                # Берем лучший Ask и отступаем ВЫШЕ (например: с 73.10 на 73.11)
                base_price = float(orderbook['asks'][0][0])
                limit_entry_price = float(exchange.price_to_precision(symbol_futures, base_price + tick_size))
                entry_side = 'sell'
                exit_side = 'buy'
            else:
                print(f"[{raw_symbol}] Неизвестное действие: {action}")
                await exchange.close()
                return

            contract_size = float(market.get('contractSize', 1.0))
            margin_usdt = free_usdt * MARGIN_PCT
            position_size_usdt = margin_usdt * LEVERAGE

            raw_contracts = position_size_usdt / (limit_entry_price * contract_size)
            amount = float(exchange.amount_to_precision(symbol_futures, raw_contracts))

            # Расчет TP / SL
            chart_sl_pct = TARGET_MARGIN_RISK / LEVERAGE
            chart_tp_pct = TARGET_MARGIN_TP / LEVERAGE

            if entry_side == "buy":
                sl_price = float(exchange.price_to_precision(symbol_futures, limit_entry_price * (1 - chart_sl_pct)))
                tp_price = float(exchange.price_to_precision(symbol_futures, limit_entry_price * (1 + chart_tp_pct)))
            else:
                sl_price = float(exchange.price_to_precision(symbol_futures, limit_entry_price * (1 + chart_sl_pct)))
                tp_price = float(exchange.price_to_precision(symbol_futures, limit_entry_price * (1 - chart_tp_pct)))

            # Выставляем плечо
            try:
                await exchange.set_leverage(LEVERAGE, symbol_futures)
            except Exception:
                pass

            # 4. Отправка ЧИСТОГО Post-Only ордера с отступом цены
            print(f"[{raw_symbol}] Ставим OFFSET LIMIT {entry_side.upper()} по цене {limit_entry_price} (база была {base_price})")
            
            main_order = await exchange.create_order(
                symbol=symbol_futures,
                type='limit',
                side=entry_side,
                amount=amount,
                price=limit_entry_price,
                params={
                    'postOnly': True,
                    'timeInForce': 'PostOnly'
                }
            )
            print(f"[{raw_symbol}] Ордер с отступом успешно выстал в стакан!")

            # 5. Выставление TP лимиткой в стакан
            try:
                await exchange.create_order(
                    symbol=symbol_futures,
                    type='limit',
                    side=exit_side,
                    amount=amount,
                    price=tp_price,
                    params={'reduceOnly': True}
                )
                print(f"[{raw_symbol}] Тейк-Профит выставлен по {tp_price}")
            except Exception as e_tp:
                print(f"[{raw_symbol}] Ошибка TP: {str(e_tp)}")

            # 6. Выставление SL
            try:
                await exchange.create_order(
                    symbol=symbol_futures,
                    type='stop_market',
                    side=exit_side,
                    amount=amount,
                    stopPrice=sl_price,
                    params={'reduceOnly': True, 'stopPrice': sl_price}
                )
                print(f"[{raw_symbol}] Стоп-Лосс выставлен по {sl_price}")
            except Exception as e_sl:
                print(f"[{raw_symbol}] Ошибка SL: {str(e_sl)}")

            await exchange.close()

        except Exception as e:
            if exchange:
                await exchange.close()
            print(f"[{data.get('symbol')}] Ошибка: {str(e)}")

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

