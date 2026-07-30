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

MARGIN_PCT = 0.97          # 97% от свободного баланса (3% запас на комиссию биржи)
LEVERAGE = 6               # Плечо 6x
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
                'options': {
                    'defaultType': 'swap',
                },
                'enableRateLimit': True,
            })

            # Загружаем рынки, чтобы корректно получить размер контракта (contractSize)
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

            # 3. Получаем цену, размер контракта и считаем точный объем КОНТРАКТОВ MEXC
            ticker = await exchange.fetch_ticker(symbol_futures)
            entry_price = float(ticker['last'])

            market = exchange.market(symbol_futures)
            contract_size = float(market.get('contractSize', 1.0))

            margin_usdt = free_usdt * MARGIN_PCT
            position_size_usdt = margin_usdt * LEVERAGE

            # Главное исправление: Объем в USDT делим на (Цену * Размер 1 контракта)
            raw_contracts = position_size_usdt / (entry_price * contract_size)
            
            # Приводим к разрешенной точности шага биржи
            amount = float(exchange.amount_to_precision(symbol_futures, raw_contracts))

            # Дистанция TP/SL по графику
            chart_sl_pct = TARGET_MARGIN_RISK / LEVERAGE
            chart_tp_pct = TARGET_MARGIN_TP / LEVERAGE

            if action == "buy":
                sl_price = float(exchange.price_to_precision(symbol_futures, entry_price * (1 - chart_sl_pct)))
                tp_price = float(exchange.price_to_precision(symbol_futures, entry_price * (1 + chart_tp_pct)))
                side = 'buy'
                close_side = 'sell'
            elif action == "sell":
                sl_price = float(exchange.price_to_precision(symbol_futures, entry_price * (1 + chart_sl_pct)))
                tp_price = float(exchange.price_to_precision(symbol_futures, entry_price * (1 - chart_tp_pct)))
                side = 'sell'
                close_side = 'buy'
            else:
                print(f"[{raw_symbol}] Неизвестное действие: {action}")
                await exchange.close()
                return

            print(f"[{raw_symbol}] Открываем {side.upper()} | Свободно: {free_usdt}$ | Маржа: {round(margin_usdt,2)}$ | Контрактов: {amount} (Contract Size: {contract_size})")

            # Устанавливаем плечо
            try:
                await exchange.set_leverage(LEVERAGE, symbol_futures)
            except Exception:
                pass

            # Открытие позиции сразу с привязанными TP/SL
            try:
                main_order = await exchange.create_order(
                    symbol=symbol_futures,
                    type='market',
                    side=side,
                    amount=amount,
                    params={
                        'stopLossPrice': sl_price,
                        'takeProfitPrice': tp_price
                    }
                )
                print(f"[{raw_symbol}] Позиция открыта с TP ({tp_price}) и SL ({sl_price})!")
            except Exception as e_main:
                print(f"[{raw_symbol}] Ошибка открытия с встроенным TP/SL, пробуем запасной вариант: {e_main}")
                
                # Запасной вариант: Открываем маркет, а TP/SL ставим отдельными ордерами
                main_order = await exchange.create_market_order(
                    symbol=symbol_futures,
                    side=side,
                    amount=amount
                )
                print(f"[{raw_symbol}] Позиция открыта по маркету!")

                await asyncio.sleep(0.5)

                # Выставление SL отдельным ордером
                try:
                    await exchange.create_order(
                        symbol=symbol_futures,
                        type='STOP_MARKET',
                        side=close_side,
                        amount=amount,
                        params={
                            'triggerPrice': sl_price,
                            'stopPrice': sl_price,
                            'reduceOnly': True
                        }
                    )
                    print(f"[{raw_symbol}] SL выставлен: {sl_price}")
                except Exception as e_sl:
                    print(f"[{raw_symbol}] Ошибка выставления SL: {e_sl}")

                # Выставление TP отдельным ордером
                try:
                    await exchange.create_order(
                        symbol=symbol_futures,
                        type='TAKE_PROFIT_MARKET',
                        side=close_side,
                        amount=amount,
                        params={
                            'triggerPrice': tp_price,
                            'stopPrice': tp_price,
                            'reduceOnly': True
                        }
                    )
                    print(f"[{raw_symbol}] TP выставлен: {tp_price}")
                except Exception as e_tp:
                    print(f"[{raw_symbol}] Ошибка выставления TP: {e_tp}")

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

