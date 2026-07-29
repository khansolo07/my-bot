from fastapi import FastAPI, HTTPException, Request, BackgroundTasks
import ccxt.async_support as ccxt
import uvicorn
import os
import json
import asyncio

app = FastAPI(title="Safe Single-Position MEXC Bridge")

@app.get("/")
@app.head("/")
async def root():
    return {"status": "MEXC Single-Position Bridge is running!"}

MEXC_API_KEY = os.getenv("MEXC_API_KEY")
MEXC_SECRET_KEY = os.getenv("MEXC_SECRET_KEY")
SECRET_PASSPHRASE = os.getenv("WEBHOOK_PASSPHRASE", "MYSUPERSECRETKEY123") 

# Настройки Маржи, Плеча, Риска и Тейка
MARGIN_PCT = 0.90          # Берем 90% от свободного баланса под маржу
LEVERAGE = 6               # Плечо 6x
TARGET_MARGIN_RISK = 0.01  # 1% риск от маржи (0.45$ при 45$ марже)
TARGET_MARGIN_TP = 0.031   # 3.1% тейк от маржи (1.40$ при 45$ марже)
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

            # Расчет маржи (90% от баланса) и объема позиции с плечом 6x
            margin_usdt = free_usdt * MARGIN_PCT
            position_size_usdt = margin_usdt * LEVERAGE
            amount = round(position_size_usdt / entry_price, 2)

            # Дистанция TP/SL по графику с учетом 6-го плеча
            chart_sl_pct = TARGET_MARGIN_RISK / LEVERAGE  # 0.166% по графику
            chart_tp_pct = TARGET_MARGIN_TP / LEVERAGE    # 0.516% по графику

            # 4. Открываем основной ордер и отдельно выставляем TP и SL для MEXC
            if action == "buy":
                sl_price = round(entry_price * (1 - chart_sl_pct), 4)
                tp_price = round(entry_price * (1 + chart_tp_pct), 4)
                order_side = 'buy'
                close_side = 'sell'
            elif action == "sell":
                sl_price = round(entry_price * (1 + chart_sl_pct), 4)
                tp_price = round(entry_price * (1 - chart_tp_pct), 4)
                order_side = 'sell'
                close_side = 'buy'
            else:
                print(f"[{raw_symbol}] Неизвестное действие: {action}")
                await exchange.close()
                return

            # 1) Вход по маркету
            order = await exchange.create_order(
                symbol=symbol_futures,
                type='market',
                side=order_side,
                amount=amount
            )
            print(f"[{raw_symbol}] {action.upper()} ОТКРЫТ | Маржа: {round(margin_usdt,2)}$ | Объем: {amount}")

            # 2) Стоп-Лосс ордер
            await exchange.create_order(
                symbol=symbol_futures,
                type='STOP_MARKET',
                side=close_side,
                amount=amount,
                params={'stopPrice': sl_price, 'triggerPrice': sl_price, 'reduceOnly': True}
            )

            # 3) Тейк-Профит ордер
            await exchange.create_order(
                symbol=symbol_futures,
                type='TAKE_PROFIT_MARKET',
                side=close_side,
                amount=amount,
                params={'stopPrice': tp_price, 'triggerPrice': tp_price, 'reduceOnly': True}
            )
            print(f"[{raw_symbol}] TP ({tp_price}) и SL ({sl_price}) успешно выставлены!")

            await exchange.close()

        except Exception as e:
            if exchange:
                await exchange.close()
            print(f"[{data.get('symbol')}] Ошибка исполнения сделки: {str(e)}")

        # Пауза перед проверкой следующего алерта
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

        # Добавляем в асинхронную очередь
        background_tasks.add_task(process_mexc_order, data)

        return {"status": "queued", "message": "Alert received"}

    except Exception as e:
        print(f"Error in webhook: {str(e)}")
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)


