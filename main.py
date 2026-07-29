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

            # 1. ПРОВЕРКА: Есть ли УЖЕ открытые позиции по аккаунту?
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

            # 2. Получение свободного баланса USDT на фьючерсах
            balance = await exchange.fetch_balance({'type': 'swap'})
            usdt_data = balance.get('USDT', {})
            free_usdt = float(usdt_data.get('free') or usdt_data.get('total') or 0)

            if free_usdt < 5:
                print(f"[{raw_symbol}] ПРОПУСК: Мало USDT на балансе ({free_usdt} USDT)")
                await exchange.close()
                await asyncio.sleep(DELAY_BETWEEN_ORDERS)
                return

            # 3. Актуальная цена
            ticker = await exchange.fetch_ticker(symbol_futures)
            entry_price = float(ticker['last'])

            # Расчет маржи и объема с плечом 6x
            margin_usdt = free_usdt * MARGIN_PCT
            position_size_usdt = margin_usdt * LEVERAGE
            amount = round(position_size_usdt / entry_price, 1)

            # Дистанция TP/SL по графику с учетом 6-го плеча
            chart_sl_pct = TARGET_MARGIN_RISK / LEVERAGE  # 0.166% по графику
            chart_tp_pct = TARGET_MARGIN_TP / LEVERAGE    # 0.516% по графику

            if action == "buy":
                sl_price = round(entry_price * (1 - chart_sl_pct), 2)
                tp_price = round(entry_price * (1 + chart_tp_pct), 2)
                side = 'buy'
                close_side = 'sell'
                side_code_close = 2  # Код закрытия Long позиции
            elif action == "sell":
                sl_price = round(entry_price * (1 + chart_sl_pct), 2)
                tp_price = round(entry_price * (1 - chart_tp_pct), 2)
                side = 'sell'
                close_side = 'buy'
                side_code_close = 4  # Код закрытия Short позиции
            else:
                print(f"[{raw_symbol}] Неизвестное действие: {action}")
                await exchange.close()
                return

            print(f"[{raw_symbol}] Открываем {side.upper()} | Депозит: {free_usdt}$ | Маржа: {round(margin_usdt,2)}$ | Объем: {amount} SOL")

            # Устанавливаем плечо перед входом
            try:
                await exchange.set_leverage(LEVERAGE, symbol_futures)
            except Exception:
                pass

            # 1) Вход по маркету
            main_order = await exchange.create_market_order(
                symbol=symbol_futures,
                side=side,
                amount=amount
            )
            print(f"[{raw_symbol}] Позиция открыта успешно!")

            await asyncio.sleep(0.5)

            # 2) Выставление Стоп-Лосса (прямой API MEXC)
            try:
                await exchange.create_trigger_order(
                    symbol=symbol_futures,
                    type='market',
                    side=close_side,
                    amount=amount,
                    price=None,
                    params={
                        'stopPrice': sl_price,
                        'triggerPrice': sl_price,
                        'type': 5,            # 5 = Market Trigger Order для MEXC
                        'planType': 'STOP_LOSS',
                        'openType': 1
                    }
                )
                print(f"[{raw_symbol}] SL выставлен: {sl_price}")
            except Exception as e_sl:
                print(f"[{raw_symbol}] Ошибка выставления SL: {e_sl}")

            # 3) Выставление Тейк-Профита (прямой API MEXC)
            try:
                await exchange.create_trigger_order(
                    symbol=symbol_futures,
                    type='market',
                    side=close_side,
                    amount=amount,
                    price=None,
                    params={
                        'stopPrice': tp_price,
                        'triggerPrice': tp_price,
                        'type': 5,            # 5 = Market Trigger Order для MEXC
                        'planType': 'TAKE_PROFIT',
                        'openType': 1
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

