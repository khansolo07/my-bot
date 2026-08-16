import os
import json
import time
from typing import Optional, List, Dict
from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

app = FastAPI(title="MT4/MT5 TradingView Webhook Bridge")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

SECRET_PASSPHRASE = os.getenv("WEBHOOK_PASSPHRASE", "MYSUPERSECRETKEY123")

# ==================== НАСТРОЙКИ ФИЛЬТРАЦИИ ====================
# 1. Время жизни сигнала в секундах (если робот был выключен)
MAX_SIGNAL_AGE_SECONDS = 120.0  # 2 минуты

# 2. Минимальный интервал между алертами по одной валютной паре
# Все алерты, пришедшие раньше этого времени после первого, БУДУТ ИГНОРИРОВАТЬСЯ.
MIN_ALERT_INTERVAL_SECONDS = 180.0  # 180 секунд (поставить 180, если нужно)
# ==============================================================

signal_queue: List[Dict] = []
# Хранит время последнего принятого алерта для каждого символа (например, "BTCUSD")
last_alert_times: Dict[str, float] = {}

@app.get("/")
@app.head("/")
async def root():
    return {
        "status": "online",
        "message": "MT4/MT5 Webhook Bridge is running!",
        "pending_signals": len(signal_queue)
    }

@app.post("/webhook")
async def receive_webhook(request: Request):
    """Принимает вебхук от TradingView с защитой от частых повторов"""
    global signal_queue, last_alert_times
    try:
        body_bytes = await request.body()
        if not body_bytes:
            raise HTTPException(status_code=400, detail="Empty payload")
        
        data = json.loads(body_bytes.decode('utf-8'))
        
        passphrase = data.get("passphrase") or data.get("pass")
        if SECRET_PASSPHRASE and passphrase != SECRET_PASSPHRASE:
            raise HTTPException(status_code=403, detail="Forbidden: Invalid passphrase")

        action = str(data.get("action", "")).lower()
        symbol = str(data.get("symbol", "")).upper()

        if not action or not symbol:
            raise HTTPException(status_code=400, detail="Missing required parameters: action or symbol")

        current_time = time.time()

        # ПРОВЕРКА НА ДУБЛИКАТЫ И ЧАСТЫЕ АЛЕРТЫ:
        last_time = last_alert_times.get(symbol, 0.0)
        time_diff = current_time - last_time

        # Если с момента последнего алерта прошло меньше MIN_ALERT_INTERVAL_SECONDS — сбрасываем
        if time_diff < MIN_ALERT_INTERVAL_SECONDS:
            left_sec = int(MIN_ALERT_INTERVAL_SECONDS - time_diff)
            print(f"[-] Повторный алерт {action} {symbol} отсечен! Прошло всего {int(time_diff)} сек.")
            return {
                "status": "ignored", 
                "message": f"Alert throttled. Cooldown active for {left_sec}s"
            }

        # Фиксируем время успешного приема алерта
        last_alert_times[symbol] = current_time

        signal = {
            "symbol": symbol,
            "action": action,
            "price": data.get("price", 0),
            "sl": data.get("sl", 0),
            "tp": data.get("tp", 0),
            "volume": data.get("volume", 0.01),
            "created_at": current_time
        }

        signal_queue.append(signal)
        print(f"[+] Первый алерт принят: {action} {symbol}")

        return {"status": "success", "message": "Signal queued"}

    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON format")
    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Error in webhook: {str(e)}")
        return {"status": "error", "message": str(e)}

@app.api_route("/signal", methods=["GET", "POST"])
async def get_signal(request: Request, passphrase: Optional[str] = Query(None)):
    """Выдает актуальные сигналы советнику MT4/MT5"""
    global signal_queue
    
    req_passphrase = passphrase
    if request.method == "POST":
        try:
            body = await request.json()
            if isinstance(body, dict):
                req_passphrase = req_passphrase or body.get("passphrase") or body.get("pass")
        except Exception:
            pass

    if SECRET_PASSPHRASE and req_passphrase != SECRET_PASSPHRASE:
        raise HTTPException(status_code=403, detail="Forbidden: Invalid passphrase")

    current_time = time.time()

    # 1. Фильтруем просроченные сигналы (если робот был выключен)
    valid_signals = []
    for sig in signal_queue:
        age = current_time - sig.get("created_at", 0)
        if age <= MAX_SIGNAL_AGE_SECONDS:
            valid_signals.append(sig)
        else:
            print(f"[-] Просроченный сигнал удален из очереди (возраст {int(age)} сек): {sig['action']} {sig['symbol']}")
    
    signal_queue = valid_signals

    # 2. Если очередь пуста — отдаем "none"
    if not signal_queue:
        return {"action": "none"}

    # 3. Достаем первый сигнал из очереди
    signal = signal_queue.pop(0)
    signal.pop("created_at", None)
    
    print(f"[-] Сигнал отдан советнику: {signal}")
    return signal

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

