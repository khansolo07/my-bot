import os
import json
from typing import Optional, List, Dict
from fastapi import FastAPI, HTTPException, Request, Query
import uvicorn

app = FastAPI(title="MT4 TradingView Webhook Bridge")

# Секретный пароль из переменных окружения Render (по умолчанию MYSUPERSECRETKEY123)
SECRET_PASSPHRASE = os.getenv("WEBHOOK_PASSPHRASE", "MYSUPERSECRETKEY123")

# Очередь сигналов в оперативной памяти сервера (FIFO)
signal_queue: List[Dict] = []

@app.get("/")
@app.head("/")
async def root():
    return {
        "status": "online",
        "message": "MT4 Webhook Bridge is running!",
        "pending_signals": len(signal_queue)
    }

@app.post("/webhook")
async def receive_webhook(request: Request):
    """Принимает вебхук от TradingView и сохраняет его в очередь"""
    try:
        body_bytes = await request.body()
        if not body_bytes:
            raise HTTPException(status_code=400, detail="Empty payload")
        
        data = json.loads(body_bytes.decode('utf-8'))
        
        # Проверка секретного ключа
        passphrase = data.get("passphrase") or data.get("pass")
        if SECRET_PASSPHRASE and passphrase != SECRET_PASSPHRASE:
            raise HTTPException(status_code=403, detail="Forbidden: Invalid passphrase")

        action = str(data.get("action", "")).lower()
        symbol = str(data.get("symbol", "")).upper()

        if not action or not symbol:
            raise HTTPException(status_code=400, detail="Missing required parameters: action or symbol")

        # Формируем объект сигнала под MT4
        signal = {
            "symbol": symbol,
            "action": action,         # "buy" или "sell"
            "price": data.get("price", 0),
            "sl": data.get("sl", 0),  # Уровень SL в пунктах или ценовое значение
            "tp": data.get("tp", 0),  # Уровень TP в пунктах или ценовое значение
            "volume": data.get("volume", 0.01) # Лот (по умолчанию 0.01)
        }

        # Добавляем в очередь
        signal_queue.append(signal)
        print(f"[+] Новое оповещение в очереди: {signal}")

        return {"status": "success", "message": "Signal queued for MT4"}

    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON format")
    except Exception as e:
        print(f"Error in webhook: {str(e)}")
        return {"status": "error", "message": str(e)}

@app.get("/signal")
async def get_signal(passphrase: Optional[str] = Query(None)):
    """Вызывается советником MT4 каждые 1-2 сек для получения нового сигнала"""
    if SECRET_PASSPHRASE and passphrase != SECRET_PASSPHRASE:
        raise HTTPException(status_code=403, detail="Forbidden")

    # Если очереди сигналов нет — отдаем статус none
    if not signal_queue:
        return {"action": "none"}

    # Забираем самый старый сигнал (FIFO) и сразу удаляем из очереди
    signal = signal_queue.pop(0)
    print(f"[-] Сигнал отдан советнику MT4: {signal}")
    return signal

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

