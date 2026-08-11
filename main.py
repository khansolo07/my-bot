import os
import json
from typing import Optional, List, Dict
from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

app = FastAPI(title="MT5 TradingView Webhook Bridge")

# Разрешаем все заголовки и методы
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Секретный пароль из переменных окружения Render (по умолчанию MYSUPERSECRETKEY123)
SECRET_PASSPHRASE = os.getenv("WEBHOOK_PASSPHRASE", "MYSUPERSECRETKEY123")

# Очередь сигналов в оперативной памяти сервера (FIFO)
signal_queue: List[Dict] = []

@app.get("/")
@app.head("/")
async def root():
    return {
        "status": "online",
        "message": "MT5 Webhook Bridge is running!",
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

        # Формируем объект сигнала под MT5
        signal = {
            "symbol": symbol,
            "action": action,         # "buy" или "sell"
            "price": data.get("price", 0),
            "sl": data.get("sl", 0),  # Уровень SL
            "tp": data.get("tp", 0),  # Уровень TP
            "volume": data.get("volume", 0.01) # Лот (по умолчанию 0.01)
        }

        # Добавляем в очередь
        signal_queue.append(signal)
        print(f"[+] Новое оповещение в очереди: {signal}")

        return {"status": "success", "message": "Signal queued for MT5"}

    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON format")
    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Error in webhook: {str(e)}")
        return {"status": "error", "message": str(e)}

@app.api_route("/signal", methods=["GET", "POST"])
async def get_signal(request: Request, passphrase: Optional[str] = Query(None)):
    """Принимает запросы от советника MT5 (поддерживает и GET, и POST)"""
    req_passphrase = passphrase
    
    # Если запрос пришел методом POST, пробуем достать passphrase из JSON
    if request.method == "POST":
        try:
            body = await request.json()
            if isinstance(body, dict):
                req_passphrase = req_passphrase or body.get("passphrase") or body.get("pass")
        except Exception:
            pass

    if SECRET_PASSPHRASE and req_passphrase != SECRET_PASSPHRASE:
        raise HTTPException(status_code=403, detail="Forbidden: Invalid passphrase")

    # Если очереди сигналов нет — отдаем статус none
    if not signal_queue:
        return {"action": "none"}

    # Забираем самый старый сигнал (FIFO) и сразу удаляем из очереди
    signal = signal_queue.pop(0)
    print(f"[-] Сигнал отдан советнику MT5: {signal}")
    return signal

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

