# ### File: app/main.py



# app/main.py
# app/main.py

from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from .pi_worker import process_all_seeds, loop_process_forever
from app.routes import router

import asyncio
from pydantic import BaseModel
from typing import List

class TransferRequest(BaseModel):
    owner: str
    amount: float
    seeds: List[str]

app = FastAPI()
app.include_router(router)
templates = Jinja2Templates(directory="app/templates")

@app.get("/", response_class=HTMLResponse)
async def form_ui(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/api/transfer")
async def transfer(data: TransferRequest):
    try:
        results = await process_all_seeds(data.seeds, data.owner, data.amount)
        return JSONResponse(content={"results": results})
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": f"Internal Server Error: {str(e)}"}
        )


# ✅ Background control
loop_task: asyncio.Task | None = None
stop_event: asyncio.Event | None = None

@app.post("/start")
async def start_loop(data: TransferRequest):
    global loop_task, stop_event
    if loop_task and not loop_task.done():
        return {"message": "Transfer already running."}

    stop_event = asyncio.Event()
    loop_task = asyncio.create_task(loop_process_forever(data.seeds, data.owner, data.amount, stop_event))
    return {"message": "Continuous transfer started."}

@app.post("/stop")
async def stop_loop():
    global stop_event, loop_task
    if stop_event:
        stop_event.set()
    if loop_task:
        await loop_task
    return {"message": "Transfer stopped."}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)



# from fastapi import FastAPI, Request, BackgroundTasks
# from fastapi.responses import HTMLResponse, JSONResponse
# from fastapi.staticfiles import StaticFiles
# from fastapi.templating import Jinja2Templates
# from .pi_worker import process_all_seeds
# from app.routes import router

# import uvicorn
# from pydantic import BaseModel
# from typing import List

# class TransferRequest(BaseModel):
#     owner: str
#     amount: float
#     seeds: List[str]

# app = FastAPI()
# app.include_router(router)
# # app.mount("/static", StaticFiles(directory="app/templates"), name="static")
# templates = Jinja2Templates(directory="app/templates")

# @app.get("/", response_class=HTMLResponse)
# async def form_ui(request: Request):
#     return templates.TemplateResponse("index.html", {"request": request})

# @app.post("/api/transfer")
# async def transfer(data: TransferRequest):
#     try:
#         results = await process_all_seeds(data.seeds, data.owner, data.amount)
#         return JSONResponse(content={"results": results})
#     except Exception as e:
#         return JSONResponse(
#             status_code=500,
#             content={"error": f"Internal Server Error: {str(e)}"}
#         )

# if __name__ == "__main__":
#     uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
