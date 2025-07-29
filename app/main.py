### File: app/main.py

from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from .pi_worker import process_all_seeds
from app.routes import router

import uvicorn
from pydantic import BaseModel
from typing import List

class TransferRequest(BaseModel):
    owner: str
    amount: float
    seeds: List[str]

app = FastAPI()
app.include_router(router)
app.mount("/static", StaticFiles(directory="app/templates"), name="static")
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

if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
