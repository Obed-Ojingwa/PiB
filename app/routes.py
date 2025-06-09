# app/routes.py

from fastapi import APIRouter, Query
from app.utils import derive_keypair

router = APIRouter()

@router.get("/check-address")
async def check_address(mnemonic: str = Query(..., description="BIP39 mnemonic phrase")):
    try:
        keypair = derive_keypair(mnemonic)
        return {"address": keypair.public_key}
    except Exception as e:
        return {"error": str(e)}
