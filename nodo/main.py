from fastapi import FastAPI, HTTPException
from motor.motor_asyncio import AsyncIOMotorClient

app = FastAPI()
@app.on_event("startup")
async def startup_event():
    client = AsyncIOMotorClient("mongodb://dfr209811:nostromo987Q_Q@172.45.0.4:27017/tennus_data_analitica")
    db = client.tennus_data_analitica
    if not await db.list_collection_names():
        raise HTTPException(status_code=500, detail="No se pudo conectar a MongoDB")

@app.get("/")
async def root():
    return {"message": "Hello World"}
    
    
    
    