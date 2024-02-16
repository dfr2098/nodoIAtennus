from fastapi import FastAPI
from motor.motor_asyncio import AsyncIOMotorClient

client = AsyncIOMotorClient('mongodb://localhost:27017')
db = client.mydatabase

app = FastAPI()

@app.get("/")
async def root():
    return {"message": "Hello World"}