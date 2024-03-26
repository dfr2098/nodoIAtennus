from fastapi import FastAPI, WebSocket
from kafka import KafkaProducer, KafkaConsumer

app = FastAPI()

producer = KafkaProducer(bootstrap_servers=["localhost:9092"])
consumer = KafkaConsumer("chat", group_id="my-group",
                        bootstrap_servers=["localhost:9092"])

@app.websocket("/chat")
async def chat(websocket: WebSocket):
    await websocket.accept()
    consumer.subscribe()
    for message in consumer:
        await websocket.send_json({"message": message.value.decode()})

@app.post("/send")
async def send(message: str):
    producer.send("chat", message.encode())
    return {"success": True}
