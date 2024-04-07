import asyncio
import json
from fastapi import FastAPI
from kafka import KafkaConsumer, TopicPartition

app = FastAPI()

# Configura el consumidor de Kafka
consumidor = KafkaConsumer(
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',  # Para empezar desde el principio
    enable_auto_commit=False,  # Desactiva el auto commit para tener control manual
    value_deserializer=lambda x: x.decode('utf-8')  # Decodifica los mensajes como cadenas
)

async def consumir_mensajes_kafka():
    particion_topic = TopicPartition('json_topic', 0)  # Partición 0 de 'json_topic'
    consumidor.assign([particion_topic])
    consumidor.seek_to_beginning(particion_topic)  # Se posiciona al principio del topic

    mensajes = []
    while True:
        batch = consumidor.poll(timeout_ms=100)  # Espera mensajes durante 100 ms
        if batch:
            for _, records in batch.items():
                for record in records:
                    try:
                        json_obj = json.loads(record.value)
                        mensajes.append(json_obj)
                    except ValueError:
                        # Agrega un diccionario indicando que el mensaje no es válido
                        mensajes.append({"mensaje": record.value, "error": "Mensaje no válido"})
        else:
            break
    return mensajes


"""
#Obtener mensajes pasados de Kafka en API
@app.get("/Mensajes de kafka")
async def obtener_mensajes_pasados_kafka():
    mensajes = await asyncio.gather(consumir_mensajes_kafka())
    return mensajes[0]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="localhost", port=8000)
"""