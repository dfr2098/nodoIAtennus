import json
from fastapi.responses import JSONResponse
from kafka import KafkaProducer, KafkaConsumer
from datetime import datetime
from fastapi import FastAPI, BackgroundTasks
from concurrent.futures import ThreadPoolExecutor
from typing import List

app = FastAPI()

# Configurar el productor de Kafka
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Configurar el consumidor de Kafka
consumer = KafkaConsumer('json_topic',
                         bootstrap_servers='localhost:9092',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                         group_id='my-group')

# Función para enviar un mensaje personalizado y guardar en el archivo JSON
def send_and_store_message(id, name, content):
    # Crear un mensaje con la estructura JSON definida y personalizada
    message = {
        "id": id,
        "name": name,
        "content": content,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    try:
        # Enviar el mensaje al topic de Kafka
        producer.send('json_topic', value=message)
        print("Enviando mensaje a Kafka...")
        print("Mensaje enviado correctamente.")
    except Exception as e:
        print("Error al enviar mensaje a Kafka:", e)

    try:
        # Almacenar el mensaje en el archivo JSON
        with open('mensajes.json', 'a') as file:
            json.dump(message, file)
            file.write('\n')  # Agregar un salto de línea para separar los mensajes
        print("Mensaje almacenado en el archivo JSON.")
    except Exception as e:
        print("Error al almacenar el mensaje en el archivo JSON:", e)

# Función para procesar un mensaje recibido
def process_message(message):
    print("Mensaje recibido:", message)
    # procesar el mensaje 

# Función para obtener mensajes del archivo JSON    
def get_messages():
    messages = []
    try:
        with open('mensajes.json', 'r') as file:
            for line in file:
                try:
                    message = json.loads(line)
                    messages.append(message)
                except json.JSONDecodeError as e:
                    print("Error al decodificar JSON en la línea:", line)
                    print("Error:", e)
    except Exception as e:
        print("Error al obtener mensajes del archivo JSON:", e)
    return messages


"""
# Función para obtener mensajes del archivo JSON, da error 
def get_messages():
    messages = []
    try:
        with open('mensajes.json', 'r') as file:
            messages = json.load(file)
    except Exception as e:
        print("Error al obtener mensajes del archivo JSON:", e)
    return messages
"""


# Función de tarea de fondo para recibir mensajes de Kafka en segundo plano
def kafka_consumer_task():
    for message in consumer:
        process_message(message.value)


# Ruta para enviar mensajes
@app.post("/send_message/")
async def send_message(id: str, name: str, content: str, background_tasks: BackgroundTasks):
    background_tasks.add_task(send_and_store_message, id, name, content)
    return {"message": "Mensaje enviado y almacenado correctamente"}

"""
# Ruta para ingresar un nuevo mensaje, da error 
@app.post("/messages/")
async def create_message(id: str, name: str, content: str):
    new_message = {"id": id, "name": name, "content": content}
    if save_message(new_message):
        return {"message": "Mensaje guardado correctamente"}
    else:
        return {"message": "Error al guardar el mensaje"}
"""

"""
# Ruta para obtener mensajes del archivo JSON, reconoce el JSON pero si está incorrecto
@app.get("/messages/", response_model=List[dict])
async def read_messages():
    return get_messages()
"""

#lee el JSON en el formato correcto necesario
def read_messages_from_file(file_path):
    with open(file_path, 'r') as file:
        data = file.read()
    messages = json.loads(data)
    return messages

@app.get("/mensajes")
async def get_messages():
    try:
        messages = read_messages_from_file("mensajes.json")
    except (FileNotFoundError, json.JSONDecodeError) as e:
        return JSONResponse({"error": str(e)}, status_code=400)
    return messages


# Iniciar el consumidor de Kafka en segundo plano
executor = ThreadPoolExecutor(max_workers=1)
executor.submit(kafka_consumer_task)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="localhost", port=8000)
