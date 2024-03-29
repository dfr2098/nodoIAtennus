import json, os
from kafka import KafkaProducer, KafkaConsumer
from datetime import datetime
"""
from fastapi import FastAPI

# Inicializar la aplicación FastAPI
app = FastAPI()
"""

# Configurar el productor de Kafka
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Configurar el consumidor de Kafka
consumer = KafkaConsumer('json_topic',
                         bootstrap_servers='localhost:9092',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))


# Verificar si el archivo "messages.json" existe / no hace nada al parecer 
if not os.path.exists('mensajes.json'):
    # Si el archivo no existe, crearlo con una lista vacía
    print("mensajes.json no existe...")
    with open('mensajes.json', 'w') as file:
        file.write('[]\n')
        print("creando mensajes.json...")

print("Entrando a la def") #se queda acá 
# Función para enviar un mensaje personalizado y guardar en el archivo JSON
def send_and_store_message(id, name, content):
    print("Creando mensaje...")
    # Crear un mensaje con la estructura JSON definida y personalizada
    message = {
        "id": id,
        "name": name,
        "content": content,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    print("Mensaje creado:", message)
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
    
"""
    Enviar el mensaje al topic de Kafka
    producer.send('json_topic', value=message)
    print("Enviando mensaje a Kafka...")
    print("Mensaje enviado correctamente.")

    # Almacenar el mensaje en el archivo JSON
    with open('mensajes.json', 'a') as file:
        json.dump(message, file)
        file.write('\n')  # Agregar un salto de línea para separar los mensajes
"""

"""
# Ruta para enviar un mensaje personalizado
@app.post("/send_message/")
async def send_message(id: str, name: str, content: str):
    send_and_update_json(id, name, content)
    return {"message": "Message sent successfully"}

# Ruta para obtener los mensajes almacenados en el archivo JSON
@app.get("/get_messages/")
async def get_messages():
    with open('messages.json', 'r') as file:
        messages = [json.loads(line) for line in file]
    return messages

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
"""

# Función para procesar un mensaje recibido
def process_message(message):
    print("Mensaje procesado:", message)
    # Procesar el mensaje 

# Ejecutar el consumidor en un bucle infinito para recibir mensajes continuamente
for message in consumer:
    print("Mensaje recibido:", message.value)
    process_message(message.value)

# Llamar a la función para enviar un mensaje personalizado y almacenarlo
send_and_store_message("unique_id_789", "Bob Johnson", "Hola, este es otro mensaje personalizado.")