import json
from fastapi.responses import JSONResponse
from kafka import KafkaProducer, KafkaConsumer
from datetime import datetime
from fastapi import FastAPI, BackgroundTasks
from concurrent.futures import ThreadPoolExecutor
from typing import List

app = FastAPI()

# Configurar el productor de Kafka
productor = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Configurar el consumidor de Kafka
consumidor = KafkaConsumer('json_topic',
                         bootstrap_servers='localhost:9092',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                         group_id='my-group')

#Función para sacar el ultimo número del JSON
def obtener_ultimo_id():
    try:
        with open('mensajes.json', 'r', encoding='utf-8') as archivo:
            mensajes = json.load(archivo)
            if mensajes:
                # Obtener el último ID como entero
                ultimo_id = mensajes[-1]['id']
                return int(ultimo_id)
            else:
                return 0
    except (FileNotFoundError, ValueError):
        return 0


# Función para enviar un mensaje personalizado y guardar en el archivo JSON
def Enviar_y_restaurar(nombre, contenido):
    # Obtener el último ID y generar el nuevo ID
    ultimo_id = obtener_ultimo_id()
    nuevo_id = ultimo_id + 1
    # Crear un mensaje con la estructura JSON definida y personalizada
    mensaje = {
        "id": str(nuevo_id), #convertir int a cadena
        "nombre": nombre,
        "contenido": contenido,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    try:
        # Enviar el mensaje al topic de Kafka
        productor.send('json_topic', value=mensaje)
        print("Enviando mensaje a Kafka...")
        print("Mensaje enviado correctamente.")
    except Exception as e:
        print("Error al enviar mensaje a Kafka:", e)

    try:
        # Leer mensajes anteriores del archivo JSON, si los hay
        mensajes = []
        try:
            with open('mensajes.json', 'r', encoding='utf-8') as archivo:
                mensajes = json.load(archivo)
        except FileNotFoundError:
            pass  # El archivo no existe, crea un archivo 
        
        # Agregar el nuevo mensaje a la lista
        mensajes.append(mensaje)
        
        # Guardar la lista completa de mensajes en el archivo JSON
        with open('mensajes.json', 'w', encoding='utf-8') as archivo:
            json.dump(mensajes, archivo, indent=4, ensure_ascii=False)
        print("Mensaje almacenado en el archivo JSON.")
    except Exception as e:
        print("Error al almacenar el mensaje en el archivo JSON:", e)

# Función para procesar un mensaje recibido
def procesar_mensaje(mensaje):
    print("Mensaje recibido:", mensaje)
    # procesar el mensaje 

# Función para obtener mensajes del archivo JSON    
def obtener_mensajes():
    mensajes = []
    try:
        with open('mensajes.json', 'r') as archivo:
            for linea in archivo:
                try:
                    mensaje = json.loads(linea)
                    mensajes.append(mensaje)
                except json.JSONDecodeError as e:
                    print("Error al decodificar JSON en la línea:", linea)
                    print("Error:", e)
    except Exception as e:
        print("Error al obtener mensajes del archivo JSON:", e)
    return mensajes

# Función de tarea de fondo para recibir mensajes de Kafka en segundo plano
def kafka_tarea_consumidor():
    for mensaje in consumidor:
        procesar_mensaje(mensaje.value)

# Ruta para enviar mensajes
@app.post("/enviar_mensajes")
async def enviar_mensajes(nombre: str, contenido: str, background_tasks: BackgroundTasks):
    background_tasks.add_task(Enviar_y_restaurar, nombre, contenido)
    return {"mensaje": "Mensaje enviado y almacenado correctamente"}

#Función para leer los archivos del JSON
def leer_mensajes_archivo(file_path):
    with open(file_path, 'r') as archivo:
        info = archivo.read()
    mensajes = json.loads(info)
    return mensajes

#Obtener los mensajes para mostrarlos en la API
@app.get("/mensajes")
async def obtener_mensajes():
    try:
        mensajes = leer_mensajes_archivo("mensajes.json")
    except (FileNotFoundError, json.JSONDecodeError) as e:
        return JSONResponse({"error": str(e)}, status_code=400)
    return mensajes

# Iniciar el consumidor de Kafka en segundo plano
ejecutor = ThreadPoolExecutor(max_workers=1)
ejecutor.submit(kafka_tarea_consumidor)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="localhost", port=8000)
