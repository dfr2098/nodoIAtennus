import json
from uuid import NAMESPACE_URL, uuid5, uuid4
from fastapi import BackgroundTasks, FastAPI, HTTPException
from kafka import KafkaConsumer, KafkaProducer
from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import datetime

app = FastAPI()

# Conexión a la base de datos de MongoDB
# Conectarse a MongoDB
cliente = MongoClient('192.168.1.120', 27018, serverSelectionTimeoutMS=5000, username='dfr209811', password='nostromo987Q_Q')  
# Acceder a la base de datos y la colección MongoDB
bd = cliente['tennus_data_analitica']  
coleccion = bd['Mensajes']  

# Configurar el productor de Kafka
productor = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Configurar el consumidor de Kafka
consumidor = KafkaConsumer('json_topic',
                         bootstrap_servers='localhost:9092',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                         group_id='my-group')


def generar_uuid(username):
    namespace = NAMESPACE_URL  # Puedes usar otro namespace si lo prefieres
    return str(uuid5(namespace, username))

# Función para enviar un mensaje personalizado y guardar en el archivo JSON y MongoDB
def enviar_y_restaurar(username, nombre, contenido):
    # Generar un UUID único como ID
    print("Tipo de username:", type(username))
    print("Valor de username:", username)
    nuevo_id = generar_uuid(str(username))

    # Crear un mensaje con la estructura JSON definida y personalizada
    mensaje = {
        "usuario_id": nuevo_id,
        "user_name": username,
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
        
    try:
        # Guardar el mensaje en MongoDB
        coleccion.insert_one(mensaje)
        print("Mensaje almacenado en MongoDB.")
    except Exception as e:
        print("Error al almacenar el mensaje en MongoDB:", e)  

# Función para procesar un mensaje recibido
def procesar_mensaje(mensaje):
    print("Mensaje recibido:", mensaje)
    # procesar el mensaje

# Endpoint para recuperar el historial general de un usuario
@app.get('/historial/{user_name}')
async def obtener_historial(user_name: str):
    mensajes = list(coleccion.find({"user_name": user_name}))
    if not mensajes:
        raise HTTPException(status_code=404, detail="No se encontraron mensajes para este usuario")
    
    historial = []
    for mensaje in mensajes:
        historial.append({
            'usuario_id': mensaje['usuario_id'], 
            'username': mensaje['user_name'],  
            'nombre': mensaje['nombre'],
            'contenido': mensaje['contenido'],
            'timestamp': mensaje['timestamp']
        })
    
    return {'historial': historial}

"""
# Endpoint para recuperar el historial de mensajes de un usuario
@app.get('/historial de mensajes/{user_name}')
async def obtener_historial_mensajes(user_name: str):
    mensajes = list(coleccion.find({"user_name": user_name}))
    if not mensajes:
        raise HTTPException(status_code=404, detail="No se encontraron mensajes para este usuario")
    
    contenido_historial = [mensaje['contenido'] for mensaje in mensajes]
    
    return {f'Mensajes de {user_name}': contenido_historial}
"""

# Endpoint para recuperar el historial de mensajes de un usuario
@app.get('/historial de mensajes/{user_name}')
async def obtener_historial_mensajes(user_name: str):
    mensajes = list(coleccion.find({"user_name": user_name}))
    if not mensajes:
        raise HTTPException(status_code=404, detail=f"No se encontraron mensajes para el usuario {user_name}")
    
    historial = []
    for mensaje in mensajes:
        historial.append({
            'usuario': mensaje['user_name'],
            'contenido': mensaje['contenido']
        })
    
    return {f'Mensajes de {user_name}': historial}

# Ruta para enviar mensajes
@app.post("/enviar_mensajes")
async def enviar_mensajes(username: str, nombre: str, contenido: str, background_tasks: BackgroundTasks):
    background_tasks.add_task(enviar_y_restaurar, username, nombre, contenido)
    return {"mensaje": "Mensaje enviado y almacenado correctamente"}

# Definición del endpoint para extraer el contenido específico
@app.get("/Mensajes")
async def obtener_solamente_mensajes():
    # Realizar la consulta a la base de datos
    documentos = coleccion.find()
    mensajes = []
    for documento in documentos:
        # Extraer el contenido específico (en este caso, el valor del campo 'contenido')
        contenido = documento.get('contenido')
        mensajes.append(contenido)
    return {"mensajes": mensajes}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="localhost", port=8000)