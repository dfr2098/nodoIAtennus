from ast import parse
import json
from typing import Optional
from uuid import NAMESPACE_URL, uuid5, uuid4
from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
from kafka import KafkaConsumer, KafkaProducer
from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import datetime
import mysql.connector

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

"""
# Configura los parámetros de conexión de MySQL
config = {
  'user': 'tennus01',
  'password': 'sulaco987Q_Q',
  'host': '192.168.1.120',
  'database': 'test',
  'port': '3307', # Puerto predeterminado de MySQL
}

# Intenta establecer la conexión
try:
    # Crea una conexión
    conexion = mysql.connector.connect(**config)

    # Comprueba si la conexión fue exitosa
    if conexion.is_connected():
        print('Conexión establecida correctamente.')
        # Realiza operaciones en la base de datos aquí

except mysql.connector.Error as error:
    print(f'Error al conectar a la base de datos: {error}')
    
finally:
    # Cierra la conexión
    if 'conexion' in locals() and conexion.is_connected():
        conexion.close()
        print('Conexión cerrada.')
"""

def generar_uuid(username):
    namespace = NAMESPACE_URL
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
        "mensaje": contenido,
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

# Función para enviar un mensaje personalizado y guardar en el archivo JSON
def Enviar_y_restaurar(request: Request, mensaje:str, username:str, nombre:str, tipo_usuario:str, ip=None, user_agent=None):
    # Generar el usuario_id
    usuario_id = generar_uuid(username)

    # Acceder a la dirección IP del cliente
    direccion_ip = request.client.host if ip is None else ip
    # Analizar el User-Agent para obtener información sobre el dispositivo y el sistema operativo
    dispositivo = parse(request.headers.get("User-Agent") if user_agent is None else user_agent)
    # Sacar el sistema operativo y su versión
    sistema_operativo = dispositivo.os.family if hasattr(dispositivo, "os") else "Desconocido"
    version_sistema_operativo = dispositivo.os.version_string if hasattr(dispositivo, "os") else None
    sistema_operativo_completo = f"{sistema_operativo} {version_sistema_operativo}" if version_sistema_operativo else sistema_operativo
    
    # Obtener la fecha y hora actual
    fecha_hora_actual = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # Crear un mensaje con la estructura JSON definida y personalizada
    conversacion = {
        "usuario_id": usuario_id,
        "ip": direccion_ip,
        "user_agent": request.headers.get("User-Agent") if user_agent is None else user_agent,
        "sistema_operativo": sistema_operativo_completo,
        "tipo_usuario": tipo_usuario,
        "nombre_usuario": nombre,
        "username_usuario": username,
        "mensaje": mensaje,
        "timestamp": fecha_hora_actual
    }
    
    try:
        # Enviar el mensaje al topic de Kafka
        productor.send('json_topic', value=conversacion)
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
        mensajes.append(conversacion)
        
        # Guardar la lista completa de mensajes en el archivo JSON
        with open('mensajes.json', 'w', encoding='utf-8') as archivo:
            json.dump(mensajes, archivo, indent=4, ensure_ascii=False)
        print("Mensaje almacenado en el archivo JSON.")
    except Exception as e:
        print("Error al almacenar el mensaje en el archivo JSON:", e)
        
    try:
        # Guardar el mensaje en MongoDB
        coleccion.insert_one(conversacion)
        print("Mensaje almacenado en MongoDB.")
    except Exception as e:
        print("Error al almacenar el mensaje en MongoDB:", e)  

# Función para procesar un mensaje recibido
def procesar_mensaje(mensaje):
    print("Mensaje recibido:", mensaje)
    # procesar el mensaje

# Endpoint para recuperar el historial general de un usuario
@app.get('/historial/{username_usuario}')
async def obtener_historial(username_usuario: str):
    mensajes = list(coleccion.find({"username_usuario": username_usuario}))
    if not mensajes:
        raise HTTPException(status_code=404, detail="No se encontraron mensajes para este usuario")
    
    historial = []
    for mensaje in mensajes:
        historial.append({
            'usuario_id': mensaje['usuario_id'], 
            'ip': mensaje['ip'], 
            'user_agent': mensaje['user_agent'], 
            'sistema_operativo': mensaje['sistema_operativo'], 
            'tipo_usuario': mensaje['tipo_usuario'], 
            'nombre_usuario': mensaje['nombre_usuario'],
            'username_usuario': mensaje['username_usuario'],  
            'mensaje': mensaje['mensaje'],
            'timestamp': mensaje['timestamp']
        })
    
    return {'historial': historial}

"""
        "usuario_id": usuario_id,
        "ip": direccion_ip,
        "user_agent": request.headers.get("User-Agent") if user_agent is None else user_agent,
        "sistema_operativo": sistema_operativo_completo,
        "tipo_usuario": tipo_usuario,
        "nombre_usuario": nombre,
        "username_usuario": username,
        "mensaje": mensaje,
        "timestamp": fecha_hora_actual
        
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
@app.get('/historial de mensajes/{username_usuario}')
async def obtener_historial_mensajes(username_usuario: str):
    mensajes = list(coleccion.find({"username_usuario": username_usuario}))
    if not mensajes:
        raise HTTPException(status_code=404, detail=f"No se encontraron mensajes para el usuario {username_usuario}")
    
    historial = []
    for mensaje in mensajes:
        historial.append({
            'usuario': mensaje['username_usuario'],
            'mensaje': mensaje['mensaje']
        })
    
    return {f'Mensajes de {username_usuario}': historial}


# Ruta para enviar mensajes
@app.post("/enviar_mensajes")
async def enviar_mensajes(request: Request, mensaje: str, username: str, nombre: str, tipo_usuario: str, background_tasks: BackgroundTasks, ip: Optional[str] = None, user_agent: Optional[str] = None):
    background_tasks.add_task(Enviar_y_restaurar, request, mensaje, username, nombre, tipo_usuario, ip, user_agent)
    return {"mensaje": "Mensaje enviado y almacenado correctamente"}

# Definición del endpoint para extraer el contenido específico
@app.get("/Mensajes")
async def obtener_solamente_mensajes():
    # Realizar la consulta a la base de datos
    documentos = coleccion.find()
    mensajes = []
    for documento in documentos:
        # Extraer el contenido específico (en este caso, el valor del campo 'mensaje')
        mensaje = documento.get('mensaje')
        mensajes.append(mensaje)
    return {"mensajes": mensajes}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="localhost", port=8000)