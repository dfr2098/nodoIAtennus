from ast import parse
import json
from urllib import request
from uuid import NAMESPACE_URL, uuid5
from fastapi.responses import JSONResponse
from kafka import KafkaProducer, KafkaConsumer
from datetime import datetime
from fastapi import FastAPI, BackgroundTasks, HTTPException, Form, Request
from concurrent.futures import ThreadPoolExecutor
from typing import List
from pymongo import MongoClient
from kafkaMostrarMen import consumir_mensajes_kafka
import mysql.connector
from user_agents import parse
from typing import Optional
import datetime

app = FastAPI()


# Conectarse a MongoDB
cliente = MongoClient('192.168.1.120', 27018, serverSelectionTimeoutMS=5000, username='dfr209811', password='nostromo987Q_Q')  
# Acceder a la base de datos y la colección MongoDB
bd = cliente['tennus_data_analitica']  
coleccion = bd['Mensajes']  

# Configura los parámetros de conexión de MySQL
config = {
  'user': 'tennus01',
  'password': 'sulaco987Q_Q',
  'host': '192.168.1.120',
  'database': 'test',
  'port': '3307', # Puerto predeterminado de MySQL
}

"""
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

# Configurar el productor de Kafka
productor = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Configurar el consumidor de Kafka
consumidor = KafkaConsumer('json_topic',
                         bootstrap_servers='localhost:9092',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                         group_id='my-group')


def generar_uuid(username):
    namespace = NAMESPACE_URL
    return str(uuid5(namespace, username))

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
        
#funcion para guardar usuario MySQL
def guardar_usuario_mysql(user_name, id_user_name, nombre_usuario, tipo_usuario, ip_usuario):
    try:
        # Guardar el usuario en MySQL
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor()
        
        sql = "INSERT INTO usuarios (user_name, id_user_name, nombre_usuario, tipo_usuario, ip_usuario) VALUES (%s, %s, %s, %s, %s)"
        val = (user_name, id_user_name, nombre_usuario, tipo_usuario, ip_usuario)
        
        cursor.execute(sql, val)
        
        connection.commit()
        
        print("Usuario almacenado en MySQL.")
    except Exception as e:
        print("Error al almacenar el usuario en MySQL:", e)
    finally:
        if 'connection' in locals():
            if connection.is_connected():
                cursor.close()
                connection.close()
                print("Conexión a MySQL cerrada.")

#Función para extraer los datos de MySQL
def leer_usuarios():
    try:
        # Establecer conexión a MySQL
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor(dictionary=True)  # Configuración para devolver un diccionario en lugar de una lista de tuplas)

        # Ejecutar consulta para obtener todos los usuarios
        cursor.execute("SELECT * FROM usuarios")
        
        # Obtener todos los usuarios
        usuarios = cursor.fetchall()

        # Cerrar cursor y conexión
        cursor.close()
        connection.close()

        return usuarios
    except Exception as e:
        print("Error al leer usuarios de MySQL:", e)
        return []


# Función para procesar un mensaje recibido
def procesar_mensaje(mensaje):
    print("Mensaje recibido:", mensaje)
    # procesar el mensaje 

    
"""
# Función para obtener mensajes del archivo JSON   No se usan  
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

# Función para obtener mensajes de la colección de MongoDB
def obtener_mensajes_mongo(coleccion):
    mensajes = []
    try:
        # Obtener todos los documentos de la colección
        documentos = coleccion.find()
        # Recorrer los documentos y agregar los mensajes a la lista
        for documento in documentos:
            mensajes.append(documento)
    except Exception as e:
        print("Error al obtener mensajes de MongoDB:", e)
    return mensajes
"""

# Función de tarea de fondo para recibir mensajes de Kafka en segundo plano
def kafka_tarea_consumidor():
    for mensaje in consumidor:
        procesar_mensaje(mensaje.value)

# Ruta para enviar mensajes
@app.post("/enviar_mensajes")
async def enviar_mensajes(request: Request, mensaje: str, username: str, nombre: str, tipo_usuario: str, background_tasks: BackgroundTasks, ip: Optional[str] = None, user_agent: Optional[str] = None):
    background_tasks.add_task(Enviar_y_restaurar, request, mensaje, username, nombre, tipo_usuario, ip, user_agent)
    return {"mensaje": "Mensaje enviado y almacenado correctamente"}

#Parte para enviar usuarios por medio de FasAPI de MySQL
@app.post("/usuarios/")
async def crear_usuario(
    user_name: str = Form(...),
    id_user_name: str = Form(...),
    nombre_usuario: str = Form(...),
    tipo_usuario: str = Form(...),
    ip_usuario: str = Form(...)
):
    try:
        guardar_usuario_mysql(
            user_name,
            id_user_name,
            nombre_usuario,
            tipo_usuario,
            ip_usuario
        )
        return {"mensaje": "Usuario creado exitosamente"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


#Función para leer los archivos del JSON
def leer_mensajes_archivo(file_path):
    with open(file_path, 'r') as archivo:
        info = archivo.read()
    mensajes = json.loads(info)
    return mensajes

#Función para leer los archivos de MongoDB
def leer_mensajes_coleccion(coleccion):
    mensajes = []
    try:
        # Obtener todos los documentos de la colección
        documentos = coleccion.find()
        # Recorrer los documentos y agregar los mensajes a la lista
        for documento in documentos:
            mensajes.append(documento)
    except Exception as e:
        print("Error al obtener mensajes de MongoDB:", e)
    return mensajes

# Función para obtener mensajes del consumidor de Kafka
def leer_mensajes_kafka():
    mensajes = []
    try:
        for mensaje in consumidor:
            mensajes.append(mensaje.value)
    except Exception as e:
        print("Error al obtener mensajes del consumidor de Kafka:", e)
    return mensajes

# Lista para almacenar los mensajes
mensajes = []

# Función para el bucle del consumidor
def bucle_consumidor():
    for mensaje in consumidor:
        try:
            # Agregar el mensaje a la lista directamente
            mensajes.append(mensaje.value)
        except Exception as e:
            # Si se produce una excepción, agregar un mensaje de error
            error_mensaje = f"Error procesando mensaje: {e}. Mensaje saltado: {mensaje.value}"
            mensajes.append(error_mensaje)

# Obtener los mensajes actuales de kafka
@app.get("/Mensajes actuales de Kafka")
async def obtener_kafka_mensajes_actuales():
    return {"mensajes": mensajes}

# Iniciar el bucle del consumidor en un hilo separado
import threading
thread = threading.Thread(target=bucle_consumidor)
thread.start()

#Parte de obtener mensajes pasados de kafka
#Obtener mensajes pasados de Kafka en API
@app.get("/Mensajes de kafka")
async def obtener_mensajes_pasados_kafka():
    mensajes = await consumir_mensajes_kafka()
    return mensajes

#Obtener los mensajes para mostrarlos en la API
@app.get("/Mensajes del JSON local")
async def obtener_mensajes():
    try:
        mensajes = leer_mensajes_archivo("mensajes.json")
    except (FileNotFoundError, json.JSONDecodeError) as e:
        return JSONResponse({"error": str(e)}, status_code=400)
    return mensajes

#Obtener los mensajes de Mongo en API
@app.get("/Mensajes de MongoDB")
async def obtener_mensajes_MongoDB():
    try:
        mensajes = leer_mensajes_coleccion(coleccion)
        # Excluir el campo "_id" de cada documento
        mensajes_sin_id = [{k: v for k, v in mensaje.items() if k != '_id'} for mensaje in mensajes]
        return mensajes_sin_id
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)

#API para leer los usuarios de la tabla de MySQL
@app.get("/usuarios de MySQL")
async def obtener_usuarios():
    try:
        # Leer usuarios de MySQL
        usuarios = leer_usuarios()

        # Verificar si hay usuarios
        if usuarios:
            return {"usuarios": usuarios}
        else:
            raise HTTPException(status_code=404, detail="No se encontraron usuarios")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Iniciar el consumidor de Kafka en segundo plano
ejecutor = ThreadPoolExecutor(max_workers=1)
ejecutor.submit(kafka_tarea_consumidor)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="localhost", port=8000)
