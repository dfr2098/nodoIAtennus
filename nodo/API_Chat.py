import json
from kafka import KafkaConsumer, KafkaProducer
from pymongo import MongoClient
import uvicorn
from Chatbot import predecir_clase, obtener_respuesta, intentos
from fastapi import FastAPI, HTTPException, Request
from typing import Optional
import datetime
from uuid import NAMESPACE_URL, uuid4, uuid5
from user_agents import parse
import requests  

# Configuración de Kafka
productor = KafkaProducer(bootstrap_servers='localhost:9092',
                          value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'))

input_consumidor = KafkaConsumer('input_topic',
                               bootstrap_servers='localhost:9092',
                               value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                               group_id='chatbot-group')

# Nueva configuración para el segundo topic de respuestas
output_consumidor = KafkaConsumer('output_topic',
                                bootstrap_servers='localhost:9092',
                                group_id='chatbot-group')

"""
# Conexión a la base de datos de MongoDB
# Conectarse a MongoDB
cliente = MongoClient('192.168.1.120', 27018, serverSelectionTimeoutMS=5000, username='dfr209811', password='nostromo987Q_Q')  
# Acceder a la base de datos y la colección MongoDB
bd = cliente['tennus_data_analitica']  
coleccion = bd['Mensajes']  
"""

# URL del servidor Kafka Connect
kafka_connect_url = "http://localhost:8084/connectors"


"""
# URL del servidor Kafka Connect
kafka_connect_url = "http://localhost:8084/connectors"

# Configuración del conector MongoDB
mongodb_sink_config = {
    "name": "mongodb-sink",
    "config": {
        "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
        "tasks.max": "1", #tareas maximas a procesar, siempre debe ser igual o menor que las particiones del topic de kafka
        "topics": "input_topic,output_topic",
        "connection.uri": "mongodb://dfr209811:nostromo987Q_Q@192.168.1.120:27018/",
        "database": "tennus_data_analitica",
        "collection": "Mensajes",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        "key.ignore": "true"
    }
}

# Enviar la solicitud HTTP para crear el conector
response = requests.post(kafka_connect_url, json=mongodb_sink_config)

# Verificar si la solicitud fue exitosa
if response.status_code == 201:
    print("Conector MongoDB creado con éxito.")
else:
    print("Error al crear el conector MongoDB:", response.text)


# URL del servidor Kafka Connect
kafka_connect_url = "http://localhost:8084/connectors"

# Configuración del conector Debezium MySQL
debezium_mysql_config = {
    "name": "debezium-mysql-connector",
    "config": {
        "connector.class": "io.debezium.connector.mysql.MySqlConnector",
        "tasks.max": "1", # número máximo de tareas
        "database.hostname": "192.168.1.120", # dirección del servidor de la base de datos MySQL
        "database.port": "3307", # puerto de la base de datos MySQL
        "database.user": "tennus01", # usuario de la base de datos MySQL
        "database.password": "sulaco987Q_Q", # contraseña de la base de datos MySQL
        "database.server.id": "1", #id del servidor para kafka
        "database.server.name": "dbusuarios", #nombre del servidor para kafka
        "database.whitelist": "test", # lista blanca de bases de datos a las que se debe conectar
        "table.whitelist": "usuarios", # lista blanca de tablas a las que se debe conectar
        "database.history.kafka.bootstrap.servers": "localhost:9092", # direcciones de los servidores de Kafka
        "database.history.kafka.topic": "usuarios_mysql", # nombre del tema de Kafka para el historial de la base de datos
        "topic.prefix": "clientes_" #prefijo descriptivo del funcionamiento para kafka
    }
}


# Enviar la solicitud HTTP para crear el conector
response = requests.post(kafka_connect_url, json=debezium_mysql_config)

# Verificar si la solicitud fue exitosa
if response.status_code == 201:
    print("Conector Debezium MySQL creado con éxito.")
else:
    print("Error al crear el conector Debezium MySQL:", response.text)
"""

app = FastAPI()

def generar_uuid(username):
    namespace = NAMESPACE_URL
    return str(uuid5(namespace, username))

# Diccionario para almacenar las conversaciones asociadas con el usuario_id
conversaciones = {}


def construir_conversacion(request: Request, mensaje: str, username: str, nombre: str, tipo_usuario: str, ip: Optional[str] = None, user_agent: Optional[str] = None):
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
    
    # Construir el objeto JSON que simula una conversación
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
    
    return conversacion


@app.get("/chat")
async def chat(request: Request, mensaje: str, username: str, nombre: str, tipo_usuario: str, ip: Optional[str] = None, user_agent: Optional[str] = None):
    # Construir la conversación
    conversacion = construir_conversacion(request, mensaje, username, nombre, tipo_usuario, ip, user_agent)
    
    # Obtener el usuario_id de la conversación
    usuario_id = conversacion["usuario_id"]
    
    # Almacenar la conversación asociada con el usuario_id
    conversaciones[usuario_id] = conversacion
    
    # Enviamos la solicitud al chatbot
    productor.send('input_topic', value={"conversación": conversacion})
    
    return {"conversación": conversacion}


@app.post("/respuesta_chat/{usuario_id}")
async def obtener_respuesta_chat(usuario_id: str):
    # Obtener los datos del usuario asociados con el usuario_id
    datos_usuario = conversaciones.get(usuario_id)

    if datos_usuario is None:
        return {"error": "Token de conversación no válido"}

    # Obtener el mensaje del usuario
    mensaje = datos_usuario.get("mensaje")

    # Obtener el intento y la respuesta del chatbot
    intento = predecir_clase(mensaje)
    respuesta = obtener_respuesta(intento, intentos)
    
    # Asegurarse de que datos_usuario tenga una clave "conversacion" que sea una lista
    if "conversacion" not in datos_usuario:
        datos_usuario["Chatbot"] = []

    # Añadir la respuesta del chatbot a la conversación
    respuesta_chatbot = {
        "autor": "Chatbot",
        "respuesta": respuesta,
        "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    # Agregar la respuesta del chatbot a la conversación del usuario
    datos_usuario["Chatbot"].append(respuesta_chatbot)
    
    # Enviamos la conversación completa al topic de salida
    productor.send('output_topic', value={"respuesta": respuesta_chatbot})

    return {"Respuesta": respuesta_chatbot}

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
"""
mongodb_sink_connector_name = "mongodb-sink"

# Endpoint para recuperar el historial de mensajes de un usuario
"""
@app.get('/historial_de_mensajes/{username_usuario}')
async def obtener_historial_mensajes(username_usuario: str):
    # Parámetros de consulta para el conector MongoDB
    params = {"username_usuario": username_usuario}

    # Enviar la solicitud HTTP al conector de Kafka Connect
    response = requests.get(f"{kafka_connect_url}/{mongodb_sink_connector_name}/status", params=params)

    # Verificar si la solicitud fue exitosa
    if response.status_code == 200:
        # Extraer los datos del historial de mensajes de la respuesta JSON
        historial = response.json()
        return historial
    elif response.status_code == 404:
        # Si no se encontraron mensajes, lanzar una excepción HTTP 404
        raise HTTPException(status_code=404, detail=f"No se encontraron mensajes para el usuario {username_usuario}")
    else:
        # Si ocurrió otro error, lanzar una excepción genérica
        raise HTTPException(status_code=response.status_code, detail="Error al obtener el historial de mensajes")
"""
@app.get('/historial_de_mensajes/{username_usuario}')
async def obtener_historial_mensajes(username_usuario: str):
    # Parámetros de consulta para filtrar los mensajes por usuario
    params = {"username_usuario": username_usuario}

    # Enviar la solicitud HTTP al conector de Kafka Connect para obtener los mensajes filtrados por usuario
    response = requests.get(f"{kafka_connect_url}/{mongodb_sink_connector_name}/topics/input_topic", params=params)

    # Verificar si la solicitud fue exitosa
    if response.status_code == 200:
        # Extraer los datos del historial de mensajes de la respuesta JSON
        historial = response.json()
        return historial
    elif response.status_code == 404:
        # Si no se encontraron mensajes, lanzar una excepción HTTP 404
        raise HTTPException(status_code=404, detail=f"No se encontraron mensajes para el usuario {username_usuario}")
    else:
        # Si ocurrió otro error, lanzar una excepción genérica
        raise HTTPException(status_code=response.status_code, detail="Error al obtener el historial de mensajes")

# Iniciar uvicorn 

if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=8000)

