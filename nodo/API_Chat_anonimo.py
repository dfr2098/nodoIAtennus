from enum import Enum
import hashlib
import json
import random
import string
import uuid
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from pymongo import MongoClient
import uvicorn
from Chatbot import predecir_clase, obtener_respuesta, intentos
from fastapi import FastAPI, HTTPException, Request
from typing import Optional
import datetime
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5
from user_agents import parse
import requests  
from kafka.admin import KafkaAdminClient, NewTopic, NewPartitions


"""
# Conexión a la base de datos de MongoDB
# Conectarse a MongoDB
cliente = MongoClient('192.168.1.120', 27018, serverSelectionTimeoutMS=5000, username='dfr209811', password='nostromo987Q_Q')  
# Acceder a la base de datos y la colección MongoDB
bd = cliente['tennus_data_analitica']  
coleccion = bd['Mensajes']  
"""

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


# Configuración de Kafka
productor = KafkaProducer(bootstrap_servers='localhost:9092',
                          key_serializer=lambda k: str(k).encode('utf-8'),
                          value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'))

input_consumidor = KafkaConsumer('input_topic',
                               bootstrap_servers='localhost:9092',
                               value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                               group_id='chatbot-group')

# Nueva configuración para el segundo topic de respuestas
output_consumidor = KafkaConsumer('output_topic',
                                bootstrap_servers='localhost:9092',
                                group_id='chatbot-group')


# Define la configuración de los servidores de Kafka
bootstrap_servers = 'localhost:9092'

# Inicializar el administrador del cluster Kafka
admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)



""" Crear topics desde Python
# Asegurarse de que el topic "historial_topic" exista
topic = NewTopic(name="historial_topic", num_partitions=NUM_PARTICIONES, replication_factor=1)
admin_client.create_topics([topic])
"""

# URL del servidor Kafka Connect
kafka_connect_url = "http://localhost:8084/connectors"

app = FastAPI()

# Diccionario para almacenar las conversaciones asociadas con el usuario_id
conversaciones = {}

class TipoUsuario(str, Enum):
    Registrado = "Registrado"
    Anonimo = "Anónimo"

def generar_uuid(username):
    namespace = UUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8')
    return str(uuid5(namespace, username))

def generar_username():
    return ''.join(random.choices(string.ascii_letters + string.digits, k=10))

def generar_nombre():
    return "Anónimo"

def construir_conversacion(request: Request, mensaje: str, tipo_usuario: TipoUsuario, username: str, nombre: str, ip: Optional[str] = None, user_agent: Optional[str] = None):
    # Rellenar username y nombre si el tipo_usuario es "Anónimo"
    if tipo_usuario == TipoUsuario.Anonimo:
        username = generar_username()
        nombre = generar_nombre()
        usuario_id = generar_uuid(username)
    else:
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
        "tipo_usuario": tipo_usuario.value,
        "nombre_usuario": nombre,
        "username_usuario": username,
        "mensaje": mensaje,
        "timestamp": fecha_hora_actual
    }
    
    return conversacion


app = FastAPI()

@app.get("/chat")
async def chat(
    request: Request,
    mensaje: str,
    tipo_usuario: TipoUsuario = TipoUsuario.Anonimo,  # Establecer "Anónimo" como opción predeterminada,
    username: str = None,
    nombre: str = None,
    ip: Optional[str] = None,
    user_agent: Optional[str] = None
):
    """
    Inicia una conversación.
    """
    # Verificar si el usuario es "Registrado"
    if tipo_usuario == TipoUsuario.Registrado:
        if not (username and nombre):
            raise HTTPException(status_code=400, detail="El username y el nombre son obligatorios para usuarios registrados.")
    # Construir la conversación
    conversacion = construir_conversacion(request, mensaje, tipo_usuario, username, nombre, ip, user_agent)
    
    # Obtener el usuario_id de la conversación
    usuario_id = conversacion["usuario_id"]
    
    # Almacenar la conversación asociada con el usuario_id
    conversaciones[usuario_id] = conversacion
    
    # Enviamos la solicitud al chatbot
    productor.send('input_topic', key=usuario_id, value={"conversación": conversacion})
    
    return {"conversación": conversacion}

# Nueva configuración para el tercer topic de historial
historial_productor = KafkaProducer(bootstrap_servers='localhost:9092',
                                    key_serializer=lambda k: str(k).encode('utf-8'),
                                    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'))

MIN_NUM_PARTICIONES = 5
claves_distintas = set()

def asignar_particion(usuario_id: str) -> int:
    num_particiones = max(len(claves_distintas), MIN_NUM_PARTICIONES)
    hash_usuario_id = hash(usuario_id)
    particion = hash_usuario_id % num_particiones
    return particion


def aumentar_particiones_si_es_necesario(topic_name, claves_distintas):
    admin_client = KafkaAdminClient(bootstrap_servers='localhost:9092')
    try:
        topics = admin_client.describe_topics(topic_name)
        if topic_name in topics:
            current_partitions = topics[topic_name].partitions
            num_particiones_actuales = len(current_partitions)
            nuevas_particiones = len(claves_distintas) # 
            if nuevas_particiones > num_particiones_actuales:
                new_partitions = NewPartitions(total_count=nuevas_particiones)
                admin_client.create_partitions({topic_name: new_partitions})
                print(f"Se crearon {nuevas_particiones - num_particiones_actuales} nuevas particiones para el topic {topic_name}")
    finally:
        admin_client.close()

def asignar_particion_modificada(usuario_id: str) -> int:
    num_claves_distintas = len(claves_distintas)
    num_particiones = max(num_claves_distintas, MIN_NUM_PARTICIONES)
    particion = hash(usuario_id) % num_particiones
    return particion


@app.post("/respuesta_chat/{usuario_id}")
async def obtener_respuesta_chat(usuario_id: str, username: Optional[str] = None):
    datos_usuario = conversaciones.get(usuario_id)

    if datos_usuario is None:
        raise HTTPException(status_code=404, detail="Token de conversación no válido")

    mensaje = datos_usuario.get("mensaje")
    usernameR = datos_usuario.get("username_usuario")

    intento = predecir_clase(mensaje)
    respuesta = obtener_respuesta(intento, intentos)
    
    if "conversacion" not in datos_usuario:
        datos_usuario["conversacion"] = []

    claves_distintas.add(usuario_id)

    conversacion_usuario = {
        "usuario_id": usuario_id,
        "ip": datos_usuario.get("ip"),
        "user_agent": datos_usuario.get("user_agent"),
        "sistema_operativo": datos_usuario.get("sistema_operativo"),
        "tipo_usuario": datos_usuario.get("tipo_usuario"),
        "nombre_usuario": datos_usuario.get("nombre_usuario"),
        "username_usuario": usernameR,
        "mensaje": mensaje,
        "timestamp": datos_usuario.get("timestamp")
    }

    respuesta_chatbot = {
        "usuario_id": usuario_id,
        "username_usuario": usernameR,
        "autor": "Chatbot",
        "respuesta": respuesta,
        "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    completo = {
        "Usuario": [conversacion_usuario],
        "Chatbot": [respuesta_chatbot]
    }
    
    productor.send('output_topic', key=usuario_id, value={"respuesta": respuesta_chatbot})
    
    aumentar_particiones_si_es_necesario('historial_topic', claves_distintas)
    
    particion = asignar_particion_modificada(usuario_id)

    historial_productor.send('historial_topic', key=usuario_id, value=completo, partition=particion)

    return {"Respuesta": respuesta_chatbot}

"""FUNCIONA
from fastapi import Header

NUM_PARTICIONES = 10

def asignar_particion(usuario_id: str) -> int:
    # Obtener un hash del usuario_id
    hash_usuario_id = hash(usuario_id)
    # Aplicar módulo para obtener la partición
    particion = hash_usuario_id % NUM_PARTICIONES
    return particion

@app.post("/respuesta_chat/{usuario_id}")
async def obtener_respuesta_chat(
    usuario_id: str,
    username: Optional[str] = None
):
    datos_usuario = conversaciones.get(usuario_id)

    if datos_usuario is None:
        return {"error": "Token de conversación no válido"}

    mensaje = datos_usuario.get("mensaje")
    usernameR = datos_usuario.get("username_usuario")

    intento = predecir_clase(mensaje)
    respuesta = obtener_respuesta(intento, intentos)
    
    if "conversacion" not in datos_usuario:
        datos_usuario["conversacion"] = []

    conversacion_usuario = {
        "usuario_id": usuario_id,
        "ip": datos_usuario.get("ip"),
        "user_agent": datos_usuario.get("user_agent"),
        "sistema_operativo": datos_usuario.get("sistema_operativo"),
        "tipo_usuario": datos_usuario.get("tipo_usuario"),
        "nombre_usuario": datos_usuario.get("nombre_usuario"),
        "username_usuario": usernameR,
        "mensaje": mensaje,
        "timestamp": datos_usuario.get("timestamp")
    }

    respuesta_chatbot = {
        "usuario_id": usuario_id,
        "username_usuario": usernameR,
        "autor": "Chatbot",
        "respuesta": respuesta,
        "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    completo = {
        "Usuario": [conversacion_usuario],
        "Chatbot": [respuesta_chatbot]
    }
    
    productor.send('output_topic', key=usuario_id, value={"respuesta": respuesta_chatbot})
    
    # Asignar partición utilizando la función de asignación de particiones
    particion = asignar_particion(usuario_id)

    historial_productor.send('historial_topic', key=usuario_id, value=completo, partition=particion)

    return {"Respuesta": respuesta_chatbot}
"""

"""
from fastapi import Header
NUM_PARTICIONES = 5

@app.post("/respuesta_chat/{usuario_id}")
async def obtener_respuesta_chat(usuario_id: str, username: Optional[str] = None, partition: int = Header(None)):
    datos_usuario = conversaciones.get(usuario_id)

    if datos_usuario is None:
        return {"error": "Token de conversación no válido"}

    mensaje = datos_usuario.get("mensaje")
    usernameR = datos_usuario.get("username_usuario")

    intento = predecir_clase(mensaje)
    respuesta = obtener_respuesta(intento, intentos)
    
    if "conversacion" not in datos_usuario:
        datos_usuario["conversacion"] = []

    conversacion_usuario = {
        "usuario_id": usuario_id,
        "ip": datos_usuario.get("ip"),
        "user_agent": datos_usuario.get("user_agent"),
        "sistema_operativo": datos_usuario.get("sistema_operativo"),
        "tipo_usuario": datos_usuario.get("tipo_usuario"),
        "nombre_usuario": datos_usuario.get("nombre_usuario"),
        "username_usuario": usernameR,
        "mensaje": mensaje,
        "timestamp": datos_usuario.get("timestamp")
    }

    respuesta_chatbot = {
        "usuario_id": usuario_id,
        "username_usuario": usernameR,
        "autor": "Chatbot",
        "respuesta": respuesta,
        "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    completo = {
        "Usuario": [conversacion_usuario],
        "Chatbot": [respuesta_chatbot]
    }
    
    productor.send('output_topic', key=usuario_id, value={"respuesta": respuesta_chatbot})
    
    # Validar la partición del encabezado
    if partition is None:
        return {"error": "Se requiere el encabezado 'partition' para especificar la partición."}

    particion = partition

    historial_productor.send('historial_topic', key=usuario_id, value=completo, partition=particion)

    return {"Respuesta": respuesta_chatbot}
"""

"""
# Define el número de particiones
NUM_PARTICIONES = 5

@app.post("/respuesta_chat/{usuario_id}")
async def obtener_respuesta_chat(usuario_id: str, username: Optional[str] = None):
    # Obtener los datos del usuario asociados con el usuario_id
    datos_usuario = conversaciones.get(usuario_id)

    if datos_usuario is None:
        return {"error": "Token de conversación no válido"}

    # Obtener el mensaje del usuario
    mensaje = datos_usuario.get("mensaje")
    usernameR = datos_usuario.get("username_usuario")

    # Obtener el intento y la respuesta del chatbot
    intento = predecir_clase(mensaje)
    respuesta = obtener_respuesta(intento, intentos)
    
    # Asegurarse de que datos_usuario tenga una clave "conversacion" que sea una lista
    if "conversacion" not in datos_usuario:
        datos_usuario["conversacion"] = []

    # Añadir la conversación del usuario
    conversacion_usuario = {
        "usuario_id": usuario_id,
        "ip": datos_usuario.get("ip"),
        "user_agent": datos_usuario.get("user_agent"),
        "sistema_operativo": datos_usuario.get("sistema_operativo"),
        "tipo_usuario": datos_usuario.get("tipo_usuario"),
        "nombre_usuario": datos_usuario.get("nombre_usuario"),
        "username_usuario": usernameR,
        "mensaje": mensaje,
        "timestamp": datos_usuario.get("timestamp")
    }

    # Añadir la respuesta del chatbot a la conversación
    respuesta_chatbot = {
        "usuario_id": usuario_id,
        "username_usuario": usernameR,
        "autor": "Chatbot",
        "respuesta": respuesta,
        "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    # Agregar la conversación del usuario y la respuesta del chatbot a la lista
    completo = {
        "Usuario": [conversacion_usuario],
        "Chatbot": [respuesta_chatbot]
    }
    
    # Enviamos la conversación completa al topic de salida
    productor.send('output_topic', key=usuario_id, value={"respuesta": respuesta_chatbot})
    
    # Definir la partición utilizando el usuario_id
    particion = int(usuario_id.replace("-", ""), 16) % NUM_PARTICIONES

    # Enviamos la conversación completa al topic de historial
    historial_productor.send('historial_topic', key=usuario_id, value=completo, partition=particion)

    return {"Respuesta": respuesta_chatbot}



# Define el número de particiones
NUM_PARTICIONES = 5

@app.post("/respuesta_chat/{usuario_id}")
async def obtener_respuesta_chat(usuario_id: str, username: Optional[str] = None):
    # Obtener los datos del usuario asociados con el usuario_id
    datos_usuario = conversaciones.get(usuario_id)

    if datos_usuario is None:
        return {"error": "Token de conversación no válido"}

    # Obtener el mensaje del usuario
    mensaje = datos_usuario.get("mensaje")
    usernameR = datos_usuario.get("username_usuario")

    # Obtener el intento y la respuesta del chatbot
    intento = predecir_clase(mensaje)
    respuesta = obtener_respuesta(intento, intentos)
    
    # Asegurarse de que datos_usuario tenga una clave "conversacion" que sea una lista
    if "conversacion" not in datos_usuario:
        datos_usuario["conversacion"] = []

    # Añadir la conversación del usuario
    conversacion_usuario = {
        "usuario_id": usuario_id,
        "ip": datos_usuario.get("ip"),
        "user_agent": datos_usuario.get("user_agent"),
        "sistema_operativo": datos_usuario.get("sistema_operativo"),
        "tipo_usuario": datos_usuario.get("tipo_usuario"),
        "nombre_usuario": datos_usuario.get("nombre_usuario"),
        "username_usuario": usernameR,
        "mensaje": mensaje,
        "timestamp": datos_usuario.get("timestamp")
    }

    # Añadir la respuesta del chatbot a la conversación
    respuesta_chatbot = {
        "usuario_id": usuario_id,
        "username_usuario": usernameR,
        "autor": "Chatbot",
        "respuesta": respuesta,
        "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    # Agregar la conversación del usuario y la respuesta del chatbot a la lista
    completo = {
        "Usuario": [conversacion_usuario],
        "Chatbot": [respuesta_chatbot]
    }
    
    # Enviamos la conversación completa al topic de salida
    productor.send('output_topic', key=usuario_id, value={"respuesta": respuesta_chatbot})
    
    # Definir la partición utilizando el usuario_id
    particion = abs(hash(usuario_id)) % NUM_PARTICIONES

    # Enviamos la conversación completa al topic de historial
    historial_productor.send('historial_topic', key=usuario_id, value=completo, partition=particion)

    return {"Respuesta": respuesta_chatbot}
"""

""" SIN LO DE LAS PARTICIONES
# Nueva configuración para el tercer topic de historial
historial_productor = KafkaProducer(bootstrap_servers='localhost:9092',
                                    key_serializer=lambda k: str(k).encode('utf-8'),
                                    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'))

@app.post("/respuesta_chat/{usuario_id}")
async def obtener_respuesta_chat(usuario_id: str, username: Optional[str] = None):
    # Obtener los datos del usuario asociados con el usuario_id
    datos_usuario = conversaciones.get(usuario_id)

    if datos_usuario is None:
        return {"error": "Token de conversación no válido"}

    # Obtener el mensaje del usuario
    mensaje = datos_usuario.get("mensaje")
    usernameR = datos_usuario.get("username_usuario")

    # Obtener el intento y la respuesta del chatbot
    intento = predecir_clase(mensaje)
    respuesta = obtener_respuesta(intento, intentos)
    
    # Asegurarse de que datos_usuario tenga una clave "conversacion" que sea una lista
    if "conversacion" not in datos_usuario:
        datos_usuario["conversacion"] = []

    # Añadir la conversación del usuario
    conversacion_usuario = {
        "usuario_id": usuario_id,
        "ip": datos_usuario.get("ip"),
        "user_agent": datos_usuario.get("user_agent"),
        "sistema_operativo": datos_usuario.get("sistema_operativo"),
        "tipo_usuario": datos_usuario.get("tipo_usuario"),
        "nombre_usuario": datos_usuario.get("nombre_usuario"),
        "username_usuario": usernameR,
        "mensaje": mensaje,
        "timestamp": datos_usuario.get("timestamp")
    }

    # Añadir la respuesta del chatbot a la conversación
    respuesta_chatbot = {
        "usuario_id": usuario_id,
        "username_usuario": usernameR,
        "autor": "Chatbot",
        "respuesta": respuesta,
        "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    # Agregar la conversación del usuario y la respuesta del chatbot a la lista
    completo = {
        "Usuario": [conversacion_usuario],
        "Chatbot": [respuesta_chatbot]
    }
    
    # Enviamos la conversación completa al topic de salida
    productor.send('output_topic', key=usuario_id, value={"respuesta": respuesta_chatbot})
    
    # Enviamos la conversación completa al topic de historial
    historial_productor.send(
        'historial_topic', 
        key=usuario_id, 
        value=completo
    )

    return {"Respuesta": respuesta_chatbot}
"""

mongodb_sink_connector_name = "mongodb-sink"

# Endpoint para recuperar el historial de mensajes de un usuario

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

