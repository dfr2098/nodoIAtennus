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
from kafka.admin import KafkaAdminClient, NewPartitions
from kafka.errors import KafkaError

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
historial_topic = 'historial_topic'  

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


from typing import Set

MIN_NUM_PARTICIONES = 5
claves_distintas: Set[str] = set()

import hashlib

class CustomPartitioner:
    def __init__(self, num_partitions):
        self.num_partitions = num_partitions

    def partition(self, key):
        # Calculamos el hash de la clave
        hash_key = hash(key)
        # Ajustamos el resultado del hash al rango de particiones
        partition = hash_key % self.num_partitions
        # Si el valor de la partición es negativo, lo convertimos a positivo
        if partition < 0:
            partition = -partition
        # Agregamos impresiones de registro para la clave y su hash
        print(f"Clave: {key}, Hash: {hash_key}, Partición: {partition}")
        return partition

def aumentar_particiones_si_es_necesario(topic_name, claves_distintas):
    admin_client = KafkaAdminClient(bootstrap_servers='localhost:9092')
    try:
        topics = admin_client.describe_topics(topic_name)
        if topic_name in topics:
            current_partitions = topics[topic_name].partitions
            num_particiones_actuales = len(current_partitions)
            nuevas_particiones = len(claves_distintas)
            if nuevas_particiones > num_particiones_actuales:
                new_partitions = NewPartitions(total_count=nuevas_particiones)
                admin_client.create_partitions({topic_name: new_partitions})
                print(f"Se crearon {nuevas_particiones - num_particiones_actuales} nuevas particiones para el topic {topic_name}")
    finally:
        admin_client.close()

def asignar_particion_modificada(usuario_id: str) -> int:
    # Calculamos el hash de usuario_id utilizando SHA-256
    hash_usuario_id = hashlib.sha256(usuario_id.encode()).hexdigest()
    # Convertimos el hash a un entero y lo mapeamos al rango de particiones
    particion = int(hash_usuario_id, 16) % MIN_NUM_PARTICIONES
    return particion

# Nueva configuración para el tercer topic de historial
historial_productor = KafkaProducer(
    bootstrap_servers='localhost:9092',
    key_serializer=lambda k: str(k).encode('utf-8'),
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
    partitioner=CustomPartitioner(MIN_NUM_PARTICIONES)
)

# Crear una instancia del particionador personalizado una sola vez
custom_partitioner = CustomPartitioner(MIN_NUM_PARTICIONES)

@app.post("/respuesta_chat/{usuario_id}")
async def obtener_respuesta_chat(usuario_id: str, username: Optional[str] = None):
    num_claves_distintas = len(claves_distintas)
    num_particiones = max(num_claves_distintas, MIN_NUM_PARTICIONES)
    particion = hash(usuario_id) % num_particiones
    
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
    # Antes de enviar el mensaje a Kafka, calcular la partición a la que se asignará la clave
    particion = CustomPartitioner(MIN_NUM_PARTICIONES).partition(key=usuario_id)
    print(f"Clave: {usuario_id}, Partición: {particion}")
    # Luego, enviar el mensaje a Kafka
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

historial_consumidor = KafkaConsumer(bootstrap_servers=bootstrap_servers,
                                   auto_offset_reset='earliest',
                                   key_deserializer=lambda k: k.decode('utf-8'),
                                   value_deserializer=lambda x: json.loads(x.decode('utf-8')))
"""
@app.get("/obtener_historial/{usuario_id}")
async def obtener_historial(usuario_id: str):
    datos_usuario = conversaciones.get(usuario_id)

    if datos_usuario is None:
        raise HTTPException(status_code=404, detail="Token de conversación no válido")

    mensaje = datos_usuario.get("mensaje")
    
    if "conversacion" not in datos_usuario:
        datos_usuario["conversacion"] = []
    
    try:
        # Obtener la partición asignada al usuario
        partition = obtener_particion_usuario(usuario_id)
        
        # Asignar la partición al consumidor
        historial_consumidor.assign([TopicPartition(historial_topic, partition)])
        
        # Filtrar mensajes por el usuario_id y la partición asignada
        mensajes_usuario = []
        for mensaje in historial_consumidor:
            if mensaje.key == usuario_id and mensaje.partition == partition:
                mensajes_usuario.append(mensaje.value)
                
        return mensajes_usuario
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
"""

MAX_MENSAJES = 5  # Definir el número máximo de mensajes del historial

@app.get("/obtener_historial/{usuario_id}")
async def obtener_historial(usuario_id: str):
    try:
        # Obtener la partición asignada al usuario
        particion = obtener_particion_usuario(usuario_id, historial_topic)
        print("la particion para el usuario id "+ usuario_id + " es " + str(particion))
        # Asignar la partición al consumidor
        historial_consumidor.assign([TopicPartition(historial_topic, particion)])

        # Filtrar mensajes por el usuario_id y la partición asignada
        mensajes_usuario = []
        print("entrando en el bucle")
        for mensaje in historial_consumidor:
            if mensaje.key == usuario_id and mensaje.partition == particion:
                # Procesar los datos del usuario
                print("dentro del bucle de historial_consumidor")
                for usuario_datos in mensaje.value["Usuario"]:
                    # Almacenar los datos en un diccionario
                    datos = {
                        "usuario_id": usuario_datos["usuario_id"],
                        "ip": usuario_datos["ip"],
                        "user_agent": usuario_datos["user_agent"],
                        "sistema_operativo": usuario_datos["sistema_operativo"],
                        "tipo_usuario": usuario_datos["tipo_usuario"],
                        "nombre_usuario": usuario_datos["nombre_usuario"],
                        "username_usuario": usuario_datos["username_usuario"],
                        "mensaje": usuario_datos["mensaje"],
                        "timestamp": usuario_datos["timestamp"]
                    }
                    mensajes_usuario.append(datos)

                # Procesar los datos del chatbot
                for chatbot_datos in mensaje.value["Chatbot"]:
                    # Almacenar los datos en un diccionario
                    datos = {
                        "usuario_id": chatbot_datos["usuario_id"],
                        "username_usuario": chatbot_datos["username_usuario"],
                        "autor": chatbot_datos["autor"],
                        "respuesta": chatbot_datos["respuesta"],
                        "timestamp": chatbot_datos["timestamp"]
                    }
                    mensajes_usuario.append(datos)

                # Confirmar que se ha procesado el mensaje
                historial_consumidor.commit()
                print("el commit del consumidor ha finalizado")

                # Detener el bucle después de cierto número de mensajes
                if len(mensajes_usuario) >= MAX_MENSAJES:
                    break

        if not mensajes_usuario:
            raise HTTPException(status_code=404, detail="No se encontró historial para este usuario")

        return mensajes_usuario
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

#función para obtener el numero de particiones de un topic
def obtener_numero_particiones(bootstrap_servers, topic):
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        topic_metadata = admin_client.describe_topics([topic])
        for topic_info in topic_metadata:
            if topic_info['topic'] == topic:
                numero_particiones = len(topic_info['partitions'])
                return numero_particiones
        # Si no se encuentra información sobre el tópico, devuelve None
        return None
    except KafkaError as e:
        print(f"Error al obtener el número de particiones del tópico {topic}: {e}")
        return None

# llamar a la función para obtener el número de particiones del topic
numero_particiones = obtener_numero_particiones('localhost:9092', 'historial_topic')
if numero_particiones is not None:
    print(f"El número de particiones en 'historial_topic' es: {numero_particiones}")
else:
    print("No se pudo obtener el número de particiones del tópico.")


"""
def obtener_particion_usuario(usuario_id: str) -> int:
    # Obtener dinámicamente el número de particiones del topic
    numero_particiones = obtener_numero_particiones('localhost:9092', 'historial_topic')
    if numero_particiones is None:
        # Manejar el caso en que no se pueda obtener el número de particiones
        raise ValueError("No se pudo obtener el número de particiones del tópico.")
    
    # Calcular el hash del usuario_id y asegurarse de que esté dentro del rango de 0 al número de particiones
    particion = hash(usuario_id) % numero_particiones
    return particion
"""

#función para obtener la partición especifica con el usuario_id en x topic
def obtener_particion_usuario(usuario_id: str, topic) -> int:
    try:
        # Obtener las particiones asignadas al usuario_id en el topic
        particiones_asignadas = topic.partitions_for_topic(topic.subscription())
        if particiones_asignadas is None:
            raise ValueError("No se encontraron particiones asignadas para el topic")

        # Convertir las particiones asignadas a una lista para facilitar la manipulación
        particiones_asignadas = list(particiones_asignadas)

        # Iterar sobre las particiones asignadas y verificar en cuál está el usuario_id
        for particion in particiones_asignadas:
            topic_partition = TopicPartition(topic.subscription(), particion)
            topic.assign([topic_partition])
            topic.seek_to_beginning(topic_partition)
            for mensaje in topic:
                if mensaje.key == usuario_id.encode('utf-8'):
                    return particion

        # Si no se encuentra el usuario_id en ninguna partición asignada, lanzar una excepción
        raise ValueError(f"No se encontró el usuario {usuario_id} en ninguna partición asignada")
    except Exception as e:
        print(f"Error al obtener la partición para el usuario {usuario_id}: {e}")
        raise


# Iniciar uvicorn 
if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=8000)