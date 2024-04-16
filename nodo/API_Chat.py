import json
from kafka import KafkaConsumer, KafkaProducer
import uvicorn
from Chatbot import predecir_clase, obtener_respuesta, intentos
from fastapi import FastAPI, Request
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


# URL del servidor Kafka Connect
kafka_connect_url = "http://localhost:8084/connectors"

# Configuración del conector MongoDB
mongodb_sink_config = {
    "name": "mongodb-sink",
    "config": {
        "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
        "tasks.max": "3", #tareas maximas a procesar, siempre debe ser igual o menor que las particiones del topic de kafka
        "topics": "input_topic,output_topic",
        "connection.uri": "mongodb://dfr209811:nostromo987Q_Q@192.168.1.120:27018",
        "database": "tennus_data_analitica",
        "collection": "Mensajes",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "true",
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


app = FastAPI()

def generar_uuid(username):
    namespace = NAMESPACE_URL
    return str(uuid5(namespace, username))

# Diccionario para almacenar las conversaciones asociadas con el usuario_id
conversaciones = {}

# Diccionario para almacenar el último mensaje de cada usuario_id
ultimo_mensaje_por_usuario = {}

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

@app.get("/respuesta_ultima_chat/{usuario_id}")
async def obtener_utima_respuesta_chat(usuario_id: str):
    # Buscar el último mensaje del usuario asociado con el usuario_id en el topic de salida
    ultima_respuesta = None
    for message in output_consumidor:
        respuesta = json.loads(message.value)["respuesta"]
        if respuesta["usuario_id"] == usuario_id:
            ultima_respuesta = respuesta
            # Avanzar el offset para evitar procesar mensajes antiguos en futuras solicitudes
            output_consumidor.commit()
    
    if ultima_respuesta is None:
        return {"error": "No se encontró una respuesta para el usuario"}

    return {"Respuesta": ultima_respuesta}

from kafka.errors import KafkaTimeoutError

@app.get("/respuesta_chat_correg/{usuario_id}")
async def obtener_respuesta_chat_correg(usuario_id: str):
    # Esperar mensajes del topic de salida con un tiempo de espera
    try:
        output_records = output_consumidor.poll(timeout_ms=1000)
    except KafkaTimeoutError:
        return {"error": "Se ha agotado el tiempo de espera de Kafka"}

    # Verificar si se recibieron nuevos registros
    if not output_records:
        return {"error": "No se encontraron nuevas respuestas"}

    # Buscar la última respuesta para el usuario_id dado
    ultima_respuesta = None
    for tp, records in output_records.items():
        for record in records:
            respuesta = json.loads(record.value)["respuesta"]
            if respuesta["usuario_id"] == usuario_id:
                ultima_respuesta = respuesta
                break

    # Verificar si se encontró la última respuesta
    if ultima_respuesta is None:
        return {"error": "No se encontró la última respuesta para el usuario"}

    return {"Respuesta": ultima_respuesta}
"""

# Iniciar uvicorn 

if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=8000)

