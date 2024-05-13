#Importación de librerías
from enum import Enum
import hashlib
import json
import random
import string
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
import uvicorn
from Chatbot import predecir_clase, obtener_respuesta, intentos
from typing import Optional
import datetime
from uuid import NAMESPACE_URL, UUID, uuid5
from user_agents import parse 
from kafka.admin import KafkaAdminClient, NewPartitions
from kafka.errors import KafkaError
from collections import namedtuple
from typing import Set
import hashlib
from bson import ObjectId
from pymongo import MongoClient
import motor.motor_asyncio
from pymongo.errors import PyMongoError
from fastapi import FastAPI, Depends, HTTPException, status, Request
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from Autentificación import DURACION_TOKEN_ACCESO_EN_MINUTOS, TOKEN_ANONIMO_POR_DEFECTO, obtener_token, autenticar_usuario, obtener_conexion_db, crear_token_acceso, Token
from datetime import timedelta

# Conexión a la base de datos de MongoDB asincrónica
client = motor.motor_asyncio.AsyncIOMotorClient('192.168.1.120', 27018, serverSelectionTimeoutMS=5000, username='dfr209811', password='nostromo987Q_Q')
db = client["tennus_data_analitica"]
conversaciones_coleccion = db["Mensajes"]
respuestas_coleccion = db["Respuestas"]
particiones_coleccion = db["Particiones"]

#Configuración del entorno y de las variables
#Iniciar FastAPI
app = FastAPI()

# URL del servidor Kafka Connect
kafka_connect_url = "http://localhost:8084/connectors"

# Define la configuración de los servidores de Kafka
bootstrap_servers = 'localhost:9092'
historial_topic = 'historial_topic'

# Inicializar el administrador del cluster Kafka
admin_cliente = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

# Obtener una lista de todos los temas
topics = admin_cliente.list_topics()

# Verificar si el topic existe
if historial_topic in topics:
    print(f"El topic '{historial_topic}' existe en el clúster Kafka.")
else:
    print(f"El topic '{historial_topic}' no existe en el clúster Kafka.")

# Imprimir la lista de temas
print(topics)

# Función para convertir ObjectId a cadena
def convertir_object_id_a_cadena(obj):
    if isinstance(obj, ObjectId):
        return str(obj)
    elif isinstance(obj, dict):
        return {k: convertir_object_id_a_cadena(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convertir_object_id_a_cadena(v) for v in obj]
    else:
        return obj

# Inicializar el consumidor de Kafka con la nueva configuración
historial_consumidor = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',  
        enable_auto_commit=True,  
        key_deserializer=lambda k: k,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        group_id='chatbot-group',
        consumer_timeout_ms=1000 # Tiempo máximo de espera para recibir mensajes en milisegundos
    )

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

#Definición de clase y funciones para tipo de usuario
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

# Función asicronica para guardar la conversación en la base de datos
async def guardar_conversacion(conversacion):
    conversacion_convertida = convertir_object_id_a_cadena(conversacion)
    await conversaciones_coleccion.insert_one(conversacion_convertida)
    
# Función asicronica para guardar las respuestas en la base de datos   
async def guardar_respuestas(respuesta):
    respuesta_convertida = convertir_object_id_a_cadena(respuesta)
    await respuestas_coleccion.insert_one(respuesta_convertida)

#Definición de funciones para manejo de particiones
#llama a la colección para hacer una dupla y verifica si realmente no hay datos duplicados con el mismo usuario_id y particion
async def determinar_numero_de_particiones_necesarias(coleccion_base_datos):
    try:
        valores_unicos = set()
        async for documento in coleccion_base_datos.find():
            id_usuario = documento['usuario_id']
            particion = documento['particion']
            valor_combinado = (id_usuario, particion)
            valores_unicos.add(valor_combinado)
        return len(valores_unicos)
    except Exception as e:
        print(f"Error en la operación con MongoDB: {str(e)}")
        return None

#función que se adapta ahora con MongoDB
async def aumentar_particiones_si_es_necesario(nombre_topic, coleccion):
    try:
        topics = admin_cliente.describe_topics([nombre_topic])
        if nombre_topic in topics:
            particiones_actuales = topics[nombre_topic]['partitions']
            num_particiones_actuales = len(particiones_actuales)
            particiones_requeridas = await determinar_numero_de_particiones_necesarias(coleccion)
            if particiones_requeridas is not None and particiones_requeridas > num_particiones_actuales:
                nueva_particion = {"total_count": particiones_requeridas}
                await admin_cliente.create_partitions({nombre_topic: nueva_particion})
                print(f"Se crearon {particiones_requeridas - num_particiones_actuales} nuevas particiones para el topic {nombre_topic}")
    except Exception as e:
        print(f"Error en la operación con Kafka: {str(e)}")
        
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

#Función para obtener la partición de un usuario 
def obtener_particion_usuario(usuario_id: str, historial_consumidor, nombre_topic: str) -> int:
    try:
        # Asignar la partición deseada al consumidor
        particion_deseada = None
        for particion in historial_consumidor.partitions_for_topic(nombre_topic):
            tp = TopicPartition(nombre_topic, particion)
            historial_consumidor.assign([tp])
            print(f"Buscando en la partición {tp.partition} del topic '{nombre_topic}'...")
            historial_consumidor.seek_to_beginning(tp)  # Ir al inicio de la partición
            
            # Iterar sobre los mensajes de la partición actual
            for mensaje in historial_consumidor:
                print(f"Clave del mensaje: {mensaje.key}")
                print(f"ID de usuario: {usuario_id}")
                
                # Convertir la clave del mensaje y el ID de usuario a cadenas antes de comparar
                if mensaje.key.decode('utf-8') == usuario_id:
                    print(f"¡Usuario encontrado en la partición {tp.partition} del topic '{nombre_topic}'!")
                    particion_deseada = tp.partition
                    break  # Salir del bucle una vez que se encuentre el usuario
        
        # Si no se encuentra el usuario en ninguna partición, lanzar una excepción
        if particion_deseada is None:
            raise ValueError(f"No se encontró el usuario {usuario_id} en ninguna partición asignada del topic '{nombre_topic}'.")
        
        return particion_deseada
    
    except Exception as e:
        print(f"Error al obtener la partición para el usuario {usuario_id}: {e}")
        raise


# llamar a la función para obtener el número de particiones del topic
numero_particiones = obtener_numero_particiones('localhost:9092', 'historial_topic')

#En el caso de que no haya ninguna partición en el topic con la función
if numero_particiones is not None:
    MIN_NUM_PARTICIONES = numero_particiones
    print(f"El número de particiones en 'historial_topic' es: {numero_particiones}")
else:
    print("No se pudo encontrar el número de particiones del topic.")
    MIN_NUM_PARTICIONES = 5

# Clase para el particionado customizado
class ParticionadorPersonalizado:
    def __init__(self, num_particiones):
        self.num_particiones = num_particiones

    def particion(self, key):
        # Extrae el usuario_id de 16 dígitos de la clave
        usuario_id = key['usuario_id']
        # Calcula la suma del código ASCII de cada carácter en el usuario_id
        hash_clave = sum(ord(c) for c in usuario_id)
        # Calcula el módulo del hash con el número de particiones
        particion = hash_clave % self.num_particiones
        # Si el valor de la partición es 0, se convierte a 1
        if particion == 0:
            particion = 1
        # Agrega impresiones de registro para la clave, su hash y la partición
        print(f"Clave: {usuario_id}, Hash: {hash_clave}, Partición: {particion}")
        return particion


# Nueva configuración para el tercer topic de historial
historial_productor = KafkaProducer(
    bootstrap_servers='localhost:9092',
    key_serializer=lambda k: str(k).encode('utf-8'),
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
    partitioner=ParticionadorPersonalizado(MIN_NUM_PARTICIONES)
)

# Crear una instancia del particionador personalizado una sola vez
custom_particionador = ParticionadorPersonalizado(MIN_NUM_PARTICIONES)

#Verificar si el usuario y la particion existen en MongoDB, y lo guarda en caso de que no existan aún
async def guardar_unico_documento(nuevo_documento):
    async def encontrar_documento(filtro_consulta):
        return await particiones_coleccion.find_one(filtro_consulta)

    async def actualizar_documento(filtro_consulta, nuevos_valores, upsert=False):
        opciones = {}
        if upsert:
            opciones['upsert'] = True
        return await particiones_coleccion.update_one(filtro_consulta, {'$set': nuevos_valores}, **opciones)

    documento_existente = await encontrar_documento({"usuario_id": nuevo_documento["usuario_id"], "particion": nuevo_documento["particion"]})

    if not documento_existente:
        resultado = await particiones_coleccion.insert_one(nuevo_documento)
        print(f"Documento insertado con ID {resultado.inserted_id}")
    else:
        actualizaciones_contador = await actualizar_documento(
            {"usuario_id": nuevo_documento["usuario_id"], "particion": nuevo_documento["particion"]},
            nuevo_documento,
            upsert=True
        )
        print(f"{actualizaciones_contador.modified_count} documentos actualizados")


#Función para construir el JSON de la conversación 
async def construir_conversacion(request: Request, mensaje: str, tipo_usuario: TipoUsuario, username: str, nombre: str, ip: Optional[str] = None, user_agent: Optional[str] = None):
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


# Variable global para indicar si se debe ejecutar la función eliminar_particion_y_historial
ejecutar_eliminar_particion = False

#middleware que registra si el usuario es anonimo o registrado, si es registrado sigue normal, si es anonimo, a la hora de cerrar la conexión con fastAPI manda a llamar la función para eliminar la partición y el historial de dicho usuario anonimo
@app.middleware("http")
async def determinar_tipo_usuario(request: Request, call_next):
    global ejecutar_eliminar_particion
    
    username = request.query_params.get("username_usuario")
    nombre = request.query_params.get("nombre_usuario")
    
    tipo_usuario = TipoUsuario.Anonimo
    if username and nombre:
        tipo_usuario = TipoUsuario.Registrado

    # Agregar el tipo de usuario al contexto de la solicitud
    request.state.tipo_usuario = tipo_usuario

    # Continuar con el manejo de la solicitud
    response = await call_next(request)

    # Si el tipo de usuario es anónimo y se activó el indicador de ejecución, ejecuta la función de eliminación
    if request.state.tipo_usuario == TipoUsuario.Anonimo and ejecutar_eliminar_particion:
        eliminar_particion_y_historial(usuario_id="usuario_id", historial_consumidor="historial", nombre_topic="nombre_topic", admin_cliente="admin_cliente")

    return response


# Endpoint que administra los tokens de autentificación
@app.post("/Token", response_model=Token)
async def iniciar_sesion_para_obtener_token_acceso(form_data: OAuth2PasswordRequestForm = Depends()):
    usuario = autenticar_usuario(obtener_conexion_db(), form_data.nombre_usuario, form_data.contrasena)
    if not usuario:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="contraseña o nombre de usuario erroneós",
            headers={"WWW-Authenticate": "Bearer"},
        )
    duracion_token_acceso = timedelta(minutes=DURACION_TOKEN_ACCESO_EN_MINUTOS)
    token_acceso = crear_token_acceso(
        datos={"sub": usuario.nombre_usuario}, duracion_delta=duracion_token_acceso
    )
    return {"token_acceso": token_acceso, "tipo_token": "bearer"}

#endpoint para manejar sesiones
@app.post("/Sesión")
async def manejar_sesion(request: Request, token: str = Depends(obtener_token)):
    if token == TOKEN_ANONIMO_POR_DEFECTO:
        tipo_usuario = TipoUsuario.Anonimo
    else:
        tipo_usuario = TipoUsuario.Registrado

    # Aquí se puede almacenar información sobre la sesión del usuario en una base de datos o en una variable de sesión
    tipo_usuario = request.state.tipo_usuario

    if tipo_usuario == TipoUsuario.Anonimo:
        # Generar el nombre y el username del usuario anónimo
        nombre_usuario = generar_nombre()
        username_usuario = generar_username()

        # Almacena la información del usuario anónimo en una variable de sesión
        request.session["nombre_usuario"] = nombre_usuario
        request.session["username_usuario"] = username_usuario

        # Permite que el usuario anónimo use el chat
        return {"mensaje": f"Bienvenido, {tipo_usuario.value}!"}
    elif tipo_usuario == TipoUsuario.Registrado:
        # El usuario es registrado, realiza las acciones necesarias
        pass

    return {"mensaje": f"Bienvenido, {tipo_usuario.value}!"}


#Endpoint para recibir la respuesta del usuario
@app.get("/Chat")
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
    conversacion = await construir_conversacion(request, mensaje, tipo_usuario, username, nombre, ip, user_agent)
    
    # Obtener el usuario_id de la conversación
    usuario_id = conversacion["usuario_id"]
    
    # Obtener el tipo de usuario del contexto de la solicitud
    tipo_usuario = request.state.tipo_usuario
    
    # Guardar la conversación en la base de datos
    await guardar_conversacion(conversacion)
    
    # Enviamos la solicitud al chatbot
    productor.send('input_topic', key=usuario_id, value={"conversación": conversacion})
    
    return {"conversación": conversacion}


#Endpoint para recibir la respuesta del chatbot
@app.post("/Respuesta_chat/{usuario_id}")
async def obtener_respuesta_chat(usuario_id: str):
    
    particion = custom_particionador.particion({"usuario_id": usuario_id})
    
    # Obtener los datos del usuario de la base de datos MongoDB
    datos_usuario = await conversaciones_coleccion.find_one(
        {"usuario_id": usuario_id},
        sort=[("timestamp", -1)],
        limit=1
    )
    
    if datos_usuario is None:
        raise HTTPException(status_code=404, detail="Token de conversación no válido")

    mensaje = datos_usuario.get("mensaje")
    usernameR = datos_usuario.get("username_usuario")

    intento = predecir_clase(mensaje)
    respuesta = obtener_respuesta(intento, intentos)
    
    if "conversacion" not in datos_usuario:
        datos_usuario["conversacion"] = []

    # Solo guardar la partición en la base de datos si el usuario es registrado
    if datos_usuario.get("tipo_usuario") == TipoUsuario.Registrado:
        particion_usuario = {
            "usuario_id": usuario_id,
            "tipo_usuario": datos_usuario.get("tipo_usuario"),
            "nombre_usuario": datos_usuario.get("nombre_usuario"),
            "username_usuario": usernameR,
            "particion": particion
        }

        # Guardar la particion en la base de datos
        await guardar_unico_documento(particion_usuario)

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
    
    await aumentar_particiones_si_es_necesario("historial_topic", particiones_coleccion)
    
    # Antes de enviar el mensaje a Kafka, calcular la partición a la que se asignará la clave
    print(f"Clave: {usuario_id}, Partición: {particion}")
    
    # Guardar la respuesta en la base de datos
    await guardar_respuestas(respuesta_chatbot)
    
    # Luego, enviar el historial a Kafka
    historial_productor.send('historial_topic', key=usuario_id, value=completo, partition=particion)
    
    return {"Respuesta": respuesta_chatbot}


#Endpoint para obtener el historial de cierto usuario por su "usuario_id"
@app.get("/Obtener_historial/{usuario_id}")
async def obtener_historial(usuario_id: str):
    try:
        # Obtener la partición asignada al usuario
        particion = obtener_particion_usuario(usuario_id, historial_consumidor, historial_topic)
        print("La partición para el usuario id " + usuario_id + " es " + str(particion))
        
        # Asignar la partición al consumidor
        historial_consumidor.assign([TopicPartition(historial_topic, particion)])
        
        # Reiniciar el offset al principio de la partición correspondiente
        particion_objeto = TopicPartition(historial_topic, particion)
        historial_consumidor.seek(particion_objeto, 0)
        
        # Filtrar mensajes por el usuario_id y la partición asignada
        mensajes_usuario = []
        print("Entrando en el bucle")
        
        # Lista para almacenar los mensajes sin procesar
        mensajes_sin_procesar = []
        
        for mensaje in historial_consumidor:
            mensaje_id = mensaje.key.decode('utf-8')
            
            if mensaje_id == usuario_id and mensaje.partition == particion:
                mensajes_sin_procesar.append(mensaje)
                
                # Detener el bucle después de cierto número de mensajes
                if len(mensajes_sin_procesar) >= MIN_NUM_PARTICIONES:
                    break

        # Ordenar los mensajes por offset de mayor a menor
        mensajes_sin_procesar.sort(key=lambda x: x.offset, reverse=True)

        # Procesar los mensajes ordenados
        for mensaje in mensajes_sin_procesar:
            for usuario_datos in mensaje.value["Usuario"]:
                datos_usuario = {
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
                mensajes_usuario.append(datos_usuario)

            for chatbot_datos in mensaje.value["Chatbot"]:
                datos_chatbot = {
                    "usuario_id": chatbot_datos["usuario_id"],
                    "username_usuario": chatbot_datos["username_usuario"],
                    "autor": chatbot_datos["autor"],
                    "respuesta": chatbot_datos["respuesta"],
                    "timestamp": chatbot_datos["timestamp"]
                }
                mensajes_usuario.append(datos_chatbot)

        if not mensajes_usuario:
            raise HTTPException(status_code=404, detail="No se encontró historial para este usuario")

        return mensajes_usuario

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

#Función que elimina la partición y el historial de los usuarios tipo anonimo 
def eliminar_particion_y_historial(usuario_id: str, historial_consumidor, nombre_topic: str, admin_cliente):
    try:
        # Obtener la partición del usuario
        particion_a_eliminar = obtener_particion_usuario(usuario_id, historial_consumidor, nombre_topic)

        # Eliminar la partición
        admin_cliente.delete_topics([nombre_topic + "-" + str(particion_a_eliminar)], operation_timeout=30)
        print(f"Partición {particion_a_eliminar} eliminada con éxito.")

        # Flushear los mensajes para asegurarse de que se envíen
        historial_productor.flush()
    
    except Exception as e:
        print(f"Error al eliminar la partición y el historial para el usuario {usuario_id}: {e}")
        raise

# Iniciar uvicorn 
if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=8000)

