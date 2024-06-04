#Importación de librerías
from enum import Enum
import json
import random
import string
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
import uvicorn
from Chatbot import predecir_clase, obtener_respuesta, intentos
from typing import Optional
import datetime
from uuid import UUID, uuid5
from user_agents import parse 
from kafka.admin import KafkaAdminClient, NewPartitions
from kafka.errors import KafkaError
from bson import ObjectId
import motor.motor_asyncio
from fastapi import FastAPI, Depends, HTTPException, status, Request, Form
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from Autentificacion import (
    autenticar_usuario,
    crear_token_acceso,
    obtener_conexion_db,
    DURACION_TOKEN_ACCESO_EN_MINUTOS,
    obtener_hash_contrasena,
    CLAVE_SECRETA, ALGORITMO, obtener_usuario_por_identificador, verificar_contrasena,
    actualizar_datos_usuario, obtener_contrasena_usuario
)
from datetime import timedelta
from pydantic import BaseModel, EmailStr, Field, ValidationError, model_validator
from email_validator import validate_email, EmailNotValidError
from fastapi.responses import JSONResponse
import re
from typing import Annotated, Union
import jwt 
from jwt import PyJWTError
from jose import JWTError
from functools import lru_cache
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
import language_tool_python

# Inicializa el corrector ortográfico
tool = language_tool_python.LanguageTool('es-ES')

# Conexión a la base de datos de MongoDB asincrónica
client = motor.motor_asyncio.AsyncIOMotorClient('192.168.1.120', 27018, serverSelectionTimeoutMS=5000, username='dfr209811', password='nostromo987Q_Q')
db = client["tennus_data_analitica"]
conversaciones_coleccion = db["Mensajes"]
respuestas_coleccion = db["Respuestas"]
particiones_coleccion = db["Particiones"]
cache_coleccion =  db["Cache"]

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

# Esquema de OAuth2 para validar tokens de acceso JWT
esquemaa_oauth2 = OAuth2PasswordBearer(tokenUrl="/Iniciar_Sesion")

# Tamaño máximo de la caché local (puedes ajustarlo según las necesidades)
MAX_LOCAL_CACHE_SIZE = 1024
local_cache = {}

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


def generar_uuid(username):
    namespace = UUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8')
    return str(uuid5(namespace, username))

def generar_username():
    return ''.join(random.choices(string.ascii_letters + string.digits, k=10))

def generar_nombre():
    return "Anonimo"

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
    
#Definición de clase y funciones para tipo de usuario
class TipoUsuario(str, Enum):
    Registrado = "Registrado"
    Anonimo = "Anonimo"

class UsuarioCreado(BaseModel):
    nombre_usuario: str
    contrasena: str
    correo: EmailStr
    user_name: str
    id_nombre_usuario: str
    ip_usuario: str = None

class DatosUsuario(BaseModel):
    nombre_usuario: str
    contrasena: str
    email: EmailStr
    user_name: str
    id_user_name: str
    ip_usuario: str = None
    
class Token(BaseModel):
    access_token: str
    token_type: str
    nombre_usuario: Optional[str] = None
    tipo_usuario: str
    id_user_name: str
    
class DatosUsuario(BaseModel):
    nombre_usuario: str
    contrasena: str
    email: EmailStr
    user_name: str
    id_user_name: str
    ip_usuario: Optional[str] = None

# Definición del modelo de datos del usuario
class DatosU(BaseModel):
    nombre_usuario: str
    correo_electronico: str
    user_name: str
    id_user_name: str
    tipo_usuario: str

class TokenAnonimo(BaseModel):
    access_token: str
    token_type: str
    tipo_usuario: str
    nombre_usuario: str
    user_name: str
    id_user_name: str
    mensaje: str
    
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

async def get_cache(token: str) -> dict:
    """
    Función para obtener la caché de MongoDB basada en el token del usuario.
    """
    # Buscar en la base de datos MongoDB la caché asociada al token
    cache_data = await cache_coleccion.find_one({"token": token})
    if cache_data:
        return cache_data
    else:
        return {}

# Función para obtener usuario o token con caché manual
async def obtener_usuario_o_token(token: str = Depends(esquemaa_oauth2)):
    try:
        # Verificar si los datos del token están en la caché local
        if token in local_cache:
            cache_data = local_cache[token]
        else:
            # Si no está en la caché local, obtener de MongoDB y actualizar la caché local
            cache_data = await get_cache(token)
            local_cache[token] = cache_data

        # Si los datos del token están en la caché, devolvemos los datos de la caché
        if cache_data:
            if cache_data.get("tipo_usuario") == TipoUsuario.Registrado:
                yield DatosU(**cache_data.get("usuario_registrado"))
            elif cache_data.get("tipo_usuario") == TipoUsuario.Anonimo:
                yield TokenAnonimo(**cache_data)
        else:
            # Si los datos del token no están en la caché, procedemos a decodificar el token
            payload = jwt.decode(token, CLAVE_SECRETA, algorithms=[ALGORITMO])
            tipo_usuario_valor = payload.get("tipo_usuario")
            tipo_usuario = TipoUsuario(tipo_usuario_valor) if tipo_usuario_valor else None

            # Si el tipo de usuario es Registrado
            if tipo_usuario == TipoUsuario.Registrado:
                nombre_usuario = payload.get("sub")
                usuario_registrado = await obtener_usuario_por_identificador(nombre_usuario)
                if usuario_registrado:
                    yield DatosU(
                        nombre_usuario=usuario_registrado.nombre_usuario,
                        correo_electronico=usuario_registrado.correo_electronico,
                        user_name=usuario_registrado.user_name,
                        id_user_name=usuario_registrado.id_user_name,
                        tipo_usuario=usuario_registrado.tipo_usuario
                    )

                    # Convertir la instancia de Pydantic a un diccionario
                    usuario_registrado_dict = usuario_registrado.model_dump()

                    # Actualizar la caché en MongoDB
                    try:
                        await cache_coleccion.update_one(
                            {"token": token},
                            {"$set": {"tipo_usuario": TipoUsuario.Registrado, "usuario_registrado": usuario_registrado_dict}},
                            upsert=True
                        )
                    except Exception as e:
                        print(f"Error al actualizar la caché: {e}")
                else:
                    raise HTTPException(status_code=404, detail="Usuario no encontrado")

            # Si el tipo de usuario es Anonimo
            elif tipo_usuario == TipoUsuario.Anonimo:
                # Comprobar si ya existe un token anónimo para el mismo identificador de usuario anónimo
                nombre_usuario = payload.get("nombre_usuario")
                user_name = payload.get("user_name")
                id_user_name = payload.get("id_user_name")
                
                # Buscar si existe ya un token con estos datos
                existing_token = await cache_coleccion.find_one({
                    "tipo_usuario": TipoUsuario.Anonimo,
                    "nombre_usuario": nombre_usuario,
                    "user_name": user_name,
                    "id_user_name": id_user_name
                })

                if existing_token:
                    yield TokenAnonimo(**existing_token)
                else:
                    # Si no existe, crear uno nuevo
                    nuevo_token_anonimo = TokenAnonimo(
                        access_token=token,
                        token_type="bearer",
                        tipo_usuario="Anonimo",
                        nombre_usuario=nombre_usuario,
                        user_name=user_name,
                        id_user_name=id_user_name,
                        mensaje="Bienvenido, usuario anónimo!"
                    )
                    yield nuevo_token_anonimo

                    # Actualizar la caché en MongoDB con el nuevo token anónimo
                    await cache_coleccion.update_one(
                        {"token": token},
                        {"$set": {"tipo_usuario": TipoUsuario.Anonimo, "nombre_usuario": nombre_usuario, "user_name": user_name, "id_user_name": id_user_name}},
                        upsert=True
                    )
            else:
                raise HTTPException(status_code=400, detail="Tipo de usuario desconocido en el token.")

    except JWTError:
        raise HTTPException(status_code=401, detail="Token inválido")

#Función que elimina la partición y el historial de los usuarios tipo anonimo 
async def eliminar_particion_y_historial(usuario_id: str, historial_consumidor, nombre_topic: str, admin_cliente):
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

async def eliminar_cache():
    """
    Elimina todos los documentos de la colección de caché en MongoDB.
    """
    await cache_coleccion.delete_many({})


async def eliminar_cache_usuario(token: str):
    """
    Elimina el documento de la colección de caché en MongoDB correspondiente al token proporcionado.
    """
    await cache_coleccion.delete_one({'token': token})


async def cerrar_sesion(request: Request, token: str = Depends(esquemaa_oauth2)):
    try:
        # Crear el generador asíncrono para obtener el usuario o los datos del token
        usuario_o_token_generator = obtener_usuario_o_token(token)

        # Iterar sobre el generador para obtener el usuario o token
        usuario_o_token = None
        async for item in usuario_o_token_generator:
            usuario_o_token = item
            break

        if usuario_o_token is None:
            raise HTTPException(status_code=400, detail="Error al obtener el usuario o los datos del token.")

        # Verificar si el usuario es anónimo o registrado
        if isinstance(usuario_o_token, TokenAnonimo):
            # Eliminar la partición y el historial del usuario anónimo
            usuario_id = usuario_o_token.id_user_name
            # Invalidar el token anónimo
            await eliminar_cache_usuario(token)
            await eliminar_particion_y_historial(usuario_id, historial_consumidor, historial_topic, admin_cliente)

        elif isinstance(usuario_o_token, DatosU):
            # Invalidar el token de usuario registrado
            await eliminar_cache_usuario(token)

        else:
            raise HTTPException(status_code=400, detail="Tipo de usuario desconocido en el token.")

        # Devolver un mensaje de éxito en lugar de lanzar una excepción
        response = JSONResponse(
            status_code=status.HTTP_200_OK,
            content={"detail": "Sesión cerrada exitosamente"}
        )

        return response

    except PyJWTError:
        raise HTTPException(status_code=401, detail="Token inválido")

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
        await eliminar_particion_y_historial(usuario_id="usuario_id", historial_consumidor="historial", nombre_topic="nombre_topic", admin_cliente="admin_cliente")

    return response

class VerificarUsuarioAnonimoMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Extraer el token del encabezado Authorization
        auth_header = request.headers.get("Authorization")
        if auth_header and auth_header.startswith("Bearer "):
            token = auth_header.split(" ")[1]
            # Crear el generador asíncrono para obtener el usuario o los datos del token
            usuario_o_token_generator = obtener_usuario_o_token(token)
            
            # Iterar sobre el generador para obtener el usuario o token
            usuario_o_token = None
            async for item in usuario_o_token_generator:
                usuario_o_token = item
                break
            
            # Verificar si el usuario es anónimo
            if isinstance(usuario_o_token, TokenAnonimo):
                usuario_id = usuario_o_token.id_user_name
                # Eliminar la partición y el historial del usuario anónimo
                await eliminar_particion_y_historial(usuario_id, historial_consumidor, historial_topic, admin_cliente)
        
        response = await call_next(request)
        return response

# Añadir el middleware a la aplicación FastAPI
app.add_middleware(VerificarUsuarioAnonimoMiddleware)
 
def lanzar_excepcion_personalizada(mensaje: str):
    raise HTTPException(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        detail=mensaje
    )

class RegistroUsuario(BaseModel):
    nombre_usuario: str = Field(..., min_length=5, max_length=20, description="El nombre de usuario debe tener entre 5 y 20 caracteres")
    contrasena: str = Field(..., min_length=5, max_length=15, description="La contraseña debe tener entre 5 y 15 caracteres")
    confirmar_contrasena: str = Field(..., min_length=5, max_length=15, description="Repetir contraseña")
    email: EmailStr = Field(..., description="Ingresa un correo")
    user_name: str = Field(..., min_length=1, max_length=15, description="El username debe tener entre 1 y 15 caracteres y no puede contener espacios en blanco")

    @model_validator(mode='before')
    def validar_campos(cls, values):
        nombre_usuario = values.get('nombre_usuario')
        contrasena = values.get('contrasena')
        confirmar_contrasena = values.get('confirmar_contrasena')
        email = values.get('email')
        user_name = values.get('user_name')
        
        if len(nombre_usuario) < 5 or len(nombre_usuario) > 20:
            lanzar_excepcion_personalizada("El nombre de usuario debe tener entre 5 y 20 caracteres")
        
        if len(contrasena) < 5 or len(contrasena) > 15:
            lanzar_excepcion_personalizada("La contraseña debe tener entre 5 y 15 caracteres")
        
        if contrasena != confirmar_contrasena:
            lanzar_excepcion_personalizada("Las contraseñas no coinciden")
        
        if len(user_name) < 1 or len(user_name) > 15:
            lanzar_excepcion_personalizada("El username debe tener entre 1 y 15 caracteres")
        
        if re.search(r'\s', user_name):
            lanzar_excepcion_personalizada("El username no puede contener espacios en blanco")
        
        try:
            validate_email(email)
        except EmailNotValidError:
            lanzar_excepcion_personalizada("El correo proporcionado no es correcto")
        
        return values

@app.post("/Registro")
async def crear_usuario(
    request: Request,
    nombre_usuario: str = Form(..., min_length=5, max_length=20, description="El nombre de usuario debe tener entre 5 y 20 caracteres"),
    contrasena: str = Form(..., min_length=5, max_length=15, description="La contraseña debe tener entre 5 y 15 caracteres"),
    confirmar_contrasena: str = Form(..., min_length=5, max_length=15, description="Repetir contraseña"),
    email: EmailStr = Form(..., description="Ingresa un correo"),
    user_name: str = Form(..., pattern=r'^\S+$', min_length=1, max_length=15, description="El username es único y no se puede cambiar, debe tener entre 1 y 15 caracteres y no puede contener espacios en blanco")
):

    try:
        RegistroUsuario(
            nombre_usuario=nombre_usuario,
            contrasena=contrasena,
            confirmar_contrasena=confirmar_contrasena,
            email=email,
            user_name=user_name
        )
    except HTTPException as e:
        raise e
    except ValidationError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=e.errors()
        )

    # Obtén la dirección IP del usuario
    ip_usuario = request.client.host if request.client.host else None

    # Verifica si el usuario ya existe en la base de datos
    conexion_db = await obtener_conexion_db()
    cursor = await conexion_db.cursor()
    await cursor.execute("SELECT * FROM usuarios WHERE user_name = %s", (user_name,))
    username_existente = await cursor.fetchone()
    await cursor.close()

    if username_existente:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="El nombre de usuario ya está en uso",
        )

    # Verifica si el correo electrónico ya existe en la base de datos
    conexion_db = await obtener_conexion_db()
    cursor = await conexion_db.cursor()
    await cursor.execute("SELECT * FROM usuarios WHERE correo_electronico = %s", (email,))
    email_existente = await cursor.fetchone()
    await cursor.close()

    if email_existente:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="El correo electrónico ya está en uso",
        )

    # Crea un hash de la contraseña
    contrasena_cifrada = await obtener_hash_contrasena(contrasena)

    # Genera un UUID para el campo id_user_name
    id_user_name = generar_uuid(user_name)

    # Almacena el usuario en la base de datos con tipo_usuario como "Registrado"
    conexion_db = await obtener_conexion_db()
    cursor = await conexion_db.cursor()
    await cursor.execute(
        "INSERT INTO usuarios (user_name, nombre_usuario, contrasena, correo_electronico, id_user_name, tipo_usuario, ip_usuario) VALUES (%s, %s, %s, %s, %s, %s, %s)",
        (user_name, nombre_usuario, contrasena_cifrada, email, id_user_name, TipoUsuario.Registrado.value, ip_usuario),
    )
    await conexion_db.commit()
    await cursor.close()

    # Genera un token de acceso para el usuario recién creado
    duracion_token_acceso = timedelta(minutes=DURACION_TOKEN_ACCESO_EN_MINUTOS)
    datos_token = {"sub": user_name}
    # Devolver el nombre del usuario junto con el tipo de usuario
    datos_token.update({"nombre_usuario": nombre_usuario, "tipo_usuario": "Registrado"})
    token_acceso = await crear_token_acceso(datos=datos_token, duracion_delta=duracion_token_acceso)

    # Devuelve el usuario creado, el id_user_name y el token de acceso
    datos_usuario = DatosUsuario(
        nombre_usuario=nombre_usuario,
        contrasena="****",  # Proporciona un valor de marcador de posición para el campo de contraseña
        email=email,
        user_name=user_name,
        id_user_name=id_user_name,
        ip_usuario=ip_usuario
    )
    
    return {
        "usuario": datos_usuario,
        "id_user_name": id_user_name,
        "access_token": token_acceso,
        "type_token": "bearer",
        "tipo_usuario":TipoUsuario.Registrado
    }
        

@app.post("/Iniciar_Sesion", response_model=Token)
async def obtener_token_acceso(form_data: OAuth2PasswordRequestForm = Depends(), request: Request = None) -> Token:
    nombre_usuario_o_correo = form_data.username  # Extraer el nombre de usuario o correo del form_data
    contrasena = form_data.password  # Extraer la contraseña del form_data

    if nombre_usuario_o_correo is not None and contrasena is not None:
        # Se proporcionaron credenciales de usuario, verificarlas
        usuario_valido = await autenticar_usuario( await obtener_conexion_db(), nombre_usuario_o_correo, contrasena)
        if not usuario_valido:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Usuario o contraseña incorrectos",
                headers={"WWW-Authenticate": "Bearer"},
            )
        # Si las credenciales son válidas, generar un token de acceso
        datos_token = {"sub": nombre_usuario_o_correo}
        tipo_usuario = TipoUsuario.Registrado  # Actualizar el tipo de usuario

        # Obtener el nombre del usuario registrado
        nombre_usuario = usuario_valido['user_name']  # Aquí se reemplaza con el campo correcto de la base de datos
        id_user_name = usuario_valido['id_user_name']

        # Devolver el nombre del usuario junto con el tipo de usuario
        datos_token.update({"nombre_usuario": nombre_usuario, "tipo_usuario": "Registrado", "id_user_name": id_user_name})
    else:
        # En caso de que falte cualquiera de las credenciales, lanzar una excepción
        if nombre_usuario_o_correo is not None:
            # Se proporcionó solo nombre de usuario o correo
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Por favor, ingresa también tu contraseña",
            )
        elif contrasena is not None:
            # Se proporcionó solo contraseña
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Por favor, ingresa tu correo o nombre de usuario",
            )
        else:
            # No se proporcionaron credenciales
            print("Ingresa tu usuario y contraseña")

    # Generar y devolver el token de acceso
    duracion_token_acceso = timedelta(minutes=DURACION_TOKEN_ACCESO_EN_MINUTOS)
    token_acceso = await crear_token_acceso(datos=datos_token, duracion_delta=duracion_token_acceso)

    # Almacena el tipo de usuario en el estado de la solicitud
    request.state.tipo_usuario = tipo_usuario

    return Token(
        access_token=token_acceso,
        token_type="bearer",
        nombre_usuario=nombre_usuario,
        tipo_usuario= TipoUsuario.Registrado,
        id_user_name = id_user_name
    )


@app.get("/usuarios/me", operation_id="obtener_usu")
async def obtener_usu(token: str = Depends(esquemaa_oauth2)):
        try:
            carga_util = jwt.decode(token, CLAVE_SECRETA, algorithms=[ALGORITMO])
            username = carga_util.get("sub")

            if username is None:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="No se pudieron validar las credenciales",
                    headers={"WWW-Authenticate": "Bearer"}
                )

            usuario = await obtener_usuario_por_identificador(username)
            if usuario is None:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Usuario no encontrado",
                    headers={"WWW-Authenticate": "Bearer"}
                )

            return usuario

        except JWTError:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="No se pudieron validar las credenciales",
                headers={"WWW-Authenticate": "Bearer"}
            )
        except jwt.exceptions.DecodeError as e:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Error decodificando token: {}".format(str(e)),
                headers={"WWW-Authenticate": "Bearer"}
            )


async def obtener_usuario_activo(current_user: Annotated[DatosU, Depends(obtener_usu)]):
    if current_user.disabled:
        raise HTTPException(status_code=400, detail="Usuario inactivo")
    return current_user


@app.post("/Sesión_Anónima", response_model=TokenAnonimo)
async def obtener_token_para_anonimos(request: Request) -> TokenAnonimo:
    tipo_usuario = TipoUsuario.Anonimo
    datos_token = {"sub": tipo_usuario.Anonimo}

    nombre_usuario_anonimo = generar_nombre()
    username_usuario_anonimo = generar_username()
    id_user_name_anonimo = generar_uuid(username_usuario_anonimo)

    datos_token.update({
        "nombre_usuario": nombre_usuario_anonimo,
        "user_name": username_usuario_anonimo,
        "id_user_name": id_user_name_anonimo,
        "tipo_usuario": "Anonimo"
    })

    duracion_token_acceso = timedelta(minutes=DURACION_TOKEN_ACCESO_EN_MINUTOS)
    token_acceso = await crear_token_acceso(datos=datos_token, duracion_delta=duracion_token_acceso)

    request.state.tipo_usuario = tipo_usuario

    return TokenAnonimo(
        access_token = token_acceso,
        token_type= "bearer",
        tipo_usuario= "Anonimo",
        nombre_usuario= nombre_usuario_anonimo,
        user_name= username_usuario_anonimo,
        id_user_name= id_user_name_anonimo,
        mensaje= f"Bienvenido, {tipo_usuario.value}!"
    )


@app.post("/Cerrar_Sesion")
async def manejar_cerrar_sesion(request: Request, token: str = Depends(esquemaa_oauth2)):
    return await cerrar_sesion(request=request, token=token)


# Endpoint para editar una cuenta
@app.put("/Editar_Cuenta/{id_usuario}")
async def editar_cuenta(
    request: Request,
    nombre_usuario: Optional[str] = Form(None, min_length=5, max_length=20, description="El nombre de usuario debe tener entre 5 y 20 caracteres"),
    contrasena_actual: str = Form(..., min_length=5, max_length=15, description="La contraseña actual"),
    nueva_contrasena: Optional[str] = Form(None, min_length=5, max_length=15, description="La nueva contraseña"),
    confirmar_contrasena: Optional[str] = Form(None, min_length=5, max_length=15, description="Repetir la nueva contraseña"),
    email: Optional[EmailStr] = Form(None, description="Ingresa un correo"),
    usuario: DatosU = Depends(obtener_usuario_o_token)
):
    # Verificar si el usuario es registrado
    if usuario.tipo_usuario != TipoUsuario.Registrado:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Acceso denegado",
        )
        
    id_usuario = usuario.id_user_name
    # Verificar si el usuario existe en la base de datos
    contrasena_hasheada = await obtener_contrasena_usuario(id_usuario)

    # Verificar si la contraseña actual es correcta
    if contrasena_actual and not await verificar_contrasena(contrasena_actual, contrasena_hasheada):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="La contraseña actual es incorrecta",
        )

    # Verificar si la nueva contraseña y la confirmación de la contraseña coinciden
    if nueva_contrasena and confirmar_contrasena and nueva_contrasena != confirmar_contrasena:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="La nueva contraseña y la confirmación de la contraseña no coinciden",
        )

    # Verificar si al menos uno de los parámetros es diferente de None
    if all(v is None for v in [nombre_usuario, nueva_contrasena, email]):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Debe proporcionar al menos un parámetro para actualizar",
        )

    # Verificar si el email ya existe en la base de datos
    if email:
        # Obtener la conexión a la base de datos
        conexion_db = await obtener_conexion_db()

        try:
            async with conexion_db.cursor() as cursor:
                await cursor.execute("SELECT COUNT(*) FROM usuarios WHERE correo_electronico = %s AND id_user_name != %s", (email, id_usuario))
                count = await cursor.fetchone()
                if count[0] > 0:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail="El correo electrónico ya está en uso",
                    )
        finally:
            # Cerrar la conexión a la base de datos
            conexion_db.close()
           
    # Actualizar los datos del usuario en la base de datos
    await actualizar_datos_usuario(id_usuario, contrasena_actual, nombre_usuario, nueva_contrasena, email)
    
    user_name = DatosU.user_name

    # Generar un nuevo token de acceso
    duracion_token_acceso = timedelta(minutes=DURACION_TOKEN_ACCESO_EN_MINUTOS)
    datos_token = {"sub": user_name}
    datos_token.update({"nombre_usuario": nombre_usuario, "tipo_usuario": "Registrado"})
    token_acceso = await crear_token_acceso(datos=datos_token, duracion_delta=duracion_token_acceso)

    # Devolver la respuesta con los datos actualizados del usuario y el token de acceso
    return {
        "access_token": token_acceso,
        "type_token": "bearer",
        "tipo_usuario": TipoUsuario.Registrado
    }   
    
@app.get("/Chat")
async def chat(
    request: Request,
    mensaje: str,
    ip: Optional[str] = None,
    user_agent: Optional[str] = None,
    usuario: Union[DatosU, TokenAnonimo] = Depends(obtener_usuario_o_token)
):
    print(type(usuario))
    if isinstance(usuario, DatosU):
        tipo_usuario = TipoUsuario.Registrado
        username = usuario.user_name
        nombre = usuario.nombre_usuario
        id_user_name = usuario.id_user_name

        # Imprime la instancia completa (opcional)
        print(f"Usuario Registrado: {usuario}")  # Imprime toda la instancia DatosU

        # Imprime atributos específicos
        print(f"Tipo usuario: {tipo_usuario}")
        print(f"Nombre de usuario: {username}")
        print(f"Nombre completo: {nombre}")
        print(f"id_user_name: {id_user_name}")

    elif isinstance(usuario, TokenAnonimo):
        print("Instancia Anónima")
        tipo_usuario = TipoUsuario.Anonimo
        username = usuario.user_name
        nombre = usuario.nombre_usuario
        id_user_name = usuario.id_user_name

        # Imprime la instancia completa (opcional)
        print(f"Usuario Anónimo: {usuario}")  # Imprime toda la instancia TokenAnonimo

        # Imprime atributos específicos
        print(f"Nombre de usuario: {username}")
        print(f"Nombre completo: {nombre}")
        print(f"id_user_name: {id_user_name}")

    else:
        raise HTTPException(status_code=400, detail="Tipo de usuario desconocido.")

    conversacion = await construir_conversacion(request, mensaje, tipo_usuario, username, nombre, ip, user_agent)
    usuario_id = conversacion["usuario_id"]
    await guardar_conversacion(conversacion)
    productor.send('input_topic', key=usuario_id, value={"conversación": conversacion})

    return {"conversación": conversacion}


@app.post("/Respuesta_chat")
async def obtener_respuesta_chat(current_user: Union[DatosU, TokenAnonimo] = Depends(obtener_usuario_o_token)):
    print(type(current_user))
    if isinstance(current_user, DatosU):
        usuario_id = current_user.id_user_name
    elif isinstance(current_user, TokenAnonimo):
        usuario_id = current_user.id_user_name
    
    print(usuario_id)

    particion = custom_particionador.particion({"usuario_id": usuario_id})

    datos_usuario = await conversaciones_coleccion.find_one(
        {"usuario_id": usuario_id},
        sort=[("timestamp", -1)],
        limit=1
    )
    
    print(datos_usuario)

    if datos_usuario is None:
        raise HTTPException(status_code=404, detail="Token de conversación no válido")

    mensaje = datos_usuario.get("mensaje")
    usernameR = datos_usuario.get("username_usuario")
    
    # Corrige la ortografía del mensaje
    matches = tool.check(mensaje)
    for match in matches:
        if match.replacements:
            mensaje = mensaje[:match.offset] + match.replacements[0] + mensaje[match.offset + match.errorLength:]
    
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

        # Guardar la partición en la base de datos
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

    print(f"Clave: {usuario_id}, Partición: {particion}")

    await guardar_respuestas(respuesta_chatbot)

    historial_productor.send('historial_topic', key=usuario_id, value=completo, partition=particion)

    return {"Respuesta": respuesta_chatbot}


#Endpoint para obtener el historial de cierto usuario por su "usuario_id"
@app.get("/Obtener_historial")
async def obtener_historial(current_user: Union[DatosU, TokenAnonimo] = Depends(obtener_usuario_o_token)):
    if isinstance(current_user, DatosU):
        usuario_id = current_user.id_user_name
    elif isinstance(current_user, TokenAnonimo):
        usuario_id = current_user.id_user_name  

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


# Iniciar uvicorn 
if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=8000)

