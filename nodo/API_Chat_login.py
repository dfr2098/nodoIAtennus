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
from fastapi import FastAPI, Depends, HTTPException, status, Request, APIRouter, Form, Response, Header
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from Autentificacion import (
    autenticar_usuario,
    crear_token_acceso,
    obtener_conexion_db,
    DURACION_TOKEN_ACCESO_EN_MINUTOS,
    obtener_hash_contrasena,
    CLAVE_SECRETA, ALGORITMO, obtener_usuario_por_identificador
)
from datetime import timedelta
from pydantic import BaseModel, EmailStr, Field, field_validator, ValidationError, model_validator
from email_validator import validate_email, EmailNotValidError
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
import re
from typing import Annotated, Union
import requests
import jwt 
from jose import JWTError
import logging


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

"""
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
"""

"""
@app.post("/Token", response_model=Token)
async def obtener_token_acceso(usuario: str = Form(None), contrasena: str = Form(None)):
    if usuario and contrasena:
        # Se proporcionaron credenciales de usuario, verificarlas
        usuario_valido = autenticar_usuario(obtener_conexion_db(), usuario, contrasena)
        if not usuario_valido:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Usuario o contraseña incorrectos",
                headers={"WWW-Authenticate": "Bearer"},
            )
        # Si las credenciales son válidas, generar un token de acceso
        datos_token = {"sub": usuario}
    else:
        # No se proporcionaron credenciales, usar el tipo de usuario anónimo
        tipo_usuario_anonimo = TipoUsuario.Anonimo
        datos_token = {"sub": tipo_usuario_anonimo}

    # Generar y devolver el token de acceso
    duracion_token_acceso = timedelta(minutes=DURACION_TOKEN_ACCESO_EN_MINUTOS)
    token_acceso = crear_token_acceso(datos=datos_token, duracion_delta=duracion_token_acceso)
    
    return {"token_acceso": token_acceso, "tipo_token": "bearer"}


#endpoint para manejar sesiones
@app.post("/Sesión")
async def manejar_sesion(request: Request, token: str = Depends(obtener_token)):
    # Intenta obtener el usuario actual a partir del token recibido en el encabezado de autorización
    try:
        usuario_actual = obtener_usuario_actual(token)
        tipo_usuario = TipoUsuario.Registrado
    except:
        # Si el token no es válido o no está presente, trata al usuario como anónimo
        tipo_usuario = TipoUsuario.Anonimo

    # Almacena el tipo de usuario en el estado de la solicitud
    request.state.tipo_usuario = tipo_usuario

    if tipo_usuario == TipoUsuario.Anonimo:
        # Genera el nombre y el username del usuario anónimo
        nombre_usuario = generar_nombre()
        username_usuario = generar_username()

        # Almacena la información del usuario anónimo en una variable de sesión
        session_data = {"nombre_usuario": nombre_usuario, "username_usuario": username_usuario}

        # Permite que el usuario anónimo use el chat
        return {"mensaje": f"Bienvenido, {tipo_usuario.value}!"}
    elif tipo_usuario == TipoUsuario.Registrado:
        # El usuario es registrado, realiza las acciones necesarias
        return {"mensaje": f"Bienvenido, {tipo_usuario.value}!"}


class RegistroUsuario(BaseModel):
    nombre_usuario: str = Field(..., min_length=5, max_length=20, description="El nombre de usuario debe tener entre 5 y 20 caracteres"),
    contrasena: str = Field(..., min_length=5, max_length=15, description="La contraseña debe tener entre 5 y 20 caracteres"),
    confirmar_contrasena: str = Field(..., min_length=5, max_length=15, description="Repetir contraseña"),
    email: EmailStr = Field(..., description="Ingresa un correo"),
    user_name: str = Field(..., pattern=r'^\S+$', min_length=1, max_length=15, description="El username debe tener entre 1 y 15 caracteres y no puede contener espacios en blanco"),  # Incluir user_name como un campo de formulario con restricciones de longitud y sin espacios en blanco),
    
    @model_validator(mode='after')
    def no_espacios_en_nombre_de_usuario(valor: str) -> str:
        if not re.match(r'^\S+$', valor):
            raise ValueError('El nombre de usuario no puede contener espacios en blanco')
        return valor

    @model_validator(mode='after')
    def contrasenas_coinciden(contrasena: str, confirmar_contrasena: str) -> None:
        if contrasena != confirmar_contrasena:
            raise ValueError('Las contraseñas no coinciden')
    
    @model_validator(mode='after')
    def validar_longitud_nombre(nombre_usuario: str) -> None:
        if len(nombre_usuario) < 5:
            raise ValueError('El nombre de usuario debe tener al menos 5 caracteres')
        
    @model_validator(mode='after')
    def validar_longitud_username(user_name: str) -> None:
        if len(user_name) < 5:
            raise ValueError('El username debe tener al menos 5 caracteres')
    
    @model_validator(mode='after')
    def correo_correcto(valor: EmailStr) -> EmailStr:
        try:
            validate_email(valor)
        except EmailNotValidError as e:
            raise ValueError('El correo proporcionado no es correcto')
        return valor
"""
     
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
    
"""
@app.post("/Registro")
async def crear_usuario(
    request: Request,
    nombre_usuario: Annotated[str, Form(min_length=5, max_length=20, description="El nombre de usuario debe tener entre 5 y 20 caracteres")],
    contrasena: Annotated[str, Form(min_length=5, max_length=15, description="La contraseña debe tener entre 5 y 15 caracteres")],
    confirmar_contrasena: Annotated[str, Form(min_length=5, max_length=15, description="Repetir contraseña")],
    correo: Annotated[str, Form(description="Ingresar un correo")],
    user_name: Annotated[str, Form(min_length=5, max_length=15, description="El nombre de usuario debe tener entre 5 y 15 caracteres y no puede contener espacios en blanco")]
):
    try:
        # Valida las contraseñas
        RegistroUsuario.contrasenas_coinciden(contrasena, confirmar_contrasena)

        # Valida longitudes de nombres de usuario
        RegistroUsuario.validar_longitudes(nombre_usuario, user_name)

        # Valida espacios en nombre de usuario
        RegistroUsuario.no_espacios_en_nombre_de_usuario(user_name)
        
        # Valida espacios en nombre de usuario
        RegistroUsuario.correo_correcto(correo)

        # Obtén la dirección IP del usuario
        ip_usuario = request.client.host if request.client.host else None

        # Verifica si el usuario ya existe en la base de datos
        conexion_db = obtener_conexion_db()
        cursor = conexion_db.cursor()
        cursor.execute("SELECT * FROM usuarios WHERE user_name = %s", (user_name,))
        nombre_de_usuario_existente = cursor.fetchone()
        if nombre_de_usuario_existente:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="El nombre de usuario ya está en uso",
            )

        cursor.execute("SELECT * FROM usuarios WHERE correo_electronico = %s", (correo,))
        correo_existente = cursor.fetchone()
        if correo_existente:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="El correo electrónico ya está en uso",
            )
        cursor.close()

        # Crea un hash de la contraseña
        contrasena_cifrada = obtener_hash_contrasena(contrasena)

        # Genera un UUID para el campo id_nombre_usuario
        id_nombre_usuario = generar_uuid(user_name)

        # Almacena el usuario en la base de datos con tipo_usuario como "Registrado"
        cursor = conexion_db.cursor()
        cursor.execute(
            "INSERT INTO usuarios (user_name, nombre_usuario, contrasena, correo_electronico, id_nombre_usuario, tipo_usuario, ip_usuario) VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (user_name, nombre_usuario, contrasena_cifrada, correo, id_nombre_usuario, TipoUsuario.Registrado.value, ip_usuario),
        )
        conexion_db.commit()
        cursor.close()

        # Genera un token de acceso para el usuario recién creado
        duracion_token_acceso = timedelta(minutes=DURACION_TOKEN_ACCESO_EN_MINUTOS)
        datos_token = {"sub": user_name}
        token_acceso = crear_token_acceso(datos=datos_token, duracion_delta=duracion_token_acceso)

        # Devuelve el usuario creado, el id_nombre_usuario y el token de acceso
        datos_usuario = UsuarioCreado(
            nombre_usuario=nombre_usuario,
            contrasena="****",  # Proporciona un valor de marcador de posición para el campo de contraseña
            correo=correo,
            user_name=user_name,
            id_nombre_usuario=id_nombre_usuario,
            ip_usuario=ip_usuario
        )

        return {
            "usuario": datos_usuario,
            "id_nombre_usuario": id_nombre_usuario,
            "token_acceso": token_acceso,
            "tipo_token": "bearer",
        }
    except ValidationError as e:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content=jsonable_encoder({"detalle": e.errors()}),
        )
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content=jsonable_encoder({"mensaje": f"Error al crear el usuario: {str(e)}"}),
        )


@app.post("/Registro")
async def crear_usuario(
    request: Request,
    nombre_usuario: str = Form(..., min_length=5, max_length=20, description="El nombre de usuario debe tener entre 5 y 20 caracteres"),
    contrasena: str = Form(..., min_length=5, max_length=15, description="La contraseña debe tener entre 5 y 20 caracteres"),
    confirmar_contrasena: str = Form(..., min_length=5, max_length=15, description="Repetir contraseña"),
    email: EmailStr = Form(..., description="Ingresa un correo"),
    user_name: str = Form(..., pattern=r'^\S+$', min_length=1, max_length=15, description="El username debe tener entre 1 y 15 caracteres y no puede contener espacios en blanco"),  # Incluir user_name como un campo de formulario con restricciones de longitud y sin espacios en blanco),
):
   
    usuario = RegistroUsuario()

    # Valida las contraseñas
    usuario.contrasenas_coinciden(contrasena, confirmar_contrasena)

    # Valida longitudes de nombres de usuario
    usuario.validar_longitud_nombre(nombre_usuario)
    
    # Valida longitudes de nombres de usuario
    usuario.validar_longitud_username(user_name)

    # Valida espacios en nombre de usuario
    usuario.no_espacios_en_nombre_de_usuario(user_name)
        
    # Valida espacios en nombre de usuario
    usuario.correo_correcto(email)

    # Obtén la dirección IP del usuario
    ip_usuario = request.client.host if request.client.host else None

    # Verifica si el usuario ya existe en la base de datos
    conexion_db = obtener_conexion_db()
    cursor = conexion_db.cursor()
    cursor.execute("SELECT * FROM usuarios WHERE user_name = %s", (user_name,))
    username_existente = cursor.fetchone()
    cursor.close()

    if username_existente:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="El nombre de usuario ya está en uso",
        )

    # Verifica si el correo electrónico ya existe en la base de datos
    conexion_db = obtener_conexion_db()
    cursor = conexion_db.cursor()
    cursor.execute("SELECT * FROM usuarios WHERE correo_electronico = %s", (email,))
    email_existente = cursor.fetchone()
    cursor.close()

    if email_existente:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="El correo electrónico ya está en uso",
        )

    # Crea un hash de la contraseña
    contrasena_cifrada = obtener_hash_contrasena(contrasena)

    # Genera un UUID para el campo id_user_name
    id_user_name = generar_uuid(user_name)

    # Almacena el usuario en la base de datos con tipo_usuario como "Registrado"
    conexion_db = obtener_conexion_db()
    cursor = conexion_db.cursor()
    cursor.execute(
        "INSERT INTO usuarios (user_name, nombre_usuario, contrasena, correo_electronico, id_user_name, tipo_usuario, ip_usuario) VALUES (%s, %s, %s, %s, %s, %s, %s)",
        (user_name, nombre_usuario, contrasena_cifrada, email, id_user_name, TipoUsuario.Registrado.value, ip_usuario),
    )
    conexion_db.commit()
    cursor.close()

    # Genera un token de acceso para el usuario recién creado
    duracion_token_acceso = timedelta(minutes=DURACION_TOKEN_ACCESO_EN_MINUTOS)
    datos_token = {"sub": user_name}
    token_acceso = crear_token_acceso(datos=datos_token, duracion_delta=duracion_token_acceso)

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
        "token_acceso": token_acceso,
        "tipo_token": "bearer",
    }
"""
 
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

@app.post("/Registro")
async def crear_usuario(
    request: Request,
    nombre_usuario: str = Form(..., min_length=5, max_length=20, description="El nombre de usuario debe tener entre 5 y 20 caracteres"),
    contrasena: str = Form(..., min_length=5, max_length=15, description="La contraseña debe tener entre 5 y 15 caracteres"),
    confirmar_contrasena: str = Form(..., min_length=5, max_length=15, description="Repetir contraseña"),
    email: EmailStr = Form(..., description="Ingresa un correo"),
    user_name: str = Form(..., pattern=r'^\S+$', min_length=1, max_length=15, description="El username debe tener entre 1 y 15 caracteres y no puede contener espacios en blanco")
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
    conexion_db = obtener_conexion_db()
    cursor = conexion_db.cursor()
    cursor.execute("SELECT * FROM usuarios WHERE user_name = %s", (user_name,))
    username_existente = cursor.fetchone()
    cursor.close()

    if username_existente:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="El nombre de usuario ya está en uso",
        )

    # Verifica si el correo electrónico ya existe en la base de datos
    conexion_db = obtener_conexion_db()
    cursor = conexion_db.cursor()
    cursor.execute("SELECT * FROM usuarios WHERE correo_electronico = %s", (email,))
    email_existente = cursor.fetchone()
    cursor.close()

    if email_existente:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="El correo electrónico ya está en uso",
        )

    # Crea un hash de la contraseña
    contrasena_cifrada = obtener_hash_contrasena(contrasena)

    # Genera un UUID para el campo id_user_name
    id_user_name = generar_uuid(user_name)

    # Almacena el usuario en la base de datos con tipo_usuario como "Registrado"
    conexion_db = obtener_conexion_db()
    cursor = conexion_db.cursor()
    cursor.execute(
        "INSERT INTO usuarios (user_name, nombre_usuario, contrasena, correo_electronico, id_user_name, tipo_usuario, ip_usuario) VALUES (%s, %s, %s, %s, %s, %s, %s)",
        (user_name, nombre_usuario, contrasena_cifrada, email, id_user_name, TipoUsuario.Registrado.value, ip_usuario),
    )
    conexion_db.commit()
    cursor.close()

    # Genera un token de acceso para el usuario recién creado
    duracion_token_acceso = timedelta(minutes=DURACION_TOKEN_ACCESO_EN_MINUTOS)
    datos_token = {"sub": user_name, "nombre_usuario": nombre_usuario, "tipo_usuario": "Registrado"}
    token_acceso = crear_token_acceso(datos=datos_token, duracion_delta=duracion_token_acceso)

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
        "token_type": "bearer",
    }
        
"""
async def obtener_token_acceso(nombre_usuario_o_correo: Optional[str] = None, contrasena: Optional[str] = None, request: Request = None):
    if nombre_usuario_o_correo and contrasena:
        # Se proporcionaron credenciales de usuario, verificarlas
        usuario_valido = autenticar_usuario(obtener_conexion_db(), nombre_usuario_o_correo, contrasena)
        if not usuario_valido:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Usuario o contraseña incorrectos",
                headers={"WWW-Authenticate": "Bearer"},
            )
        # Si las credenciales son válidas, generar un token de acceso
        datos_token = {"sub": nombre_usuario_o_correo}
    else:
        # No se proporcionaron credenciales, usar el tipo de usuario anónimo
        tipo_usuario_anonimo = TipoUsuario.Anonimo
        datos_token = {"sub": tipo_usuario_anonimo}

    # Generar y devolver el token de acceso
    duracion_token_acceso = timedelta(minutes=DURACION_TOKEN_ACCESO_EN_MINUTOS)
    token_acceso = crear_token_acceso(datos=datos_token, duracion_delta=duracion_token_acceso)

    # Almacena el tipo de usuario en el estado de la solicitud
    request.state.tipo_usuario = tipo_usuario_anonimo if not nombre_usuario_o_correo else TipoUsuario.Registrado

    return {"token_acceso": token_acceso, "tipo_token": "bearer"}


@app.post("/token-y-sesion")
async def manejar_sesion(request: Request, token_acceso: Optional[str] = Depends(obtener_token_acceso)):
    try:
        #usuario_actual = obtener_usuario_actual(token_acceso)
        tipo_usuario = TipoUsuario.Registrado
    except:
        # Si el token no es válido o no está presente, trata al usuario como anónimo
        tipo_usuario = TipoUsuario.Anonimo

    # Almacena el tipo de usuario en el estado de la solicitud
    request.state.tipo_usuario = tipo_usuario

    if tipo_usuario == TipoUsuario.Anonimo:
        # Genera el nombre y el username del usuario anónimo
        nombre_usuario = generar_nombre()
        username_usuario = generar_username()

        # Almacena la información del usuario anónimo en una variable de sesión
        session_data = {"nombre_usuario": nombre_usuario, "username_usuario": username_usuario}

        # Permite que el usuario anónimo use el chat
        return {"token_acceso": token_acceso, "mensaje": f"Bienvenido, {tipo_usuario.value}!"}
    elif tipo_usuario == TipoUsuario.Registrado:
        # El usuario es registrado, realiza las acciones necesarias
        return {"token_acceso": token_acceso, "mensaje": f"Bienvenido, {tipo_usuario.value}!"}
"""



# Esquema de OAuth2 para validar tokens de acceso JWT
esquemaa_oauth2 = OAuth2PasswordBearer(tokenUrl="/")

"""
@app.post("/Token")
def obtener_token_acceso(nombre_usuario_o_correo: Optional[str] = None, contrasena: Optional[str] = None, request: Request = None):
    if nombre_usuario_o_correo is not None and contrasena is not None:
        # Se proporcionaron credenciales de usuario, verificarlas
        usuario_valido = autenticar_usuario(obtener_conexion_db(), nombre_usuario_o_correo, contrasena)
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
        nombre_usuario = usuario_valido['nombre_usuario']  # Aquí debes reemplazar 'nombre' con el campo correcto de la base de datos

        # Devolver el nombre del usuario junto con el tipo de usuario
        datos_token.update({"nombre_usuario": nombre_usuario})
    elif nombre_usuario_o_correo is not None:
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
        # No se proporcionaron credenciales, usar el tipo de usuario anónimo
        tipo_usuario = TipoUsuario.Anonimo
        datos_token = {"sub": tipo_usuario}

    # Generar y devolver el token de acceso
    duracion_token_acceso = timedelta(minutes=DURACION_TOKEN_ACCESO_EN_MINUTOS)
    token_acceso = crear_token_acceso(datos=datos_token, duracion_delta=duracion_token_acceso)
    

    # Almacena el tipo de usuario en el estado de la solicitud
    request.state.tipo_usuario = tipo_usuario

    return {"token_acceso": token_acceso, "tipo_token": "bearer", "nombre_usuario": nombre_usuario if tipo_usuario == TipoUsuario.Registrado else None}
"""


"""
@app.post("/token")
def obtener_token_acceso(request: Request = None, form_data: OAuth2PasswordRequestForm = Depends()):
    usuario_valido = autenticar_usuario(obtener_conexion_db(), form_data.username, form_data.password)
    if not usuario_valido:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Usuario o contraseña incorrectos",
            headers={"WWW-Authenticate": "Bearer"},
        )

    datos_token = {"sub": form_data.username}
    tipo_usuario = TipoUsuario.Registrado

    nombre_usuario = usuario_valido['nombre_usuario']
    datos_token.update({"nombre_usuario": nombre_usuario})

    duracion_token_acceso = timedelta(minutes=DURACION_TOKEN_ACCESO_EN_MINUTOS)
    token_acceso = crear_token_acceso(datos=datos_token, duracion_delta=duracion_token_acceso)

    request.state.tipo_usuario = tipo_usuario

    return {"token_acceso": token_acceso, "tipo_token": "bearer", "nombre_usuario": nombre_usuario}



async def obtener_usu(token: str = Depends(esquemaa_oauth2)):
    try:
        carga_util = jwt.decode(token, CLAVE_SECRETA, algorithms=[ALGORITMO])
        username = carga_util.get("sub")
        if username == None:
            raise HTTPException(status_code=401, detail="No se pudieron validar las credenciales", headers={"WWW-Authenticate": "Bearer"})
    except JWTError:
        raise HTTPException(status_code=401, detail="No se pudieron validar las credenciales", headers={"WWW-Authenticate": "Bearer"})
    usuario = obtener_usuario_por_id(username)
    if not usuario:
        raise HTTPException(status_code=401, detail="No se pudieron validar las credenciales", headers={"WWW-Authenticate": "Bearer"})
    return usuario

async def obtener_usuario_actual(token: str = Depends(esquemaa_oauth2)) -> DatosU:
    try:
        carga_util = jwt.decode(token, CLAVE_SECRETA, algorithms=[ALGORITMO])
        tipo_usuario = carga_util.get("sub")
        
        if tipo_usuario == TipoUsuario.Anonimo.value:
            return DatosU(
                nombre_usuario=carga_util.get("nombre_usuario"),
                email=None,
                user_name=carga_util.get("user_name"),
                id_user_name=carga_util.get("id_user_name")
            )
        
        id_user_name = carga_util.get("sub")
        if id_user_name is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="No se pudo validar el token",
                headers={"WWW-Authenticate": "Bearer"},
            )
        
        usuario = obtener_usuario_por_id(id_user_name)
        if usuario is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Usuario no encontrado",
                headers={"WWW-Authenticate": "Bearer"},
            )
        return usuario
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token inválido o expirado",
            headers={"WWW-Authenticate": "Bearer"},
        )   
"""

"""
@app.post("/token", response_model=Token)
async def obtener_token_acceso(form_data: OAuth2PasswordRequestForm = Depends()) -> Token:
        usuario_valido = autenticar_usuario(obtener_conexion_db(), form_data.username, form_data.password)
        if not usuario_valido:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Usuario o contraseña incorrectos",
                headers={"WWW-Authenticate": "Bearer"},
            )

        datos_token = {"sub": form_data.username}
        nombre_usuario = usuario_valido['nombre_usuario']
        datos_token.update({"nombre_usuario": nombre_usuario})

        duracion_token_acceso = timedelta(minutes=DURACION_TOKEN_ACCESO_EN_MINUTOS)
        token_acceso = crear_token_acceso(datos=datos_token, duracion_delta=duracion_token_acceso)

        return {"access_token": token_acceso, "token_type": "bearer", "nombre_usuario": nombre_usuario}
"""


@app.post("/Iniciar_Sesion", response_model=Token)
def obtener_token_acceso(form_data: OAuth2PasswordRequestForm = Depends(), request: Request = None) -> Token:
    nombre_usuario_o_correo = form_data.username  # Extraer el nombre de usuario o correo del form_data
    contrasena = form_data.password  # Extraer la contraseña del form_data

    if nombre_usuario_o_correo is not None and contrasena is not None:
        # Se proporcionaron credenciales de usuario, verificarlas
        usuario_valido = autenticar_usuario(obtener_conexion_db(), nombre_usuario_o_correo, contrasena)
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

        # Devolver el nombre del usuario junto con el tipo de usuario
        datos_token.update({"nombre_usuario": nombre_usuario, "tipo_usuario": "Registrado"})
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
    token_acceso = crear_token_acceso(datos=datos_token, duracion_delta=duracion_token_acceso)

    # Almacena el tipo de usuario en el estado de la solicitud
    request.state.tipo_usuario = tipo_usuario

    return Token(
        access_token=token_acceso,
        token_type="bearer",
        nombre_usuario=nombre_usuario,
        tipo_usuario= TipoUsuario.Registrado
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

            usuario = obtener_usuario_por_identificador(username)
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

"""
async def obtener_usu(token: str = Depends(esquemaa_oauth2)) -> DatosU:
    try:
        carga_util = jwt.decode(token, CLAVE_SECRETA, algorithms=[ALGORITMO])
        username = carga_util.get("sub")
        if username is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="No se pudieron validar las credenciales",
                headers={"WWW-Authenticate": "Bearer"}
            )
        usuario = obtener_usuario_por_identificador(username)
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
"""

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
    token_acceso = crear_token_acceso(datos=datos_token, duracion_delta=duracion_token_acceso)

    request.state.tipo_usuario = tipo_usuario

    return TokenAnonimo(
        access_token = token_acceso,
        token_type= "bearer",
        tipo_usuario= TipoUsuario.Anonimo,
        nombre_usuario= nombre_usuario_anonimo,
        user_name= username_usuario_anonimo,
        id_user_name= id_user_name_anonimo,
        mensaje= f"Bienvenido, {tipo_usuario.value}!"
    )

async def cerrar_sesion(request: Request = None):
    # incluir eliminar el token de una lista negra o invalidar el token en la base de datos.
    # Dependiendo de cómo se maneje los tokens, esta lógica variará.
    # excepción 401 Unauthorized.
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Sesión cerrada exitosamente",
        headers={"WWW-Authenticate": "Bearer"},
    )


@app.post("/Cerrar_Sesion")
async def manejar_cerrar_sesion():
    await cerrar_sesion()

"""
@app.get("/Chat")
async def chat(
    request: Request,
    mensaje: str,
    ip: Optional[str] = None,
    user_agent: Optional[str] = None,
    usuario: DatosU = Depends(obtener_usu)
):
    
    tipo_usuario = request.state.tipo_usuario
    if tipo_usuario == TipoUsuario.Registrado:
        if not (usuario.user_name and usuario.nombre_usuario and usuario.id_user_name):
            raise HTTPException(status_code=400, detail="El username, el nombre y el id_user_name son obligatorios para usuarios registrados.")
        username = usuario.user_name
        nombre = usuario.nombre_usuario
        id_user_name = usuario.id_user_name
    else:
        username = usuario.user_name
        nombre = usuario.nombre_usuario
        id_user_name = usuario.id_user_name

    conversacion = await construir_conversacion(request, mensaje, tipo_usuario, username, nombre, ip, user_agent)
    usuario_id = conversacion["usuario_id"]
    await guardar_conversacion(conversacion)
    productor.send('input_topic', key=usuario_id, value={"conversación": conversacion})

    return {"conversación": conversacion}
"""

# Función para validar el token y obtener el usuario
async def obtener_usuario_o_token(request: Request, token: str = Depends(esquemaa_oauth2)):
    try:
        payload = jwt.decode(token, CLAVE_SECRETA, algorithms=[ALGORITMO])
        tipo_usuario_valor = payload.get("tipo_usuario")  # Obtener el valor de 'tipo_usuario' del payload
        tipo_usuario = TipoUsuario(tipo_usuario_valor) if tipo_usuario_valor else None

        if tipo_usuario == TipoUsuario.Registrado:
            nombre_usuario = payload.get("sub")
            usuario_registrado = obtener_usuario_por_identificador(nombre_usuario)
            if usuario_registrado:
                yield usuario_registrado  # Devolver el objeto DatosU si se encuentra
            else:
                raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Usuario no encontrado")
        elif tipo_usuario == TipoUsuario.Anonimo:
            # Si es un usuario anónimo, crea una instancia de TokenAnonimo
            yield TokenAnonimo(
                access_token=token,
                token_type="bearer",
                tipo_usuario=TipoUsuario.Anonimo,
                nombre_usuario=payload.get("nombre_usuario"),
                user_name=payload.get("user_name"),
                id_user_name=payload.get("id_user_name"),
                mensaje="Bienvenido, usuario anónimo!"
            )
        else:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Tipo de usuario desconocido en el token.")

    except JWTError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token inválido")


@app.get("/Chat")
async def chat(
    request: Request,
    mensaje: str,
    ip: Optional[str] = None,
    user_agent: Optional[str] = None,
    usuario: Union[DatosU, TokenAnonimo] = Depends(obtener_usuario_o_token)
):
    if isinstance(usuario, DatosU):
        tipo_usuario = TipoUsuario.Registrado
        username = usuario.user_name
        nombre = usuario.nombre_usuario
        # Obtén id_user_name del token en obtener_usu:
        id_user_name = usuario.id_user_name 
    elif isinstance(usuario, TokenAnonimo):
        tipo_usuario = TipoUsuario.Anonimo
        username = usuario.user_name
        nombre = usuario.nombre_usuario
        id_user_name = usuario.id_user_name
    else:
        raise HTTPException(status_code=400, detail="Tipo de usuario desconocido.")

    conversacion = await construir_conversacion(request, mensaje, tipo_usuario, username, nombre, ip, user_agent)
    usuario_id = conversacion["usuario_id"]
    await guardar_conversacion(conversacion)
    productor.send('input_topic', key=usuario_id, value={"conversación": conversacion})

    return {"conversación": conversacion}


@app.post("/Respuesta_chat/{usuario_id}")
async def obtener_respuesta_chat(
    usuario_id: str,
    usuario: DatosU = Depends(obtener_usu)
):
    particion = custom_particionador.particion({"usuario_id": usuario_id})

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
@app.get("/Obtener_historial/{usuario_id}")
async def obtener_historial(
    usuario_id: str,
    usuario: DatosUsuario = Depends(obtener_usu) # Cambia la dependencia a obtener_usuario_actual
):
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

