from passlib.context import CryptContext
from jose import jwt, JWTError
from datetime import datetime, timedelta, timezone
from pydantic import BaseModel, ValidationError
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
import mysql.connector
from enum import Enum
from typing import Optional, Union
import aiomysql

class TipoUsuario(str, Enum):
    Anonimo = "Anonimo"
    Registrado = "Registrado"

# Definición del modelo de datos del usuario
class DatosU(BaseModel):
    nombre_usuario: str
    correo_electronico: str
    user_name: str
    id_user_name: str
    tipo_usuario: str
    
# Configuración los parámetros de conexión de MySQL
config = {
    'user': 'tennus01',
    'password': 'sulaco987Q_Q',
    'host': '192.168.1.120',
    'db': 'test',  # Cambiado 'database' a 'db' para aiomysql
    'port': 3307, # Puerto predeterminado de MySQL
}

# Creación un contexto de cifrado
contexto_cifrado = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Establecer la clave secreta, el algoritmo y la duración del token de acceso
CLAVE_SECRETA = "tu-clave-secreta"
ALGORITMO = "HS256"
DURACION_TOKEN_ACCESO_EN_MINUTOS = 30

# Define el token predeterminado para usuarios anónimos
TOKEN_ANONIMO_POR_DEFECTO = jwt.encode({"sub": "anonimo"}, CLAVE_SECRETA, algorithm=ALGORITMO)

"""
# Obtiener una conexión a la base de datos
def obtener_conexion_db():
    return mysql.connector.connect(**config)
"""

# Obtiener una conexión a la base de datos
async def obtener_conexion_db():
    return await aiomysql.connect(**config)

# Obtener un usuario de la base de datos
async def obtener_usuario(conexion_db, nombre_usuario_o_correo, contrasena):
    async with conexion_db.cursor(aiomysql.DictCursor) as cursor:
        # Ejecutar la consulta
        await cursor.execute(
            "SELECT * FROM usuarios WHERE correo_electronico=%s OR user_name=%s",
            (nombre_usuario_o_correo, nombre_usuario_o_correo)
        )
        usuario = await cursor.fetchone()
    
    # Si no se encuentra el usuario, lanzar excepción
    if not usuario:
        raise HTTPException(
            status_code=401,
            detail="No se pudieron validar las credenciales",
            headers={"WWW-Authenticate": "Bearer"}
        )

    hash_almacenado = usuario["contrasena"]  # Suponiendo que el campo se llama "contrasena"

    # Verificar la contraseña
    if not await verificar_contrasena(contrasena, hash_almacenado):
        raise HTTPException(
            status_code=401,
            detail="No se pudieron validar las credenciales",
            headers={"WWW-Authenticate": "Bearer"}
        )
    
    return usuario

# Función para verificar la contraseña
async def verificar_contrasena(contrasena_texto_plano, contrasena_cifrada):
    return contexto_cifrado.verify(contrasena_texto_plano, contrasena_cifrada)

# Obtener un hash de una contraseña
async def obtener_hash_contrasena(contrasena):
    return contexto_cifrado.hash(contrasena)

# Esquema de OAuth2 para validar tokens de acceso JWT
esquema_oauth2 = OAuth2PasswordBearer(tokenUrl="/Token")

# Función para obtener el token de acceso
async def obtener_token_acceso(usuario: dict): # Cambiamos el argumento a 'usuario'
    fecha_expiracion = datetime.now(timezone.utc) + timedelta(minutes=DURACION_TOKEN_ACCESO_EN_MINUTOS)
    # Incluimos 'tipo_usuario' en el payload:
    token_acceso = jwt.encode({
        "sub": usuario['user_name'], 
        "exp": fecha_expiracion,
        "tipo_usuario": usuario['tipo_usuario'].value  # Obtener el valor de la enumeración
    }, CLAVE_SECRETA, algorithm=ALGORITMO)
    return token_acceso

# Función para manejar la autenticación y obtener el token
async def obtener_token(token: str = Depends(esquema_oauth2)):
    if token is None:
        return TOKEN_ANONIMO_POR_DEFECTO
    else:
        return token
    
# Autentificar un usuario
# Autenticar un usuario
async def autenticar_usuario(conexion_db, nombre_usuario_o_correo, contrasena):
    usuario = await obtener_usuario(conexion_db, nombre_usuario_o_correo, contrasena)
    # Verificar nuevamente si no se encontró el usuario (aunque es redundante, lo añadimos para claridad)
    if not usuario:
        raise HTTPException(
            status_code=401,
            detail="No se pudieron validar las credenciales",
            headers={"WWW-Authenticate": "Bearer"}
        )
    return usuario

# Crear un token de acceso JWT
async def crear_token_acceso(datos: dict, duracion_delta: timedelta | None = None):
    datos_para_codificar = datos.copy()
    if duracion_delta:
        fecha_expiracion = datetime.now(timezone.utc) + duracion_delta
    else:
        fecha_expiracion = datetime.now(timezone.utc) + timedelta(minutes=15)
    datos_para_codificar.update({"exp": fecha_expiracion})
    token_codificado = jwt.encode(datos_para_codificar, CLAVE_SECRETA, algorithm=ALGORITMO)
    return token_codificado

"""
# Obtiener el usuario actual a partir de un token de acceso JWT
def obtener_usuario_actual(token: str = Depends(esquema_oauth2)):
    try:
        carga_util = jwt.decode(token, CLAVE_SECRETA, algorithms=[ALGORITMO])
        nombre_usuario: str = carga_util.get("sub")
        if nombre_usuario is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="No se pudo validar el token",
                headers={"WWW-Authenticate": "Bearer"},
            )
        return nombre_usuario
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token inválido o expirado",
            headers={"WWW-Authenticate": "Bearer"},
        )
"""

# Obtiener el usuario actual a partir de un token de acceso JWT
async def obtener_usuario_actual(token: str = Depends(esquema_oauth2)) -> DatosU:
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
        
        usuario = obtener_usuario_por_identificador(id_user_name)
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
# Función para obtener datos del usuario especifico desde la base de datos
def obtener_usuario_por_identificador(identificador: str) -> Optional[DatosU]:
    conexion = obtener_conexion_db()
    if not conexion:
        return None

    cursor = conexion.cursor(dictionary=True)

    # Primero intenta buscar por user_name
    query = "SELECT nombre_usuario, correo_electronico, user_name, id_user_name, tipo_usuario FROM usuarios WHERE user_name = %s"
    cursor.execute(query, (identificador,))
    resultado = cursor.fetchone()

    # Si no encuentra, intenta buscar por correo_electronico
    if not resultado:
        query = "SELECT nombre_usuario, correo_electronico, user_name, id_user_name, tipo_usuario FROM usuarios WHERE correo_electronico = %s"
        cursor.execute(query, (identificador,))
        resultado = cursor.fetchone()

    cursor.close()
    conexion.close()

    if resultado:
        try:
            return DatosU(
                nombre_usuario=resultado.get('nombre_usuario'),
                correo_electronico=resultado.get('correo_electronico'),
                user_name=resultado.get('user_name'),
                id_user_name=resultado.get('id_user_name'),
                tipo_usuario=resultado.get('tipo_usuario')  
            )
        except ValidationError as e:
            print("Error de validación:", e)
            return None
    else:
        return None
"""

async def obtener_usuario_por_identificador(identificador: str) -> Optional[DatosU]:
    conexion = await obtener_conexion_db()
    if not conexion:
        return None

    async with conexion.cursor(aiomysql.DictCursor) as cursor:
        # Primero intenta buscar por user_name
        query = "SELECT nombre_usuario, correo_electronico, user_name, id_user_name, tipo_usuario FROM usuarios WHERE user_name = %s"
        await cursor.execute(query, (identificador,))
        resultado = await cursor.fetchone()

        # Si no encuentra, intenta buscar por correo_electronico
        if not resultado:
            query = "SELECT nombre_usuario, correo_electronico, user_name, id_user_name, tipo_usuario FROM usuarios WHERE correo_electronico = %s"
            await cursor.execute(query, (identificador,))
            resultado = await cursor.fetchone()

    if resultado:
        try:
            return DatosU(
                nombre_usuario=resultado.get('nombre_usuario'),
                correo_electronico=resultado.get('correo_electronico'),
                user_name=resultado.get('user_name'),
                id_user_name=resultado.get('id_user_name'),
                tipo_usuario=resultado.get('tipo_usuario')  
            )
        except ValidationError as e:
            print("Error de validación:", e)
            return None
    else:
        return None


class DatosToken(BaseModel):
    nombre_usuario: str | None = None

class Token(BaseModel):
    token_acceso: str
    tipo_token: str
    nombre_usuario: Optional[str] = None

