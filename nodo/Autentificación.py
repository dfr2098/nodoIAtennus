from passlib.context import CryptContext
from jose import jwt, JWTError
from datetime import datetime, timedelta, timezone
from pydantic import BaseModel
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
import mysql.connector
from enum import Enum

class TipoUsuario(str, Enum):
    Anonimo = "Anónimo"
    Registrado = "Registrado"

# Configuración los parámetros de conexión de MySQL
config = {
  'user': 'tennus01',
  'password': 'sulaco987Q_Q',
  'host': '192.168.1.120',
  'database': 'test',
  'port': '3307', # Puerto predeterminado de MySQL
}

# Creación un contexto de cifrado
contexto_cifrado = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Establecer la clave secreta, el algoritmo y la duración del token de acceso
CLAVE_SECRETA = "tu-clave-secreta"
ALGORITMO = "HS256"
DURACION_TOKEN_ACCESO_EN_MINUTOS = 30

# Define el token predeterminado para usuarios anónimos
TOKEN_ANONIMO_POR_DEFECTO = jwt.encode({"sub": "anonimo"}, CLAVE_SECRETA, algorithm=ALGORITMO)

# Obtiener una conexión a la base de datos
def obtener_conexion_db():
    return mysql.connector.connect(**config)

# Obtener un usuario de la base de datos
def obtener_usuario(conexion_db, user_name_or_email, contrasena):
    cursor = conexion_db.cursor()
    cursor.execute("SELECT * FROM usuarios WHERE (correo_electronico=%s OR user_name=%s) AND contrasena=%s", (user_name_or_email, user_name_or_email, contrasena))
    usuario = cursor.fetchone()
    cursor.close()
    return usuario

# Verificar una contraseña
def verificar_contrasena(contrasena_texto_plano, contrasena_cifrada):
    return contexto_cifrado.verify(contrasena_texto_plano, contrasena_cifrada)

# Obtiener un hash de una contraseña
def obtener_hash_contrasena(contrasena):
    return contexto_cifrado.hash(contrasena)

# Esquema de OAuth2 para validar tokens de acceso JWT
esquema_oauth2 = OAuth2PasswordBearer(tokenUrl="token")

# Función para obtener el token de acceso
def obtener_token_acceso(nombre_usuario: str):
    fecha_expiracion = datetime.now(timezone.utc) + timedelta(minutes=DURACION_TOKEN_ACCESO_EN_MINUTOS)
    token_acceso = jwt.encode({"sub": nombre_usuario, "exp": fecha_expiracion}, CLAVE_SECRETA, algorithm=ALGORITMO)
    return token_acceso

# Función para manejar la autenticación y obtener el token
def obtener_token(token: str = Depends(esquema_oauth2)):
    if token is None:
        return TOKEN_ANONIMO_POR_DEFECTO
    else:
        return token
    
# Autentificar un usuario
def autenticar_usuario(conexion_db, user_name_o_correo, contrasena):
    usuario = obtener_usuario(conexion_db, user_name_o_correo, contrasena)
    if not usuario:
        return False
    return usuario

# Crear un token de acceso JWT
def crear_token_acceso(datos: dict, duracion_delta: timedelta | None = None):
    datos_para_codificar = datos.copy()
    if duracion_delta:
        fecha_expiracion = datetime.now(timezone.utc) + duracion_delta
    else:
        fecha_expiracion = datetime.now(timezone.utc) + timedelta(minutes=15)
    datos_para_codificar.update({"exp": fecha_expiracion})
    token_codificado = jwt.encode(datos_para_codificar, CLAVE_SECRETA, algorithm=ALGORITMO)
    return token_codificado

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

class DatosToken(BaseModel):
    nombre_usuario: str | None = None

class Token(BaseModel):
    token_acceso: str
    tipo_token: str
