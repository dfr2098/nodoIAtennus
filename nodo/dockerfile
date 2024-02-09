# Usa una versión específica de la imagen base de Anaconda
FROM continuumio/anaconda3:2020.11

# Establece el directorio de trabajo en /nodo
WORKDIR /nodo

# Copia el contenido de la carpeta 'nodo' y el archivo requirements.txt al directorio '/nodo' dentro del contenedor
COPY nodo .
COPY requirements.txt .

# Instala las dependencias especificadas en requirements.txt y limpia la caché de pip
RUN pip install --no-cache-dir -r requirements.txt

# Define la variable de entorno PATH para que Anaconda pueda encontrar los ejecutables instalados
ENV PATH="/opt/conda/bin:${PATH}"

# Crea un usuario no root para ejecutar Jupyter
RUN useradd -ms /bin/bash jupyteruser
USER jupyteruser

# Exponer el puerto utilizado por tu aplicación
EXPOSE 8888

# Comando para iniciar tu aplicación
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888"]