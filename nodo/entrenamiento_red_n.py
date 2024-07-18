#Entrenamiento del modelo de arriba con los datos JSON

from keras.models import Sequential, Model
from keras.layers import Embedding, LSTM, Dense, TimeDistributed, Dropout, GlobalAveragePooling1D, Input, Attention, LayerNormalization, Add, Concatenate, Reshape, Lambda
from keras.preprocessing.text import Tokenizer, text_to_word_sequence
from keras.preprocessing.sequence import pad_sequences
import pickle
import numpy as np
import pandas as pd
import tensorflow as tf
import os
import re
from sklearn.model_selection import train_test_split
# Graficar la pérdida y la precisión durante el entrenamiento
import matplotlib.pyplot as plt
from tensorflow.keras import mixed_precision
from tensorflow.keras.layers import Wrapper
from tensorflow.keras.callbacks import EarlyStopping, ModelCheckpoint, ReduceLROnPlateau, TensorBoard
from keras.utils import plot_model
from tensorflow.keras.regularizers import l2
from tensorflow.keras.optimizers import AdamW
from tensorflow.keras.optimizers.schedules import ExponentialDecay
import json
import glob
from tensorflow.keras.models import load_model

# Configurar la sesión de TensorFlow para utilizar la GPU
config = tf.compat.v1.ConfigProto()
config.gpu_options.allow_growth = True
tf.compat.v1.keras.backend.set_session(tf.compat.v1.Session(config=config))


ruta_base = '/content/drive/My Drive/Red neuronal/Modelo LLM'
ruta_archivo = os.path.join(ruta_base, 'cc.es.300.vec')
ruta_base_2 = '/content/drive/My Drive/Red neuronal'
ruta_json = os.path.join(ruta_base_2, 'datos_e.json')
ruta_dataset = os.path.join(ruta_base, 'espanol.parquet')
ruta_base_3 = '/content/drive/My Drive/Red neuronal/Modelo LLM/Red LSTM'
ruta_base_4 = '/content/drive/My Drive/Red neuronal/Modelo LLM/Red LSTM/Entrenamiento'
ruta_embedding = os.path.join(ruta_base, 'modelo_reducido.vec')
ruta_tokenizer = "/content/drive/My Drive/Red neuronal/Modelo LLM/Red LSTM/tokenizer_embedding_reducido.pkl"
ruta_modelo = "/content/drive/My Drive/Red neuronal/Modelo LLM/Red LSTM/solo_modelo.h5"

ruta_hexagono = os.path.join(ruta_base_4, 'conocimiento_hexagono.json')
ruta_conversacion = os.path.join(ruta_base_4, 'conversacion_datos.json')
ruta_empresa = os.path.join(ruta_base_4, 'empresa_datos.json')
ruta_modelo_nuevaArq = "/content/drive/My Drive/Red neuronal/Modelo LLM/Red LSTM/solo_modelo_nuevaArq.h5"

# Hiperparametros del modelo
lstm_units = 64
embedding_dim = 300
num_epochs = 2
maxlen = 100
batch_size = 16

# Lista para almacenar los DataFrames de cada archivo JSON
dataframes = []

"""
# Leer todos los archivos JSON en la carpeta
for archivo in glob.glob(ruta_hexagono):
    with open(archivo, 'r') as f:
        data = json.load(f)
    df = pd.DataFrame(data)
    dataframes.append(df)
"""

# Leer todos los archivos JSON en la carpeta
for archivo in glob.glob(ruta_base_4 + '/*.json'): # Añade '/*.json' para buscar archivos JSON dentro del directorio
    with open(archivo, 'r') as f:
        data = json.load(f)
    df = pd.DataFrame(data)
    dataframes.append(df)

# Concatenar los DataFrames en uno solo
df = pd.concat(dataframes, ignore_index=True)

# Obtener las secuencias de entrada y salida
x_seq = df['Pregunta'].tolist()
y_seq = df['Respuesta'].tolist()

# Convertir las secuencias de entrada y salida a minúsculas
X_seq_lower = [text.lower() for text in x_seq]
y_seq_lower = [text.lower() for text in y_seq]

# Imprimir los primeros 5 ejemplos de las secuencias limpias
print("Primeras 5 secuencias de entrada originales:")
for text in x_seq[:5]:
    print(text)

print("\nPrimeras 5 secuencias de entrada limpias:")
for text in X_seq_lower[:5]:
    print(text)

print("\nPrimeras 5 secuencias de salida originales:")
for text in y_seq[:5]:
    print(text)

print("\nPrimeras 5 secuencias de salida limpias:")
for text in y_seq_lower[:5]:
    print(text)

# Función para tokenizar texto, separando símbolos especiales
def tokenize_with_special_chars(text):
    # Separar símbolos especiales y puntuación
    pattern = r'(\w+|[^\w\s])'
    return re.findall(pattern, text.lower())

# Crear el tokenizer personalizado
class CustomTokenizer(Tokenizer):
    def texts_to_sequences(self, texts):
        return [[self.word_index.get(word, self.word_index.get('<OOV>')) for word in tokenize_with_special_chars(text)] for text in texts]

# Cargar el tokenizer desde el archivo pickle
with open(ruta_tokenizer, 'rb') as handle:
    tokenizer = pickle.load(handle)

# Imprimir el número de palabras en el vocabulario del tokenizer
print(f"El número de palabras en el vocabulario del tokenizer es: {len(tokenizer.word_index)}")
print("aqui deberia salir la verificación de palabras")


# Cargar el embedding pre-entrenado desde el archivo .vec
embedding_index = {}
batch_sizee = 10000  # Tamaño del lote

with open(ruta_embedding, encoding='utf-8') as f:
    next(f)  # Saltar la primera línea porque solo contiene metadatos

    batch = []
    for line in f:
        batch.append(line)

        if len(batch) == batch_sizee:
            for line in batch:
                values = line.split()
                word = values[0]
                coefs = np.asarray(values[1:], dtype='float32')
                embedding_index[word] = coefs

            batch = []

    # Procesar el último lote si no es vacío
    if batch:
        for line in batch:
            values = line.split()
            word = values[0]
            coefs = np.asarray(values[1:], dtype='float32')
            embedding_index[word] = coefs

# Obtener el número de palabras en el vocabulario del embedding
num_palabras = len(embedding_index)

# Imprimir el número de palabras en el vocabulario del embedding
print(f"El número de palabras en el vocabulario del embedding es: {num_palabras}")

# Imprimir el número de palabras en el vocabulario del tokenizer
print(f"El número de palabras en el vocabulario del tokenizer antes de dataset_vocab es: {len(tokenizer.word_index)}")

# Crear un conjunto de todas las palabras en el vocabulario del dataset (tokenizer)
dataset_vocab = set(tokenizer.word_index.keys())

# Imprimir el número de palabras en el vocabulario del tokenizer
print(f"El número de palabras en el vocabulario del tokenizer después de dataset_vocab es: {len(tokenizer.word_index)}")

# Imprimir el número de palabras en el vocabulario del dataset (tokenizer)
print(f"El número de palabras en el tokenizer es: {len(dataset_vocab)}")

# Obtener las dimensiones de la matriz de embeddings
embedding_dim = next(iter(embedding_index.values())).shape[0]  # Dimensión del embedding (300 en este caso)

# Imprimir las dimensiones de la matriz de embeddings
print(f"Las dimensiones de la matriz de embeddings son: ({num_palabras}, {embedding_dim})")

# Asignar la dimensión de salida
output_dim = embedding_dim
print(f"La dimensión de salida de los embeddings es: {output_dim}")

#Hacer sólo un corpus
texts = x_seq + y_seq

# Tokenización de y_seq y x_seq
x_seq_num = tokenizer.texts_to_sequences(x_seq)
y_seq_num = tokenizer.texts_to_sequences(y_seq)

# Imprimir los primeros 5 ejemplos de las secuencias limpias
print("Primeras 5 secuencias de entrada originales:")
for text in x_seq[:5]:
    print(text)

print("\nPrimeras 5 secuencias de entrada tokenizadas:")
for text in x_seq_num[:5]:
    print(text)

print("\nPrimeras 5 secuencias de salida originales:")
for text in y_seq[:5]:
    print(text)

print("\nPrimeras 5 secuencias de salida tokenizadas:")
for text in y_seq_num[:5]:
    print(text)

x_seq_lengths = [len(seq) for seq in x_seq_num]
y_seq_lengths = [len(seq) for seq in y_seq_num]

# Grafica el histograma
plt.hist(x_seq_lengths, bins=50, alpha=0.5, label='Entradas')
plt.hist(y_seq_lengths, bins=50, alpha=0.5, label='Salidas')
plt.xlabel('Longitud de Secuencia')
plt.ylabel('Frecuencia')
plt.legend()
plt.show()

maxlen_x = max(x_seq_lengths)
maxlen_y = max(y_seq_lengths)

# Imprime los valores calculados
print(f"maxlen para entradas: {maxlen_x}")
print(f"maxlen para salidas: {maxlen_y}")

maxlen = max(maxlen_x, maxlen_y)

print(f"maxlen final: {maxlen}")

maxlen += 10

print(f"maxlen final += 10: {maxlen}")

# Imprimir el número de palabras en el vocabulario del tokenizer antes de crear la matriz de embeddings
print(f"El número de palabras en el vocabulario del tokenizer antes del preprocesamiento de datos es: {len(tokenizer.word_index)}")

# Preprocesamiento de datos
#tokenizer.fit_on_texts(texts)
# Tokenización de X_seq_lower
X_seq_tokenized = tokenizer.texts_to_sequences(X_seq_lower)

# Tokenización de Y_seq_lower
y_seq_tokenized = tokenizer.texts_to_sequences(y_seq_lower)

# Verificar si la palabra "antibióticos" está en el vocabulario del tokenizer
if "antibióticos" in tokenizer.word_index:
    print("La palabra 'antibióticos' está en el vocabulario del tokenizer.")
else:
    print("La palabra 'antibióticos' no está en el vocabulario del tokenizer.")

# Convertir las secuencias en arrays de numpy
x_seq_num_padded = pad_sequences(x_seq_num, maxlen=maxlen, padding='post')
y_seq_num_padded = pad_sequences(y_seq_num, maxlen=maxlen, padding='post')

# Verificar las dimensiones después del padding
print("Dimensiones de las secuencias después del padding:")
print(f"X_seq_num_padded.shape: {x_seq_num_padded.shape}")
print(f"y_seq_num_padded.shape: {y_seq_num_padded.shape}")

# Imprimir muestras de los datos preprocesados
for i in range(5):
    print(f"Secuencia de entrada {i+1}: {x_seq_num_padded[i]}")
    print(f"Secuencia de salida {i+1}: {y_seq_num_padded[i]}")

# Dividir los datos en conjuntos de prueba y entrenamiento
x_train, x_test, y_train, y_test = train_test_split(x_seq_num_padded, y_seq_num_padded, test_size=0.2, random_state=42)

# Asegurarse de que los tipos de datos sean correctos
x_train = x_train.astype('int32')
x_test = x_test.astype('int32')
y_train = y_train.astype('int32')
y_test = y_test.astype('int32')

# Verificar las dimensiones de los conjuntos de entrenamiento y prueba
print(f"x_train.shape: {x_train.shape}")
print(f"x_test.shape: {x_test.shape}")
print(f"y_train.shape: {y_train.shape}")
print(f"y_test.shape: {y_test.shape}")

# Crear una matriz de embeddings utilizando los coeficientes del diccionario y el vocabulario del tokenizer
embedding_matrix = np.zeros((num_palabras, embedding_dim))
batch_sizee = 1000  # Tamaño del lote para el procesamiento de embeddings

# Contadores para palabras sin embedding y palabras fuera de rango
palabras_sin_embedding = 0
palabras_fuera_de_rango = 0

# Imprimir el número de palabras en el vocabulario del tokenizer antes de crear la matriz de embeddings
print(f"El número de palabras en el vocabulario del tokenizer antes de crear la matriz de embeddings es: {len(tokenizer.word_index)}")

for i, (word, index) in enumerate(tokenizer.word_index.items()):
    if index < num_palabras:
        embedding_vector = embedding_index.get(word)
        if embedding_vector is not None:
            embedding_matrix[index] = embedding_vector
        else:
            palabras_sin_embedding += 1
    else:
        palabras_fuera_de_rango += 1

    # Actualizar la matriz de embeddings en lotes más pequeños
    if (i + 1) % batch_sizee == 0:
        print(f"Procesando el lote {(i + 1) // batch_sizee} de {len(tokenizer.word_index) // batch_sizee + 1}")

# Procesar el último lote si es necesario
if len(tokenizer.word_index) % batch_sizee != 0:
    print(f"Procesando el lote final de {len(tokenizer.word_index) // batch_sizee + 1}")

# Imprimir el número de palabras en el vocabulario del tokenizer después de crear la matriz de embeddings
print(f"El número de palabras en el vocabulario del tokenizer después de crear la matriz de embeddings es: {len(tokenizer.word_index)}")

# Imprimir los contadores de palabras sin embedding y palabras fuera de rango
print(f"Número de palabras sin embedding: {palabras_sin_embedding}")
print(f"Número de palabras fuera de rango: {palabras_fuera_de_rango}")

# Guardar la matriz de embeddings en la ruta especificada
np.save(os.path.join(ruta_base_3, 'embedding_matrix.npy'), embedding_matrix)

# Habilitar el gradient checkpointing
tf.config.experimental_run_functions_eagerly(True)

# Habilitar el entrenamiento de precisión mixta
policy = tf.keras.mixed_precision.Policy('mixed_float16')
tf.keras.mixed_precision.set_global_policy(policy)

# Definir la función nuevamente
def maximum_inner_product_search(query_embeddings, embedding_matrix):
    query_embeddings = tf.cast(query_embeddings, tf.int32)
    scores = tf.matmul(query_embeddings, embedding_matrix, transpose_b=True)
    mips_indices = tf.argmax(scores, axis=-1)
    return mips_indices

# Convertir la matriz de embeddings a tensor
embedding_matrix_tensor = tf.convert_to_tensor(embedding_matrix, dtype=tf.int32)

# Cargar el modelo
custom_objects = {'maximum_inner_product_search': maximum_inner_product_search}

# Cargar el modelo
modelo = load_model(ruta_modelo_nuevaArq, custom_objects=custom_objects)

# Resumen del modelo
modelo.summary()

# Compilación del modelo
modelo.compile(optimizer='adam',
              loss='sparse_categorical_crossentropy',
              metrics=['accuracy'])

# Preparar el callback para guardar checkpoints usando ruta_base_3
checkpoint = ModelCheckpoint(f'{ruta_base_3}checkpoint-{{epoch:02d}}.h5',
                             save_best_only=True,
                             save_weights_only=False,
                             monitor='val_loss',
                             mode='min')

# Entrenar el modelo
modelo.fit(x_train, y_train, epochs=10, batch_size=16, validation_split=0.2, callbacks=[checkpoint])

# Guardar el modelo entrenado en un archivo .h5
modelo.save(os.path.join(ruta_base_3, 'chatbot_model.h5'))

# Guardar los pesos del modelo en un archivo HDF5
modelo.save_weights(os.path.join(ruta_base_3, "pesos_modelo_nuevaArq.h5"))
