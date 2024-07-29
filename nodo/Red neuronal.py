#NUEVO MODELO CON EL DATASET JSON ESPECIFICO 

from keras.models import Sequential, Model
from keras.layers import Embedding, LSTM, Dense, TimeDistributed, Dropout, GlobalAveragePooling1D, Input, Attention, LayerNormalization, Add, Concatenate, Reshape, Lambda, Activation, Bidirectional, GRU, Conv1D, MaxPooling1D
from keras.preprocessing.text import Tokenizer, text_to_word_sequence
from keras.preprocessing.sequence import pad_sequences
import pickle
import numpy as np
import pandas as pd
import tensorflow as tf
import os
import re
import shutil
from sklearn.model_selection import train_test_split
# Graficar la pérdida y la precisión durante el entrenamiento
import matplotlib.pyplot as plt
from tensorflow.keras import mixed_precision
from tensorflow.keras.layers import Wrapper
from tensorflow.keras.callbacks import EarlyStopping, ModelCheckpoint, ReduceLROnPlateau, TensorBoard
from keras.utils import plot_model
from tensorflow.keras.regularizers import l2
from tensorflow.keras.optimizers import AdamW, Adam
from tensorflow.keras.optimizers.schedules import ExponentialDecay
import json
import glob
from tensorflow.keras.initializers import glorot_uniform, he_normal


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
#ruta_embedding = os.path.join(ruta_base, 'modelo_reducido.vec')
#ruta_tokenizer = "/content/drive/My Drive/Red neuronal/Modelo LLM/Red LSTM/tokenizer_modelo_reducido.pkl"
ruta_modelo = "/content/drive/My Drive/Red neuronal/Modelo LLM/Red LSTM/solo_modelo.h5"

ruta_hexagono = os.path.join(ruta_base_4, 'conocimiento_hexagono.json')
ruta_conversacion = os.path.join(ruta_base_4, 'conversacion_datos.json')
ruta_empresa = os.path.join(ruta_base_4, 'empresa_datos.json')

#rutas extras
# Ruta tokenizer nueva
ruta_tokenizer = os.path.join(ruta_base_3, 'tokenizer_embedding_mejorado.pkl')

# Ruta del archivo de embeddings
ruta_embedding = '/content/drive/My Drive/Red neuronal/Modelo LLM/modelo_reducido_con_OOV.vec'

#Ruta de la matriz de numpy
ruta_archivo_numpy = os.path.join(ruta_base_3, 'embedding_matrix_ultima.npy')

# Hiperparametros del modelo
lstm_units = 128
embedding_dim = 300
num_epochs = 20
maxlen = 211
batch_size = 16
num_palabras = 100000

# Lista para almacenar los DataFrames de cada archivo JSON
dataframes = []

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

# Función de tokenización que maneja caracteres especiales
def tokenize_with_special_chars(text):
    pattern = r'(\w+|[^\w\s])'
    return re.findall(pattern, text.lower())

class CustomTokenizer(Tokenizer):
    def __init__(self, embeddings_index, **kwargs):
        super().__init__(**kwargs)
        self.embeddings_index = embeddings_index
        self.word_index = {word: i for i, word in enumerate(embeddings_index.keys(), start=1)}
        self.index_word = {v: k for k, v in self.word_index.items()}

    def texts_to_sequences(self, texts):
        return [[self.word_index.get(word, self.word_index['<OOV>'])
                 for word in tokenize_with_special_chars(text)]
                for text in texts]

    def get_vocab_size(self):
        return len(self.word_index)

# Cargar el tokenizer desde el archivo pickle
with open(ruta_tokenizer, 'rb') as handle:
    tokenizer = pickle.load(handle)

# Imprimir el número de palabras en el vocabulario del tokenizer
print(f"El número de palabras en el vocabulario del tokenizer es: {len(tokenizer.word_index)}")
print("aqui deberia salir la verificación de palabras")


# Tokenización de X_seq_lower
X_seq_tokenized = tokenizer.texts_to_sequences(X_seq_lower)

# Tokenización de Y_seq_lower
y_seq_tokenized = tokenizer.texts_to_sequences(y_seq_lower)

# Verificar si la palabra "antibióticos" está en el vocabulario del tokenizer
if "antibióticos" in tokenizer.word_index:
    print("La palabra 'antibióticos' está en el vocabulario del tokenizer.")
else:
    print("La palabra 'antibióticos' no está en el vocabulario del tokenizer.")

# Verificar si la palabra "infecciones" está en el vocabulario del tokenizer
if "infecciones" in tokenizer.word_index:
    print("La palabra 'infecciones' está en el vocabulario del tokenizer.")
else:
    print("La palabra 'infecciones' no está en el vocabulario del tokenizer.")

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

# Asegurarse de que el vocabulario cubra todos los índices desde 0 hasta num_palabras - 1
missing_indices = set(range(num_palabras)) - set(tokenizer.word_index.values())
if missing_indices:
    print(f"Índices faltantes en tokenizer.word_index: {missing_indices}")
else:
    print("Todos los índices están presentes en tokenizer.word_index.")

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
sequences = tokenizer.texts_to_sequences(texts)

# Imprimir el número de palabras en el vocabulario del tokenizer antes de crear la matriz de embeddings
print(f"El número de palabras en el vocabulario del tokenizer después del preprocesamiento de datos es: {len(tokenizer.word_index)}")

x_seq_num_padded = pad_sequences(x_seq_num, maxlen=maxlen, padding='post')
y_seq_num_padded = pad_sequences(y_seq_num, maxlen=maxlen, padding='post')

# Verificar las dimensiones después del padding
print("Dimensiones de las secuencias después del padding:")
print(f"X_seq_num_padded.shape: {x_seq_num_padded.shape}")
print(f"y_seq_num_padded.shape: {y_seq_num_padded.shape}")


# Imprimir muestras de los datos preprocesados
# Imprimir las primeras 5 secuencias de entrada preprocesadas
for i in range(5):
    print(f"Secuencia de entrada {i+1}: {x_seq_num_padded[i]}")

# Imprimir las primeras 5 secuencias de salida preprocesadas
for i in range(5):
    print(f"Secuencia de salida {i+1}: {y_seq_num_padded[i]}")

# Divide los datos en conjuntos de prueba y entrenamiento
x_train, x_test, y_train, y_test = train_test_split(x_seq_num_padded, y_seq_num_padded, test_size=0.2, random_state=42)

#num_palabras = 100001

num_palabras = tokenizer.get_vocab_size()

embedding_matrix = np.load(ruta_archivo_numpy)
print(f"Forma de la matriz de embedding cargada: {embedding_matrix.shape}")

# Habilitar el gradient checkpointing
tf.config.experimental_run_functions_eagerly(True)

# Habilitar el entrenamiento de precisión mixta
policy = tf.keras.mixed_precision.Policy('mixed_float16')
tf.keras.mixed_precision.set_global_policy(policy)

""" Primera versión del modelo
# Capa de embedding y entrada del modelo
entrada = Input(shape=(maxlen,), name='entrada')
capa_embedding = Embedding(input_dim=num_palabras, output_dim=embedding_dim, weights=[embedding_matrix], trainable=True, mask_zero=True, dtype='float32', name='capa_embedding')(entrada)
capa_dropout_1 = Dropout(0.2, name='capa_dropout_1')(capa_embedding)

# Capas LSTM
capa_lstm_1 = LSTM(units=lstm_units, return_sequences=True, name='capa_lstm_1')(capa_dropout_1)
capa_lstm_2 = LSTM(units=lstm_units, return_sequences=False, name='capa_lstm_2')(capa_lstm_1)

# Normalización y Dropout
capa_normalizacion = LayerNormalization(name='capa_normalizacion')(capa_lstm_2)
capa_dropout_2 = Dropout(0.2, name='capa_dropout_2')(capa_normalizacion)

# Concatenación y Atención
capa_concatenacion = Concatenate(name='capa_concatenacion')([capa_lstm_1, capa_lstm_1])
capa_atencion = Attention(name='capa_atencion')([capa_concatenacion, capa_concatenacion])

# Capa densa distribuida y Dropout
capa_densa_distribuida = TimeDistributed(Dense(units=embedding_dim), name='capa_densa_distribuida')(capa_atencion)
capa_dropout_3 = Dropout(0.2, name='capa_dropout_3')(capa_densa_distribuida)

# Capa densa final y softmax distribuido
capa_densa_final = TimeDistributed(Dense(units=num_palabras), name='capa_densa_final')(capa_dropout_3)
salida_respuesta = tf.keras.layers.Activation('softmax', name='salida_respuesta')(capa_densa_final)

# Modelo final
modelo = Model(inputs=entrada, outputs=salida_respuesta, name='Modelo_LSTM')
"""

""" Segunda versión del modelo
# Capa de embedding y entrada del modelo
entrada = Input(shape=(maxlen,), name='entrada')
# Capa de embedding con inicializador glorot_uniform
capa_embedding = Embedding(input_dim=num_palabras, output_dim=embedding_dim,
                           weights=[embedding_matrix], trainable=False,
                           mask_zero=True, dtype='float32',
                           name='capa_embedding',
                           embeddings_initializer=glorot_uniform())(entrada)
capa_dropout_1 = Dropout(0.3, name='capa_dropout_1')(capa_embedding)

# Capas LSTM
# Capas LSTM con inicializador he_normal
capa_lstm_1 = LSTM(units=lstm_units, return_sequences=True,
                   kernel_initializer=he_normal(),
                   recurrent_initializer=he_normal(),
                   name='capa_lstm_1')(capa_dropout_1)
capa_lstm_2 = LSTM(units=lstm_units, return_sequences=True, name='capa_lstm_2')(capa_lstm_1)  # Cambiado a return_sequences=True

# Normalización y Dropout
capa_normalizacion = LayerNormalization(name='capa_normalizacion')(capa_lstm_2)
capa_dropout_2 = Dropout(0.3, name='capa_dropout_2')(capa_normalizacion)

# Concatenación y Atención
capa_concatenacion = Concatenate(name='capa_concatenacion')([capa_lstm_1, capa_lstm_2])
capa_atencion = Attention(name='capa_atencion')([capa_concatenacion, capa_concatenacion])

# Capa densa distribuida y Dropout
capa_densa_distribuida = TimeDistributed(Dense(units=embedding_dim), name='capa_densa_distribuida')(capa_atencion)
capa_dropout_3 = Dropout(0.3, name='capa_dropout_3')(capa_densa_distribuida)

# Capa densa final y softmax distribuido
# Capa densa con L2 regularization
capa_densa_final = TimeDistributed(Dense(units=num_palabras,
                                         kernel_regularizer=l2(0.01)),
                                   name='capa_densa_final')(capa_dropout_3)
salida_respuesta = tf.keras.layers.Activation('softmax', name='salida_respuesta')(capa_densa_final)

# Modelo final
modelo = Model(inputs=entrada, outputs=salida_respuesta, name='Modelo_LSTM')

# Optimizador AdamW con gradient clipping
optimizer = AdamW(learning_rate=0.001, weight_decay=1e-5, clipnorm=1.0)

# Compilar el modelo con el nuevo optimizador
modelo.compile(optimizer=optimizer,
               loss='sparse_categorical_crossentropy',
               metrics=['accuracy'])

# Resumen del modelo
modelo.summary()

# Guardar el modelo en un archivo HDF5
modelo.save(os.path.join(ruta_base_3, "solo_modelo_nuevaArq.h5"))
"""

# Capa de embedding y entrada del modelo
entrada = Input(shape=(maxlen,), name='entrada')
capa_embedding = Embedding(input_dim=num_palabras, output_dim=embedding_dim,
                           weights=[embedding_matrix], trainable=True,
                           mask_zero=True, dtype='float32',
                           name='capa_embedding',
                           embeddings_initializer=glorot_uniform())(entrada)
capa_dropout_1 = Dropout(0.3, name='capa_dropout_1')(capa_embedding)

# Capa convolucional
capa_conv = Conv1D(filters=256, kernel_size=3, padding='same', activation='relu', name='capa_conv')(capa_dropout_1)
capa_pooling = MaxPooling1D(pool_size=1, padding='same', name='capa_pooling')(capa_conv)

# Capas LSTM bidireccionales
capa_lstm_1 = Bidirectional(LSTM(units=lstm_units, return_sequences=True,
                   kernel_initializer=he_normal(),
                   recurrent_initializer=he_normal(),
                   name='capa_lstm_1'))(capa_pooling)
capa_lstm_2 = Bidirectional(LSTM(units=lstm_units, return_sequences=True, name='capa_lstm_2'))(capa_lstm_1)
capa_lstm_3 = Bidirectional(LSTM(units=lstm_units, return_sequences=True, name='capa_lstm_3'))(capa_lstm_2)

# Normalización y Dropout
capa_normalizacion = LayerNormalization(name='capa_normalizacion')(capa_lstm_3)
capa_dropout_2 = Dropout(0.3, name='capa_dropout_2')(capa_normalizacion)

# Concatenación y Atención
capa_concatenacion = Concatenate(name='capa_concatenacion')([capa_lstm_1, capa_lstm_2, capa_lstm_3])
capa_atencion = Attention(name='capa_atencion')([capa_concatenacion, capa_concatenacion])

# Capa densa distribuida y Dropout
capa_densa_distribuida = TimeDistributed(Dense(units=embedding_dim,
                                               kernel_regularizer=l2(0.001),
                                               activation='relu'),
                                         name='capa_densa_distribuida')(capa_atencion)
capa_dropout_3 = Dropout(0.3, name='capa_dropout_3')(capa_densa_distribuida)

# Capa densa final y softmax distribuido
capa_densa_final = TimeDistributed(Dense(units=num_palabras,
                                         kernel_regularizer=l2(0.001)),
                                   name='capa_densa_final')(capa_dropout_3)
salida_respuesta = Activation('softmax', name='salida_respuesta')(capa_densa_final)

# Modelo final
modelo = Model(inputs=entrada, outputs=salida_respuesta, name='Modelo_LSTM_Mejorado')

# Optimizador y compilación
optimizer = AdamW(learning_rate=0.0002, weight_decay=1e-5, clipnorm=1.0)

modelo.compile(optimizer=optimizer,
               loss='sparse_categorical_crossentropy',
               metrics=['accuracy'])

# Resumen del modelo
modelo.summary()

# Guardar el modelo en un archivo HDF5
modelo.save(os.path.join(ruta_base_3, "solo_modelo_nuevaArq.h5"))

# Visualizar y guardar la arquitectura del modelo en la ruta especificada en la variable "ruta_base_3"
plot_model(modelo, to_file=ruta_base_3 + 'modelo_plot.png', show_shapes=True, show_layer_names=True)