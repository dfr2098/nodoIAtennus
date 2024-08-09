#USO DEL MODELO, Inferencia

from keras.models import Sequential, Model
from keras.layers import Embedding, LSTM, Dense, TimeDistributed, Dropout, GlobalAveragePooling1D, Input, Attention, LayerNormalization, Add, Concatenate, Reshape, Lambda, Activation, Bidirectional, GRU, Conv1D, MaxPooling1D, SpatialDropout1D
from tensorflow.keras.preprocessing.text import Tokenizer, text_to_word_sequence
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
from tensorflow.keras.callbacks import EarlyStopping, ModelCheckpoint, ReduceLROnPlateau, TensorBoard, LearningRateScheduler
from keras.utils import plot_model
from tensorflow.keras.regularizers import l1, l2, l1_l2
from tensorflow.keras.optimizers import AdamW, Adam
from tensorflow.keras.optimizers.schedules import ExponentialDecay
import json
import glob
from tensorflow.keras.models import load_model
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

ruta_modelo_nuevaArq = os.path.join(ruta_base_3, 'solo_modelo_nuevaArq.keras')

ruta_modelo_entrenado = os.path.join(ruta_base_3, 'chatbot_model.keras')
ruta_modelo_pesos = os.path.join(ruta_base_3, "pesos_modelo_nuevaArq.keras")

# Hiperparametros del modelo
lstm_units = 128
embedding_dim = 300
num_epochs = 20
maxlen = 211
batch_size = 16
num_palabras = 100000

#tf.keras.models.load_model

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

# Cargar el modelo
modelo = tf.keras.models.load_model(ruta_modelo_entrenado, custom_objects={'AdamW': AdamW, 'Softmax': tf.keras.layers.Activation('softmax')})

# Si se necesitan cargar los pesos
#pesos  = modelo.load_weights(os.path.join(ruta_base_3, "pesos_modelo_nuevaArq.keras"))

# Resumen del modelo
modelo.summary()

#Optimizador
optimizer = AdamW(learning_rate=0.0002, weight_decay=1e-5, clipnorm=1.0)

# Recompilar el modelo con el nuevo optimizador
modelo.compile(optimizer=optimizer,
               loss='sparse_categorical_crossentropy',
               metrics=['accuracy'])
"""
# Función para generar texto
def generar_texto(modelo, texto_inicial, num_palabras_generar, temperatura=0.7):
    texto_generado = texto_inicial

    for _ in range(num_palabras_generar):
        # Tokenizar el texto generado hasta ahora
        tokens_entrada = tokenize_with_special_chars(texto_generado)

        # Convertir tokens a secuencias de índices
        secuencia = tokenizer.texts_to_sequences([tokens_entrada])[0]

        # Asegurarse de que la entrada tenga la longitud correcta
        secuencia = secuencia[-maxlen:]
        secuencia = tf.keras.preprocessing.sequence.pad_sequences([secuencia], maxlen=maxlen, padding='pre')

        # Predecir la siguiente palabra
        predicciones = modelo.predict(secuencia)[0][-1]

        # Aplicar temperatura a las predicciones
        predicciones = np.log(predicciones) / temperatura
        exp_predicciones = np.exp(predicciones)
        predicciones = exp_predicciones / np.sum(exp_predicciones)

        # Seleccionar la siguiente palabra
        indice_proximo = np.random.choice(len(predicciones), p=predicciones)

        # Convertir el índice a palabra y añadirla al texto generado
        palabra_siguiente = tokenizer.index_word.get(indice_proximo, '<OOV>')
        texto_generado += " " + palabra_siguiente

    return texto_generado

# Usar la función para generar texto
texto_inicial = "Hola"
num_palabras_generar = 5  # Ajustar este valor para generar más o menos palabras
temperatura = 0.7  # Ajustar este valor para controlar la aleatoriedad de las predicciones

texto_generado = generar_texto(modelo, texto_inicial, num_palabras_generar, temperatura)
print(texto_generado)
"""

"""
def generar_texto_dinamico(modelo, texto_inicial, max_palabras=200, temperatura=temperatura, umbral_confianza=umbral_confianza):
    texto_generado = texto_inicial
    palabras_generadas = 0
    puntuacion_final = {'.', '!', '?'}  # Conjunto de puntuaciones que podrían indicar el final de una oración

    while palabras_generadas < max_palabras:
        # Tokenizar el texto generado hasta ahora
        tokens_entrada = tokenize_with_special_chars(texto_generado)

        # Convertir tokens a secuencias de índices
        secuencia = tokenizer.texts_to_sequences([tokens_entrada])[0]

        # Asegurarse de que la entrada tenga la longitud correcta
        secuencia = secuencia[-maxlen:]
        secuencia = tf.keras.preprocessing.sequence.pad_sequences([secuencia], maxlen=maxlen, padding='pre')

        # Predecir la siguiente palabra
        predicciones = modelo.predict(secuencia)[0][-1]

        # Aplicar temperatura a las predicciones
        predicciones = np.log(predicciones) / temperatura
        exp_predicciones = np.exp(predicciones)
        predicciones = exp_predicciones / np.sum(exp_predicciones)

        # Seleccionar la siguiente palabra
        indice_proximo = np.random.choice(len(predicciones), p=predicciones)

        # Convertir el índice a palabra
        palabra_siguiente = tokenizer.index_word.get(indice_proximo, '<OOV>')

        # Verificar condiciones de terminación
        max_probabilidad = np.max(predicciones)
        if (palabra_siguiente in puntuacion_final and palabras_generadas > 10) or \
           (max_probabilidad < umbral_confianza) or \
           (palabras_generadas >= max_palabras):
            texto_generado += palabra_siguiente
            break

        # Añadir la palabra al texto generado
        texto_generado += " " + palabra_siguiente
        palabras_generadas += 1

    return texto_generado

# Usar la función para generar texto
texto_inicial = "Hola"
max_palabras = 200  # Límite máximo por seguridad
temperatura = 0.7 # Ajustar este valor para controlar la aleatoriedad de las predicciones
umbral_confianza = 0.1  # Inseguridad del modelo para seguir creando palabras

texto_generado = generar_texto_dinamico(modelo, texto_inicial, max_palabras, temperatura, umbral_confianza)
print(texto_generado)
"""

"""
def generar_texto_dinamico(modelo, texto_inicial, tokenizer, maxlen, max_palabras=200, temperatura=1.0, umbral_confianza=0.1):
    if isinstance(texto_inicial, str):
        tokens_entrada = tokenize_with_special_chars(texto_inicial)
    elif isinstance(texto_inicial, list):
        tokens_entrada = texto_inicial
    else:
        raise ValueError("texto_inicial debe ser una cadena de texto o una lista de tokens")

    texto_generado = " ".join(tokens_entrada)
    palabras_generadas = 0
    puntuacion_final = {'.', '!', '?'}

    while palabras_generadas < max_palabras:
        # Convertir tokens a secuencias de índices
        secuencia = tokenizer.texts_to_sequences([" ".join(tokens_entrada)])[0]

        # Asegurarse de que la entrada tenga la longitud correcta
        secuencia = secuencia[-maxlen:]
        secuencia = tf.keras.preprocessing.sequence.pad_sequences([secuencia], maxlen=maxlen, padding='pre')

        # Generar la siguiente palabra
        predicciones = modelo(secuencia, training=False)
        predicciones = predicciones[0, -1, :]  # Tomar las predicciones para la última posición

        # Aplicar temperatura
        predicciones = predicciones / temperatura
        predicciones = tf.nn.softmax(predicciones).numpy()

        # Seleccionar la siguiente palabra
        indice_proximo = np.random.choice(len(predicciones), p=predicciones)

        # Convertir el índice a palabra
        palabra_siguiente = tokenizer.index_word.get(indice_proximo, '<OOV>')

        # Verificar condiciones de terminación
        max_probabilidad = np.max(predicciones)
        if (palabra_siguiente in puntuacion_final and palabras_generadas > 10) or \
           (max_probabilidad < umbral_confianza):
            tokens_entrada.append(palabra_siguiente)
            texto_generado += " " + palabra_siguiente
            break

        # Añadir la palabra al texto generado
        tokens_entrada.append(palabra_siguiente)
        texto_generado += " " + palabra_siguiente
        palabras_generadas += 1

        # Opcional: Imprimir información sobre la confianza del modelo
        if max_probabilidad < umbral_confianza:
            print(f"Baja confianza detectada: {max_probabilidad:.4f}")

    return texto_generado

# Usar la función para generar texto
texto_inicial = "adiós"
max_palabras = 200 # Límite máximo por seguridad
temperatura = 1.0 # Ajustar este valor para controlar la aleatoriedad de las predicciones
umbral_confianza = 0.1 # Inseguridad del modelo para seguir creando palabras

texto_generado = generar_texto_dinamico(modelo, texto_inicial, tokenizer, maxlen, max_palabras, temperatura, umbral_confianza)
print(texto_generado)
"""

"""
def generar_texto_dinamico(modelo, texto_inicial, tokenizer, maxlen, max_palabras=200, temperatura=0.7, umbral_confianza=0.1):
    if isinstance(texto_inicial, str):
        tokens_entrada = tokenize_with_special_chars(texto_inicial)
    elif isinstance(texto_inicial, list):
        tokens_entrada = texto_inicial
    else:
        raise ValueError("texto_inicial debe ser una cadena de texto o una lista de tokens")

    print(f"Tokens iniciales: {tokens_entrada}")

    texto_generado = " ".join(tokens_entrada)
    palabras_generadas = 0
    puntuacion_final = {'.', '!', '?'}

    while palabras_generadas < max_palabras:
        # Convertir tokens a secuencias de índices
        secuencia = tokenizer.texts_to_sequences([" ".join(tokens_entrada)])[0]
        print(f"Secuencia de índices: {secuencia}")

        # Asegurarse de que la entrada tenga la longitud correcta
        secuencia = secuencia[-maxlen:]
        secuencia = tf.keras.preprocessing.sequence.pad_sequences([secuencia], maxlen=maxlen, padding='post')
        print(f"Secuencia padded: {secuencia}")

        # Generar la siguiente palabra
        predicciones = modelo(secuencia, training=False)
        predicciones = predicciones[0, -1, :]  # Tomar las predicciones para la última posición

        # Aplicar temperatura
        predicciones = predicciones / temperatura
        predicciones = tf.nn.softmax(predicciones).numpy()

        # Seleccionar la siguiente palabra
        indice_proximo = np.random.choice(len(predicciones), p=predicciones)

        # Convertir el índice a palabra
        palabra_siguiente = tokenizer.index_word.get(indice_proximo, '<OOV>')
        print(f"Palabra generada: {palabra_siguiente}")

        # Verificar condiciones de terminación
        max_probabilidad = np.max(predicciones)
        print(f"Probabilidad máxima: {max_probabilidad:.4f}")

        if (palabra_siguiente in puntuacion_final and palabras_generadas > 10) or \
           (max_probabilidad < umbral_confianza):
            tokens_entrada.append(palabra_siguiente)
            texto_generado += " " + palabra_siguiente
            print("Condición de terminación alcanzada")
            break

        # Añadir la palabra al texto generado
        tokens_entrada.append(palabra_siguiente)
        texto_generado += " " + palabra_siguiente
        palabras_generadas += 1

        # Opcional: Imprimir información sobre la confianza del modelo
        if max_probabilidad < umbral_confianza:
            print(f"Baja confianza detectada: {max_probabilidad:.4f}")

    return texto_generado

# Usar la función para generar texto
texto_inicial = "Hola"
max_palabras = 200
temperatura = 0.7
umbral_confianza = 0.001

texto_generado = generar_texto_dinamico(modelo, texto_inicial, tokenizer, maxlen, max_palabras, temperatura, umbral_confianza)
print("Texto generado final:")
print(texto_generado)
"""


def generar_texto_dinamico(modelo, texto_inicial, tokenizer, maxlen, max_palabras=200, temperatura=1.0, umbral_confianza=0.01):
    if isinstance(texto_inicial, str):
        tokens_entrada = tokenize_with_special_chars(texto_inicial)
    elif isinstance(texto_inicial, list):
        tokens_entrada = texto_inicial
    else:
        raise ValueError("texto_inicial debe ser una cadena de texto o una lista de tokens")

    print(f"Tokens iniciales: {tokens_entrada}")

    texto_generado = " ".join(tokens_entrada)
    palabras_generadas = 0
    puntuacion_final = {'.', '!', '?'}

    while palabras_generadas < max_palabras:
        # Convertir tokens a secuencias de índices
        secuencia = tokenizer.texts_to_sequences([" ".join(tokens_entrada)])[0]
        print(f"Secuencia de índices: {secuencia}")

        # Asegurarse de que la entrada tenga la longitud correcta
        secuencia = secuencia[-maxlen:]
        secuencia = tf.keras.preprocessing.sequence.pad_sequences([secuencia], maxlen=maxlen, padding='post')
        print(f"Secuencia padded: {secuencia}")

        # Generar la siguiente palabra
        predicciones = modelo.predict(secuencia)[0, -1, :]

        # Aplicar temperatura
        predicciones = np.log(predicciones) / temperatura
        exp_preds = np.exp(predicciones)
        predicciones = exp_preds / np.sum(exp_preds)

        # Seleccionar la siguiente palabra
        indice_proximo = np.random.choice(len(predicciones), p=predicciones)

        # Convertir el índice a palabra
        palabra_siguiente = tokenizer.index_word.get(indice_proximo, '<OOV>')
        print(f"Palabra generada: {palabra_siguiente}")

        # Verificar condiciones de terminación
        max_probabilidad = np.max(predicciones)
        print(f"Probabilidad máxima: {max_probabilidad:.4f}")

        if (palabra_siguiente in puntuacion_final and palabras_generadas > 10) or \
           (max_probabilidad < umbral_confianza):
            tokens_entrada.append(palabra_siguiente)
            texto_generado += " " + palabra_siguiente
            print("Condición de terminación alcanzada")
            break

        # Añadir la palabra al texto generado
        tokens_entrada.append(palabra_siguiente)
        texto_generado += " " + palabra_siguiente
        palabras_generadas += 1

    return texto_generado

# Usar la función para generar texto
texto_inicial = "hola"
max_palabras = 200
temperatura = 1.0
umbral_confianza = 0.01

texto_generado = generar_texto_dinamico(modelo, texto_inicial, tokenizer, maxlen, max_palabras, temperatura, umbral_confianza)
print("Texto generado final:")
print(texto_generado)


"""
def generar_texto_dinamico(modelo, texto_inicial, tokenizer, maxlen, max_palabras=50, temperatura=1.0, umbral_confianza=0.01):
    tokens_entrada = texto_inicial.split() if isinstance(texto_inicial, str) else texto_inicial
    texto_generado = " ".join(tokens_entrada)
    palabras_generadas = 0

    while palabras_generadas < max_palabras:
        # Convertir tokens a secuencias de índices
        secuencia = tokenizer.texts_to_sequences([" ".join(tokens_entrada)])[-maxlen:]
        secuencia = tf.keras.preprocessing.sequence.pad_sequences(secuencia, maxlen=maxlen, padding='pre')

        # Generar la siguiente palabra
        predicciones = modelo.predict(secuencia, verbose=0)[0][-1]  # Tomar las predicciones para el último token

        print("Forma de las predicciones:", predicciones.shape)
        print("Primeros 5 valores de predicciones:", predicciones[:5])
        print("Suma de predicciones:", np.sum(predicciones))

        # Verificar la forma de las predicciones
        if len(predicciones.shape) > 1:
            print("Advertencia: predicciones multidimensionales. Aplanando...")
            predicciones = predicciones.flatten()

        # Aplicar temperatura de manera más estable
        predicciones = np.clip(predicciones, 1e-10, None)  # Clip values to avoid log(0)
        predicciones = np.log(predicciones) / temperatura
        exp_preds = np.exp(predicciones - np.max(predicciones))
        predicciones = exp_preds / np.sum(exp_preds)

        # Verificar NaN o Inf
        if np.isnan(predicciones).any() or np.isinf(predicciones).any():
            print("Advertencia: NaN o Inf en predicciones")
            break

        # Normalizar las predicciones
        predicciones = predicciones / np.sum(predicciones)

        print("Suma de predicciones después de la normalización:", np.sum(predicciones))

        # Seleccionar la siguiente palabra
        try:
            indice_proximo = np.random.choice(len(predicciones), p=predicciones)
        except ValueError as e:
            print("Error al seleccionar la siguiente palabra:", str(e))
            print("Predicciones:", predicciones)
            break

        palabra_siguiente = tokenizer.index_word.get(indice_proximo, '<UNK>')

        # Verificar condiciones de terminación
        max_probabilidad = np.max(predicciones)
        if max_probabilidad < umbral_confianza or palabras_generadas >= max_palabras:
            break

        # Añadir la palabra al texto generado
        tokens_entrada.append(palabra_siguiente)
        texto_generado += " " + palabra_siguiente
        palabras_generadas += 1

        print(f"Palabra generada: {palabra_siguiente} (índice: {indice_proximo})")
        print(f"Probabilidad máxima: {max_probabilidad:.4f}")
        print(f"Secuencia de índices: {secuencia[0][-5:]}")  # Últimos 5 índices

    return texto_generado

# Uso de la función
texto_inicial = "Este es un ejemplo de texto inicial más largo para proporcionar más contexto"
max_palabras = 50
temperatura = 0.7
umbral_confianza = 0.01

texto_generado = generar_texto_dinamico(modelo, texto_inicial, tokenizer, maxlen, max_palabras, temperatura, umbral_confianza)
print("Texto generado final:")
print(texto_generado)
"""