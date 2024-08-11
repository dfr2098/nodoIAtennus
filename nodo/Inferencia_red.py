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
from tqdm import tqdm

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

# Función de tokenización que maneja caracteres especiales
def tokenize_with_special_chars(text):
    pattern = r'(\w+|[^\w\s])'
    return re.findall(pattern, text.lower())

# Custom Tokenizer (asumiendo que ya tienes esto configurado correctamente)
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
modelo = tf.keras.models.load_model(ruta_modelo_entrenado, custom_objects={'AdamW': AdamW})

# Cargar los pesos si es necesario
modelo.load_weights(os.path.join(ruta_base_3, "pesos_modelo_nuevaArq.keras"))

# Resumen del modelo
modelo.summary()

# Optimizador
optimizer = AdamW(learning_rate=0.0002, weight_decay=1e-5, clipnorm=1.0)

# Recompilar el modelo con el nuevo optimizador
modelo.compile(optimizer=optimizer,
               loss='sparse_categorical_crossentropy',
               metrics=['accuracy'])

# Función de diagnóstico
def diagnosticar_modelo(modelo, tokenizer):
    print("Diagnóstico del modelo y tokenizer:")
    print(f"Tamaño del vocabulario del tokenizer: {len(tokenizer.word_index)}")
    print(f"Forma de la capa de salida del modelo: {modelo.output_shape}")
    print(f"Tipo de pérdida del modelo: {modelo.loss}")
    print("Primeras 10 palabras del tokenizer:")
    for word, index in list(tokenizer.word_index.items())[:10]:
        print(f"{word}: {index}")

# Ejecutar diagnóstico
diagnosticar_modelo(modelo, tokenizer)


def generar_frase(modelo, tokenizer, texto_entrada, max_tokens=10, temperatura=0.7, top_k=30):
    secuencia_entrada = tokenizer.texts_to_sequences([texto_entrada])
    secuencia_entrada = tf.keras.preprocessing.sequence.pad_sequences(secuencia_entrada, maxlen=maxlen, padding='post')
    
    frase_generada = []
    for _ in tqdm(range(max_tokens), desc="Generando frase"):
        predicciones = modelo.predict(secuencia_entrada, verbose=0)
        
        predicciones = predicciones[0, -1, 1:]  # Excluir el índice 0
        predicciones = np.log(np.clip(predicciones, 1e-5, None)) / temperatura
        exp_preds = np.exp(predicciones)
        predicciones = exp_preds / np.sum(exp_preds)

        # Top-k sampling
        top_k_indices = np.argsort(predicciones)[-top_k:]
        top_k_probs = predicciones[top_k_indices]
        top_k_probs = top_k_probs / np.sum(top_k_probs)

        indice_predicho = np.random.choice(top_k_indices, p=top_k_probs) + 1
        
        palabra_predicha = tokenizer.index_word.get(indice_predicho, "<OOV>")
        frase_generada.append(palabra_predicha)
        
        nueva_secuencia = np.zeros((1, maxlen))
        nueva_secuencia[0, :-1] = secuencia_entrada[0, 1:]
        nueva_secuencia[0, -1] = indice_predicho
        secuencia_entrada = nueva_secuencia
        
        if palabra_predicha in [".", "!", "?"] or len(frase_generada) >= max_tokens:
            break
    
    return ' '.join(frase_generada)

def limpiar_frase(frase):
    # Eliminar espacios antes de signos de puntuación
    frase = re.sub(r'\s([?.!,"](?:\s|$))', r'\1', frase)
    # Capitalizar la primera letra
    frase = frase.capitalize()
    # Asegurar que hay un punto final si no hay otro signo de puntuación
    if not frase[-1] in [".", "!", "?"]:
        frase += "."
    return frase

def generar_respuesta(texto_usuario, max_tokens=20, temperatura=0.7, top_k=30):
    print(f"Generando respuesta para: '{texto_usuario}'")
    frase_generada = generar_frase(modelo, tokenizer, texto_usuario, max_tokens, temperatura, top_k)
    respuesta_limpia = limpiar_frase(frase_generada)
    print(f"\nRespuesta generada: '{respuesta_limpia}'")
    return respuesta_limpia

# Ejemplo de uso
texto_usuario = "¿Qué es el ADN?"
respuesta_generada = generar_respuesta(texto_usuario, max_tokens=20, temperatura=0.7, top_k=30)