#RECORTAR UN .VEC + espacio de <OOV> al final
import os

ruta_base = '/content/drive/My Drive/Red neuronal/Modelo LLM'
ruta_embedding = os.path.join(ruta_base, 'cc.es.300.vec')
reduced_embeddings_path = os.path.join(ruta_base, 'modelo_reducido.vec')

# Define el número de palabras que se desea conservar
num_words_to_keep = 500000

def process_file():
    with open(ruta_embedding, 'r', encoding='utf-8') as input_file, \
         open(reduced_embeddings_path, 'w', encoding='utf-8') as output_file:
        # Leer la primera línea para obtener las dimensiones
        first_line = input_file.readline().strip()
        num_words, dim = map(int, first_line.split())
        print(f"Número del vocabulario original: {num_words}")
        print(f"Dimensión del vector: {dim}")

        # Escribir la nueva primera línea con una palabra extra para <OOV>
        output_file.write(f"{num_words_to_keep + 1} {dim}\n")

        # Procesar las siguientes líneas
        for i, line in enumerate(input_file, start=1):
            if i <= num_words_to_keep:
                output_file.write(line)

        # Añadir el token OOV como la última línea
        oov_vector = ' '.join(['<OOV>'] + ['0.0'] * dim)
        output_file.write(f"{oov_vector}\n")

    print(f"Archivo reducido guardado en: {reduced_embeddings_path}")
    print(f"Número de palabras en el nuevo archivo (incluyendo <OOV>): {num_words_to_keep + 1}")

# Ejecutar el procesamiento
process_file()


#CREAR TOKENIZER CON EL <OOV> Y MANEJO DE SIMBOLOS ESPECIALES

import numpy as np
from keras.preprocessing.text import Tokenizer
import pickle
import os
import re

ruta_base = '/content/drive/My Drive/Red neuronal/Modelo LLM'
ruta_embedding = os.path.join(ruta_base, 'modelo_reducido.vec')
ruta_base_3 = '/content/drive/My Drive/Red neuronal/Modelo LLM/Red LSTM'

#Función para aliviar el tiempo de procesamiento y el uso de RAM
def cargar_embeddings_vec(ruta_vec, batch_size=5000):
    embeddings_index = {}
    embedding_dim = None
    with open(ruta_vec, 'r', encoding='utf-8') as f:
        primera_linea = f.readline()
        vocab_size, embedding_dim = map(int, primera_linea.split())
        batch = []
        for i, linea in enumerate(f):
            batch.append(linea)
            if len(batch) == batch_size:
                for line in batch:
                    valores = line.split()
                    palabra = valores[0]
                    vector = np.asarray(valores[1:], dtype='float32')
                    embeddings_index[palabra] = vector
                batch = []
                print(f"Procesados {i+1} embeddings")
        # Procesar el último lote si no es vacío
        if batch:
            for line in batch:
                valores = line.split()
                palabra = valores[0]
                vector = np.asarray(valores[1:], dtype='float32')
                embeddings_index[palabra] = vector
            print(f"Procesados {i+1} embeddings")
    return embeddings_index, embedding_dim

embeddings_index, embedding_dim = cargar_embeddings_vec(ruta_embedding)

# Imprimir el tamaño del vocabulario y la dimensión de los vectores
vocab_size = len(embeddings_index)
print(f"Tamaño del vocabulario: {vocab_size}")
print(f"Dimensión de los vectores: {embedding_dim}")

# Función para tokenizar texto, separando símbolos especiales
def tokenize_with_special_chars(text):
    # Separar símbolos especiales y puntuación
    pattern = r'(\w+|[^\w\s])'
    return re.findall(pattern, text.lower())

# Crear el tokenizer personalizado
class CustomTokenizer(Tokenizer):
    def texts_to_sequences(self, texts):
        return [[self.word_index.get(word, self.word_index.get('<OOV>')) for word in tokenize_with_special_chars(text)] for text in texts]

# Crear el diccionario que mapeará palabras a índices
vocabulario = {palabra: i for i, palabra in enumerate(embeddings_index.keys(), start=1)}

# Crear el tokenizer y ajustar el vocabulario
tokenizer = CustomTokenizer(oov_token='<OOV>')
tokenizer.word_index = vocabulario
tokenizer.index_word = {v: k for k, v in vocabulario.items()}

# Crear una matriz de embeddings
num_palabras = len(vocabulario)
embedding_matrix = np.zeros((num_palabras + 1, embedding_dim))  # +1 para incluir el índice 0
for palabra, i in vocabulario.items():
    embedding_vector = embeddings_index.get(palabra)
    if embedding_vector is not None:
        embedding_matrix[i] = embedding_vector

# Guardar el tokenizer en un archivo pickle en ruta_base_3
ruta_pickle = os.path.join(ruta_base_3, 'tokenizer_embedding_reducido.pkl')
with open(ruta_pickle, 'wb') as handle:
    pickle.dump(tokenizer, handle, protocol=pickle.HIGHEST_PROTOCOL)

print(f"Tokenizer guardado en: {ruta_pickle}")
print(f"Tamaño del vocabulario (incluyendo <OOV>): {num_palabras}")
print(f"ID del token <OOV>: {tokenizer.word_index['<OOV>']}")

# Ejemplo de uso
texto_ejemplo = "¿Qué es esto? Es un ejemplo de tokenización con palabras y símbolos especiales."
secuencia = tokenizer.texts_to_sequences([texto_ejemplo])
print(f"\nEjemplo de tokenización:")
print(f"Texto: {texto_ejemplo}")
print(f"Secuencia: {secuencia}")
print(f"Tokens: {[tokenizer.index_word[idx] for idx in secuencia[0]]}")