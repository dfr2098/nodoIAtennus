from keras.models import Sequential
from keras.layers import Embedding, LSTM, Dense, TimeDistributed, Dropout, GlobalAveragePooling1D
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
from tensorflow.keras.callbacks import EarlyStopping, ModelCheckpoint, ReduceLROnPlateau

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
ruta_txt = "/content/drive/My Drive/Red neuronal/Modelo LLM/Red LSTM/corpus_sinStop.txt"
#ruta_embedding = "/content/drive/My Drive/Red neuronal/Modelo LLM/Red LSTM/embeddings_combinados.vec"
ruta_embedding = os.path.join(ruta_base, 'modelo_reducido.vec')
#ruta_tokenizer = "/content/drive/My Drive/Red neuronal/Modelo LLM/Red LSTM/tokenizer_embeddings.pkl"
#ruta_tokenizer = "/content/drive/My Drive/Red neuronal/Modelo LLM/Red LSTM/tokenizer_embedding_preentrenado.pkl"
ruta_tokenizer = "/content/drive/My Drive/Red neuronal/Modelo LLM/Red LSTM/tokenizer_embedding_reducido.pkl"
ruta_modelo = "/content/drive/My Drive/Red neuronal/Modelo LLM/Red LSTM/solo_modelo.h5"

# Hiperparametros del modelo
lstm_units = 64
embedding_dim = 300
num_epochs = 2
maxlen = 100
batch_size = 32

"""
# Descargar los recursos necesarios de NLTK
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('wordnet')
"""

# Cargar los datos de entrenamiento desde el archivo Parquet
df = pd.read_parquet(ruta_dataset)

# Obtener las secuencias de entrada y salida
x_seq = df['inputs'].tolist()
y_seq = df['targets'].tolist()

# Cargar el modelo de Spacy para el idioma español
#nlp = spacy.load('es_core_news_sm')

# Convertir las secuencias de entrada y salida a minúsculas
X_seq_lower = [text.lower() for text in x_seq]
y_seq_lower = [text.lower() for text in y_seq]

# Imprimir los primeros 5 ejemplos de las secuencias limpias
print("Primeras 5 secuencias de entrada originales:")
for text in y_seq[:5]:
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

# Cargar el tokenizer desde el archivo pickle
with open(ruta_tokenizer, 'rb') as handle:
    tokenizer = pickle.load(handle)

# Imprimir el número de palabras en el vocabulario del tokenizer
print(f"El número de palabras en el vocabulario del tokenizer es: {len(tokenizer.word_index)}")

# Tokenización de X_seq_lower
X_seq_tokenized = tokenizer.texts_to_sequences(X_seq_lower)

# Tokenización de Y_seq_lower
y_seq_tokenized = tokenizer.texts_to_sequences(y_seq_lower)

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

# Creación del modelo
model = Sequential()
model.add(Embedding(input_dim=num_palabras, output_dim=embedding_dim, weights=[embedding_matrix], trainable=False, mask_zero=True, dtype='float32'))
model.add(Dropout(0.2))
model.add(LSTM(units=lstm_units, return_sequences=True))
model.add(Dropout(0.2))
model.add(TimeDistributed(Dense(units=num_palabras, activation='softmax')))
model.add(Dropout(0.2))

# Guardar el modelo en un archivo HDF5
model.save(os.path.join(ruta_base_3, "solo_modelo_nuevaArq.h5"))

# Define los checkpoints para las variables del modelo
checkpoint = tf.train.Checkpoint(model=model)

# Usa un optimizador que soporte gradient checkpointing y mixed precision
optimizer = tf.keras.optimizers.Adam(learning_rate=0.001)
optimizer = mixed_precision.LossScaleOptimizer(optimizer, dynamic=True)

# Define la función de pérdida
def loss_fn(y_true, y_pred):
    return tf.keras.losses.sparse_categorical_crossentropy(y_true, y_pred, from_logits=False)

# Define el tamaño del lote efectivo que deseas simular
effective_batch_size = 64

# Define el tamaño del lote real que puedes ajustar en tu GPU
real_batch_size = 8

# Define el número de pasos de acumulación
accumulation_steps = effective_batch_size // real_batch_size

# Inicializa un acumulador de gradientes
accumulated_gradients = [tf.Variable(tf.zeros_like(var), trainable=False) for var in model.trainable_variables]

# Clipping de gradientes para evitar gradientes explosivos
def apply_clipping(gradients, clip_value=1.0):
    return [tf.clip_by_value(grad, -clip_value, clip_value) for grad in gradients]

# Define la función de entrenamiento con acumulación de gradientes
@tf.function
def train_step_with_accumulation(x_batch, y_batch, step):
    with tf.GradientTape() as tape:
        predictions = model(x_batch, training=True)
        loss = tf.reduce_mean(loss_fn(y_batch, predictions))
        scaled_loss = optimizer.get_scaled_loss(loss)
    scaled_gradients = tape.gradient(scaled_loss, model.trainable_variables)
    gradients = optimizer.get_unscaled_gradients(scaled_gradients)

    # Aplica clipping a los gradientes
    gradients = apply_clipping(gradients)

    # Acumula los gradientes
    for i, grad in enumerate(gradients):
        accumulated_gradients[i].assign_add(grad)

    # Si has acumulado suficientes pasos, aplica los gradientes y reinicia el acumulador
    if (step + 1) % accumulation_steps == 0:
        optimizer.apply_gradients(zip(accumulated_gradients, model.trainable_variables))
        for i in range(len(accumulated_gradients)):
            accumulated_gradients[i].assign(tf.zeros_like(accumulated_gradients[i]))

    return loss

# Crear el dataset usando tf.data
train_dataset = tf.data.Dataset.from_tensor_slices((x_train, y_train))
train_dataset = train_dataset.batch(real_batch_size).prefetch(buffer_size=tf.data.experimental.AUTOTUNE)

val_dataset = tf.data.Dataset.from_tensor_slices((x_test, y_test))
val_dataset = val_dataset.batch(real_batch_size).prefetch(buffer_size=tf.data.experimental.AUTOTUNE)

# Función para evaluar el modelo en el conjunto de validación
@tf.function
def evaluate(dataset):
    total_loss = 0.0
    num_batches = 0
    for x_batch, y_batch in dataset:
        predictions = model(x_batch, training=False)
        loss = tf.reduce_mean(loss_fn(y_batch, predictions))
        total_loss += tf.reduce_sum(loss)
        num_batches += 1
    return total_loss / num_batches

# Verificar los datos de entrada para valores inf o nan
print("Número de valores inf en x_train:", np.sum(np.isinf(x_train)))
print("Número de valores nan en x_train:", np.sum(np.isnan(x_train)))
print("Número de valores inf en y_train:", np.sum(np.isinf(y_train)))
print("Número de valores nan en y_train:", np.sum(np.isnan(y_train)))

# Definir EarlyStopping, ModelCheckpoint y ReduceLROnPlateau
early_stopping = EarlyStopping(monitor='val_loss', patience=3, restore_best_weights=True)
model_checkpoint = ModelCheckpoint(filepath=os.path.join(ruta_base_3, 'best_model.h5'), save_best_only=True, monitor='val_loss')
reduce_lr = ReduceLROnPlateau(monitor='val_loss', factor=0.2, patience=2, min_lr=0.0001)

# Entrenamiento del modelo con acumulación de gradientes y callbacks
for epoch in range(num_epochs):
    num_batches = x_train.shape[0] // real_batch_size  # Calcula el número de batches
    print(f"Epoch {epoch + 1}, Número de batches: {num_batches}")  # Imprime el número de batches

    for batch, (x_batch, y_batch) in enumerate(train_dataset):
        loss = train_step_with_accumulation(x_batch, y_batch, batch)
        if (batch + 1) % accumulation_steps == 0:
            print(f"Epoch {epoch + 1}, Batch {batch + 1}, Accumulated Step Loss: {loss.numpy()}")
        else:
            print(f"Epoch {epoch + 1}, Batch {batch + 1}, Intermediate Step Loss: {loss.numpy()}")

    # Evaluar el modelo en el conjunto de validación
    val_loss = evaluate(val_dataset)
    print(f"Epoch {epoch + 1}, Validation Loss: {val_loss.numpy()}")

    if early_stopping.stopped_epoch > 0:
        print("Early stopping triggered.")
        break

# Guardar el modelo entrenado en un archivo .h5
model.save(os.path.join(ruta_base_3, 'chatbot_model.h5'))

# Guardar los pesos del modelo en un archivo HDF5
model.save_weights(os.path.join(ruta_base_3, "pesos_modelo_nuevaArq.h5"))

"""
# Graficar la pérdida
plt.plot(history.history['loss'], label='Training Loss')
plt.plot(history.history['val_loss'], label='Validation Loss')
plt.title('Training and Validation Loss')
plt.xlabel('Epoch')
plt.ylabel('Loss')
plt.legend()
plt.show()

# Graficar la precisión
plt.plot(history.history['accuracy'], label='Training Accuracy')
plt.plot(history.history['val_accuracy'], label='Validation Accuracy')
plt.title('Training and Validation Accuracy')
plt.xlabel('Epoch')
plt.ylabel('Accuracy')
plt.legend()
plt.show()


# Generación de texto
def generate_text(seed_text, num_words):
    for _ in range(num_words):
        sequence = tokenizer.texts_to_sequences([seed_text])[0]
        sequence = pad_sequences([sequence], maxlen=max_sequence_length)
        predicted_probs = model.predict(sequence)
        predicted_index = np.argmax(predicted_probs)
        predicted_word = tokenizer.index_word[predicted_index]
        seed_text += ' ' + predicted_word
    return seed_text
"""

