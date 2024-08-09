#Entrenamiento del modelo de arriba con los datos JSON y metricas especiales

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
#import tensorflow_addons as tfa
#from tensorflow_addons.optimizers import RectifiedAdam, Lookahead
#import tensorflow.keras.backend as K
#from keras.utils import custom_object_scope


# Configurar la sesión de TensorFlow para utilizar la GPU
config = tf.compat.v1.ConfigProto()
config.gpu_options.allow_growth = True
tf.compat.v1.keras.backend.set_session(tf.compat.v1.Session(config=config))


"""
# Configurar el crecimiento dinámico de la memoria GPU
gpus = tf.config.list_physical_devices('GPU')
if gpus:
    try:
        # Restringir la memoria GPU para que crezca según sea necesario
        for gpu in gpus:
            tf.config.experimental.set_memory_growth(gpu, True)
    except RuntimeError as e:
        print(e)
"""


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


num_palabras = tokenizer.get_vocab_size()

embedding_matrix = np.load(ruta_archivo_numpy)
print(f"Forma de la matriz de embedding cargada: {embedding_matrix.shape}")

"""PRIMERA VERSIÓN
# Habilitar el gradient checkpointing
tf.config.experimental_run_functions_eagerly(True)

# Habilitar el entrenamiento de precisión mixta
policy = tf.keras.mixed_precision.Policy('mixed_float16')
tf.keras.mixed_precision.set_global_policy(policy)

# Cargar el modelo
modelo = load_model(ruta_modelo_nuevaArq, custom_objects={'Softmax': tf.keras.layers.Activation('softmax')})

# Resumen del modelo
modelo.summary()

# Definir la ruta para TensorBoard logs
log_dir = os.path.join(ruta_base_3, 'logs')
#tensorboard_callback = TensorBoard(log_dir=log_dir, histogram_freq=1, write_graph=True)
#file_writer = tf.summary.create_file_writer(log_dir + "/metrics")
shutil.rmtree(log_dir, ignore_errors=True)

# Crear directorio de logs nuevamente
os.makedirs(log_dir, exist_ok=True)

# Preparar el callback para guardar checkpoints usando ruta_base_3
checkpoint = ModelCheckpoint(f'{ruta_base_3}checkpoint-{{epoch:02d}}.h5',
                             save_best_only=True,
                             save_weights_only=False,
                             monitor='val_loss',
                             mode='min')
tensorboard_callback = TensorBoard(log_dir=log_dir, histogram_freq=1, profile_batch=0)



class PerplexityCallback(tf.keras.callbacks.Callback):
    def on_epoch_end(self, epoch, logs=None):
        # Obtener la pérdida en el conjunto de validación
        val_loss = logs.get('val_loss')
        if val_loss is not None:
            # Calcular la perplejidad
            perplexity = tf.exp(val_loss)
            print(f'\nPerplexity: {perplexity:.2f}')

# Usar Adam personalizado
optimizer = Adam(learning_rate=0.001)  # Ajustar el learning rate según sea necesario

modelo.compile(optimizer=optimizer,
              loss='sparse_categorical_crossentropy',
              metrics=['accuracy'])


# Función de pérdida personalizada para generación de texto
def masked_loss(y_true, y_pred):
    mask = tf.math.logical_not(tf.math.equal(y_true, 0))
    loss = tf.keras.losses.sparse_categorical_crossentropy(y_true, y_pred)
    mask = tf.cast(mask, dtype=loss.dtype)
    loss *= mask
    return tf.reduce_sum(loss) / tf.reduce_sum(mask)

# Métrica personalizada para generación de texto
def masked_accuracy(y_true, y_pred):
    mask = tf.math.logical_not(tf.math.equal(y_true, 0))
    correct = tf.math.equal(tf.math.argmax(y_pred, axis=-1, output_type=tf.int32), tf.cast(y_true, tf.int32))
    correct = tf.cast(correct, dtype=tf.float32)
    mask = tf.cast(mask, dtype=tf.float32)
    return tf.reduce_sum(correct * mask) / tf.reduce_sum(mask)


modelo.compile(optimizer=optimizer,
               loss=masked_loss,
               metrics=[masked_accuracy])


# Configurar los callbacks
early_stopping = EarlyStopping(monitor='val_loss', patience=5, restore_best_weights=True)
reduce_lr = ReduceLROnPlateau(monitor='val_loss', factor=0.5, patience=2)

# Preparar los callbacks para Perplejidad
perplexity_callback = PerplexityCallback()

# Callback para encontrar el mejor learning rate
def scheduler(epoch, lr):
    if epoch < 5:
        return lr
    else:
        return lr * tf.math.exp(-0.1)

lr_scheduler = LearningRateScheduler(scheduler)

# Entrenar el modelo
modelo.fit(
    x_train, y_train,
    epochs=15,
    batch_size=20,
    validation_split=0.2,
    callbacks=[early_stopping, reduce_lr, checkpoint, tensorboard_callback, perplexity_callback, lr_scheduler]
)

# Guardar el modelo entrenado en un archivo .h5
modelo.save(os.path.join(ruta_base_3, 'chatbot_model.h5'))

# Guardar los pesos del modelo en un archivo HDF5
modelo.save_weights(os.path.join(ruta_base_3, "pesos_modelo_nuevaArq.h5"))

# Evaluar el modelo en el conjunto de prueba
resultados = modelo.evaluate(x_test, y_test, verbose=1)
print(f'Pérdida: {resultados[0]}, Precisión: {resultados[1]}')
"""

""" SEGUNDA VERSIÓN
# Habilitar el gradient checkpointing
tf.config.experimental_run_functions_eagerly(True)

# Habilitar el entrenamiento de precisión mixta
policy = tf.keras.mixed_precision.Policy('mixed_float16')
tf.keras.mixed_precision.set_global_policy(policy)

# Cargar el modelo
modelo = load_model(ruta_modelo_nuevaArq, custom_objects={'AdamW': AdamW, 'Softmax': tf.keras.layers.Activation('softmax')})

# Resumen del modelo
modelo.summary()

# Definir la ruta para TensorBoard logs
log_dir = os.path.join(ruta_base_3, 'logs')
shutil.rmtree(log_dir, ignore_errors=True)

# Preparar el callback para guardar checkpoints
checkpoint = ModelCheckpoint(f'{ruta_base_3}checkpoint-{{epoch:02d}}.h5',
                             save_best_only=True,
                             save_weights_only=False,
                             monitor='val_loss',
                             mode='min')

tensorboard_callback = TensorBoard(log_dir=log_dir, histogram_freq=1, profile_batch=0)

class PerplexityCallback(tf.keras.callbacks.Callback):
    def on_epoch_end(self, epoch, logs=None):
        val_loss = logs.get('val_loss')
        if val_loss is not None:
            perplexity = tf.exp(val_loss)
            print(f'\nPerplexity: {perplexity:.2f}')
            tf.summary.scalar('perplexity', perplexity, step=epoch)

# Planificador de tasa de aprendizaje
def lr_schedule(epoch):
    initial_lr = 0.001  # Ajuste de la tasa de aprendizaje inicial
    if epoch < 3:
        return initial_lr
    else:
        return initial_lr * tf.math.exp(-0.1 * (epoch - 3))

lr_scheduler = LearningRateScheduler(lr_schedule)

# Optimizer AdamW con gradient clipping
optimizer = AdamW(learning_rate=0.001, weight_decay=1e-5, clipnorm=1.0)

# Recompilar el modelo con el nuevo optimizador
modelo.compile(optimizer=optimizer,
               loss='sparse_categorical_crossentropy',
               metrics=['accuracy'])

# Configurar los callbacks
early_stopping = EarlyStopping(monitor='val_loss', patience=5, restore_best_weights=True)
reduce_lr = ReduceLROnPlateau(monitor='val_loss', factor=0.2, patience=5, min_lr=1e-6)
perplexity_callback = PerplexityCallback()

# Entrenar el modelo
history = modelo.fit(
    x_train, y_train,
    epochs=20,
    batch_size=16,
    validation_split=0.2,
    callbacks=[early_stopping, reduce_lr, checkpoint, tensorboard_callback, perplexity_callback, lr_scheduler]
)

# Guardar el modelo entrenado
modelo.save(os.path.join(ruta_base_3, 'chatbot_model.h5'))
modelo.save_weights(os.path.join(ruta_base_3, "pesos_modelo_nuevaArq.h5"))

# Evaluar el modelo en el conjunto de prueba
resultados = modelo.evaluate(x_test, y_test, verbose=1)
print(f'Pérdida: {resultados[0]}, Precisión: {resultados[1]}')
"""

# Habilitar el gradient checkpointing
tf.config.run_functions_eagerly(True)

# Habilitar el entrenamiento de precisión mixta
policy = tf.keras.mixed_precision.Policy('mixed_float16')
tf.keras.mixed_precision.set_global_policy(policy)

# Cargar el modelo
modelo = load_model(ruta_modelo_nuevaArq)

# Resumen del modelo
modelo.summary()

# Definir la ruta para TensorBoard logs
log_dir = os.path.join(ruta_base_3, 'logs')
shutil.rmtree(log_dir, ignore_errors=True)

# Preparar el callback para guardar checkpoints
checkpoint = ModelCheckpoint(f'{ruta_base_3}checkpoint-{{epoch:02d}}.keras',
                             save_best_only=True,
                             save_weights_only=False,
                             monitor='val_loss',
                             mode='min')

best_model_callback = ModelCheckpoint(
    filepath=os.path.join(ruta_base_3, 'best_model.keras'),
    save_best_only=True,
    monitor='val_loss',
    mode='min',
    verbose=1
)

class LRTensorBoard(TensorBoard):
    def __init__(self, log_dir, **kwargs):
        super().__init__(log_dir=log_dir, **kwargs)

    def on_epoch_end(self, epoch, logs=None):
        logs = logs or {}
        logs['lr'] = self.model.optimizer.lr
        super().on_epoch_end(epoch, logs)

tensorboard_callback = LRTensorBoard(log_dir=log_dir, histogram_freq=1, profile_batch=0)


# Optimizer AdamW con gradient clipping
#optimizer = AdamW(learning_rate=0.0002, weight_decay=1e-4, clipnorm=0.5) #de 0.0001 a 0.0005, 0.0003, 0.00075
optimizer = AdamW(learning_rate=0.0002, weight_decay=1e-5, clipnorm=1.0)

"""
# Optimizer AdamW con gradient clipping y Lookahead
base_optimizer = AdamW(learning_rate=0.0002, weight_decay=1e-5, clipnorm=1.0)
optimizer = Lookahead(base_optimizer, sync_period=6, slow_step_size=0.5)
"""

# Recompilar el modelo con el nuevo optimizador
modelo.compile(optimizer=optimizer,
               loss='sparse_categorical_crossentropy',
               metrics=['accuracy'])

class PerplexityCallback(tf.keras.callbacks.Callback):
    def on_epoch_end(self, epoch, logs=None):
        val_loss = logs.get('val_loss')
        if val_loss is not None:
            perplexity = tf.exp(val_loss)
            print(f'\nPerplexity: {perplexity:.2f}')
            tf.summary.scalar('perplexity', perplexity, step=epoch)

class GradientNormCallback(tf.keras.callbacks.Callback):
    def __init__(self, train_data):
        super().__init__()
        self.train_data = train_data.repeat()
        self.gradient_norms = []

    def on_train_batch_end(self, batch, logs=None):
        # Obtener el optimizador base si es un LossScaleOptimizer
        optimizer = self.model.optimizer
        if isinstance(optimizer, tf.keras.mixed_precision.LossScaleOptimizer):
            optimizer = optimizer.inner_optimizer

        # Crear una función para calcular los gradientes
        @tf.function
        def get_gradients():
            # Obtener un batch de datos
            inputs, targets = next(iter(self.train_data))
            with tf.GradientTape() as tape:
                # Forward pass
                y_pred = self.model(inputs, training=True)
                # Calcular la pérdida
                loss = self.model.compiled_loss(targets, y_pred)
            # Calcular gradientes
            grads = tape.gradient(loss, self.model.trainable_variables)
            return grads

        # Calcular los gradientes
        gradients = get_gradients()

        # Calcular la norma global de los gradientes
        global_norm = tf.linalg.global_norm(gradients)

        # Guardar la norma del gradiente
        self.gradient_norms.append(global_norm.numpy())

        # Registrar la norma del gradiente en TensorBoard
        tf.summary.scalar('gradient_norm', global_norm, step=optimizer.iterations)

    def on_epoch_end(self, epoch, logs=None):
        # Calcular y registrar la norma promedio de los gradientes para esta época
        avg_norm = np.mean(self.gradient_norms)
        print(f'\nPromedio de la norma del gradiente para la época {epoch+1}: {avg_norm:.4f}')
        self.gradient_norms = []  # Reiniciar para la siguiente época

# Crear el dataset de entrenamiento
train_dataset = tf.data.Dataset.from_tensor_slices((x_train, y_train)).batch(16)

perplexity_callback = PerplexityCallback()
gradient_norm_callback = GradientNormCallback(train_dataset)

# Función de planificación de tasa de aprendizaje modificada
def cyclic_lr_with_warmup(epoch, initial_lr=0.0001, max_lr=0.0003, warmup_epochs=2, cycle_length=5):
    if epoch < warmup_epochs:
        return initial_lr + (max_lr - initial_lr) * epoch / warmup_epochs
    else:
        cycle = np.floor(1 + (epoch - warmup_epochs) / cycle_length)
        x = np.abs((epoch - warmup_epochs) / cycle_length - 2 * cycle + 1)
        return initial_lr + (max_lr - initial_lr) * max(0, (1 - x)) * 0.5 ** (cycle - 1)


# Callbacks modificados
lr_scheduler = LearningRateScheduler(cyclic_lr_with_warmup)

"""
early_stopping = EarlyStopping(monitor='val_loss', patience=8, restore_best_weights=True)
reduce_lr = ReduceLROnPlateau(monitor='val_loss', factor=0.7, patience=3, min_lr=5e-6)
"""

early_stopping = EarlyStopping(monitor='val_loss', patience=5, restore_best_weights=True, verbose=1)
reduce_lr = ReduceLROnPlateau(monitor='val_loss', factor=0.5, patience=2, min_lr=1e-6, verbose=1)



# Optimizar el Data Pipeline
def prepare_dataset(x, y, batch_size):
    dataset = tf.data.Dataset.from_tensor_slices((x, y))
    return dataset.cache().shuffle(buffer_size=1000).batch(batch_size).prefetch(tf.data.AUTOTUNE)

train_dataset = prepare_dataset(x_train, y_train, batch_size=16)
val_dataset = prepare_dataset(x_test, y_test, batch_size=16)


"""
# Entrenar el modelo
history = modelo.fit(
    x_train, y_train,
    epochs=20,
    batch_size=16,
    validation_split=0.2,
    callbacks=[early_stopping, reduce_lr, checkpoint, tensorboard_callback, perplexity_callback, lr_scheduler, gradient_norm_callback, best_model_callback]
)
"""


# Entrenar el modelo
history = modelo.fit(
    train_dataset,
    epochs=20,
    batch_size=16,
    validation_data=val_dataset,
    callbacks=[early_stopping, reduce_lr, checkpoint, tensorboard_callback, perplexity_callback, lr_scheduler, gradient_norm_callback, best_model_callback]
)


# Guardar el modelo entrenado
modelo.save(os.path.join(ruta_base_3, 'chatbot_model.keras'))
modelo.save_weights(os.path.join(ruta_base_3, "pesos_modelo_nuevaArq.keras"))


# Evaluar el modelo en el conjunto de prueba
test_dataset = prepare_dataset(x_test, y_test, batch_size=16)
resultados = modelo.evaluate(x_test, y_test, verbose=1)
print(f'Pérdida: {resultados[0]}, Precisión: {resultados[1]}')


"""
# Evaluar el modelo en el conjunto de prueba
resultados = modelo.evaluate(x_test, y_test, verbose=1)
print(f'Pérdida: {resultados[0]}, Precisión: {resultados[1]}')
"""
