import random
import json
import pickle
import numpy as np

import nltk
from nltk.stem import WordNetLemmatizer #Para pasar las palabras a su forma raíz


#Para crear la red neuronal
from keras.models import Sequential
from keras.layers import Dense, Activation, Dropout
from keras.optimizers import SGD

lematizador = WordNetLemmatizer()

intentos = json.loads(open('intentos.json').read())

nltk.download('punkt')
nltk.download('wordnet')
nltk.download('omw-1.4')

palabras = []
clases = []
documentos = []
letras_ignoradas = ['?', '!', '¿', '.', ',']

#Clasifica los patrones y las categorías
for intento in intentos['intentos']:
    for patron in intento['patrones']:
        lista_palabras = nltk.word_tokenize(patron)
        palabras.extend(lista_palabras)
        documentos.append((lista_palabras, intento["tag"]))
        if intento["tag"] not in clases:
            clases.append(intento["tag"])

palabras = [lematizador.lemmatize(palabra) for palabra in palabras if palabra not in letras_ignoradas]
palabras = sorted(set(palabras))

pickle.dump(palabras, open('palabras.pkl', 'wb'))
pickle.dump(clases, open('clases.pkl', 'wb'))

#Pasa la información a unos y ceros según las palabras presentes en cada categoría para hacer el entrenamiento
entrenamiento = []
entrada_vacia = [0]*len(clases)
for documento in documentos:
    bolsa = []
    patron_palabra = documento[0]
    patron_palabra = [lematizador.lemmatize(palabra.lower()) for palabra in patron_palabra]
    for palabra in palabras:
        bolsa.append(1) if palabra in patron_palabra else bolsa.append(0)
    fila_entrada = list(entrada_vacia)
    fila_entrada[clases.index(documento[1])] = 1
    entrenamiento.append([bolsa, fila_entrada])
    
    
#Este es el nuevo proceso para solucionar el error
# Separar las características (train_x) y las etiquetas (train_y)
entrenamiento_x = [x[0] for x in entrenamiento]
entrenamiento_y = [x[1] for x in entrenamiento]

# Convertir a arrays NumPy con el tipo de dato adecuado
entrenamiento_x = np.array(entrenamiento_x)
entrenamiento_y = np.array(entrenamiento_y)

#print(entrenamiento_x.shape)
#print(entrenamiento_y.shape)

#Este es el nuevo proceso para solucionar el error

# Creamos la red neuronal
modelo = Sequential()
modelo.add(Dense(128, input_shape=(len(entrenamiento_x[0]),), activation='relu'))
modelo.add(Dropout(0.5))
modelo.add(Dense(64, activation='relu'))
modelo.add(Dropout(0.5))
modelo.add(Dense(len(entrenamiento_y[0]), activation='softmax'))

# Creamos el optimizador y lo compilamos
sgd = SGD(learning_rate=0.001, momentum=0.9, nesterov=True)
modelo.compile(loss='categorical_crossentropy', optimizer=sgd, metrics=['accuracy'])

# Entrenamos el modelo y lo guardamos
proceso_entrenamiento = modelo.fit(np.array(entrenamiento_x), np.array(entrenamiento_y), epochs=100, batch_size=5, verbose=0)
modelo.save("modelo_chatbot.h5", proceso_entrenamiento)

    
    
"""
random.shuffle(training)
training = np.array(training) 
print(training) 

#Reparte los datos para pasarlos a la red
train_x = list(training[:,0])
train_y = list(training[:,1])

#Creamos la red neuronal
model = Sequential()
model.add(Dense(128, input_shape=(len(train_x[0]),), activation='relu'))
model.add(Dropout(0.5))
model.add(Dense(64, activation='relu'))
model.add(Dropout(0.5))
model.add(Dense(len(train_y[0]), activation='softmax'))

#Creamos el optimizador y lo compilamos
sgd = SGD.SGD(learning_rate=0.001, decay=1e-6, momentum=0.9, nesterov=True)
model.compile(loss='categorical_crossentropy', optimizer = sgd, metrics = ['accuracy'])

#Entrenamos el modelo y lo guardamos
train_process = model.fit(np.array(train_x), np.array(train_y), epochs=100, batch_size=5, verbose=1)
model.save("chatbot_model.h5", train_process)
"""