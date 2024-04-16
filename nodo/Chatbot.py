import os
import logging
logging.basicConfig(level=logging.ERROR)
# Configura la salida de registro para evitar que se muestre en la consola
# Oculta los mensajes de TensorFlow y Keras
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'  # Oculta los mensajes de TensorFlow
os.environ['TF_CPP_MIN_VLOG_LEVEL'] = '3'  # Oculta los mensajes de TensorFlow en verbose
logging.getLogger('tensorflow').setLevel(logging.FATAL)  # Oculta los mensajes de TensorFlow

import random
import json
import pickle
import numpy as np
import keras
import nltk
from nltk.stem import WordNetLemmatizer

from keras.models import load_model

import sys

from fastapi import FastAPI
from kafka import KafkaProducer, KafkaConsumer
import json
import threading

lematizador = WordNetLemmatizer()
#Importamos los archivos generados en el código anterior
intentos = json.loads(open('intentos.json').read())
palabras = pickle.load(open('palabras.pkl', 'rb'))
clases = pickle.load(open('clases.pkl', 'rb'))
modelo = load_model('modelo_chatbot.h5')

#Pasamos las palabras de oración a su forma raíz
def limpiar_oracion(oracion):
    palabras_oracion = nltk.word_tokenize(oracion)
    palabras_oracion = [lematizador.lemmatize(word) for word in palabras_oracion]
    return palabras_oracion

#Convertimos la información a unos y ceros según si están presentes en los patrones
def bolsa_de_palabras(oracion):
    palabras_oracion = limpiar_oracion(oracion)
    bolsa = [0]*len(palabras)
    for w in palabras_oracion:
        for i, palabra in enumerate(palabras):
            if palabra == w:
                bolsa[i]=1
    #print(bolsa)
    return np.array(bolsa)

#Predecimos la categoría a la que pertenece la oración
def predecir_clase(oracion):
    bow = bolsa_de_palabras(oracion)
    res = modelo.predict(np.array([bow]), verbose=0)[0]
    indice_max = np.where(res ==np.max(res))[0][0]
    categoria = clases[indice_max]
    return categoria

#Obtenemos una respuesta aleatoria
def obtener_respuesta(tag, intentos_json):
    lista_de_intentos = intentos_json['intentos']
    resultado = ""
    for i in lista_de_intentos:
        if i["tag"]==tag:
            resultado = random.choice(i['respuestas'])
            break
    return resultado


"""
#Ejecutamos el chat en bucle
while True:

    # Obtiene la entrada del usuario
    sys.stdout.write("Usuario: ")
    sys.stdout.flush()
    mensaje = input("")

    # Predice la clase a la que pertenece la oración
    intento = predecir_clase(mensaje)

    # Obtiene una respuesta aleatoria
    res = obtener_respuesta(intento, intentos)

    # Imprime la respuesta en la salida estándar con la leyenda de "Chatbot"
    print("Chatbot:", res)
"""
