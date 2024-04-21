import requests

# URL del servidor Kafka Connect
kafka_connect_url = "http://localhost:8084/connectors"

# Configuración del conector MongoDB
mongodb_sink_config = {
    "name": "mongodb-sink",
    "config": {
        "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
        "tasks.max": "1",  # tareas maximas a procesar, siempre debe ser igual o menor que las particiones del topic de kafka
        "topics": "input_topic,output_topic",
        "connection.uri": "mongodb://dfr209811:nostromo987Q_Q@192.168.1.120:27018/",
        "database": "tennus_data_analitica",
        "collection": "Mensajes",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        "key.ignore": "true",
        "transforms": "ValueToKey,RouteMessages",
        "transforms.ValueToKey.type": "org.apache.kafka.connect.transforms.ValueToKey",
        "transforms.ValueToKey.fields": "tipo",
        "transforms.RouteMessages.type": "org.apache.kafka.connect.transforms.RegexRouter",
        "transforms.RouteMessages.regex": "(\\{\"tipo\":\"conversación\",\"doc\":\\{.*\\}\\}|\\{\"tipo\":\"Respuesta\",\"doc\":\\{.*\\}\\})",
        "transforms.RouteMessages.replacement": "$1",
        "transforms.RouteMessages.topic.regex": ".*",
        "transforms.RouteMessages.topic.format": "${topic.withoutPrefix}"
    }
}

# Enviar la solicitud HTTP para crear el conector
response = requests.post(kafka_connect_url, json=mongodb_sink_config)

# Verificar si la solicitud fue exitosa
if response.status_code == 201:
    print("Conector MongoDB creado con éxito.")
else:
    print("Error al crear el conector MongoDB:", response.text)