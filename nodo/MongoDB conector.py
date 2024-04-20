import requests

# URL del servidor Kafka Connect
kafka_connect_url = "http://localhost:8084/connectors"

# Configuración del transformador Multirouter
transform_config = {
    "name": "mongodb-sink-transform",
    "config": {
        "transforms": "extractFields,mergeValue,route",
        "transforms.extractFields.type": "org.apache.kafka.connect.transforms.ExtractField$Key",
        "transforms.extractFields.field": "usuario_id,username_usuario",
        "transforms.mergeValue.type": "org.apache.kafka.connect.transforms.Merge$Value",
        "transforms.mergeValue.fields": "usuario_id,username_usuario",
        "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
        "transforms.route.regex": ".*",
        "transforms.route.replacement": "output_topic"
    }
}

# Configuración del conector MongoDB
mongodb_sink_config = {
    "name": "mongodb-sink",
    "config": {
        "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
        "tasks.max": "1",
        "topics": "input_topic",
        "connection.uri": "mongodb://dfr209811:nostromo987Q_Q@192.168.1.120:27018/",
        "database": "tennus_data_analitica",
        "collection": "Mensajes",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        "transforms": "mongodb-sink-transform"
    }
}

# Enviar la solicitud HTTP para crear el transformador
response = requests.post(kafka_connect_url, json=transform_config)

# Verificar si la solicitud fue exitosa
if response.status_code == 201:
    print("Transformador Multirouter creado con éxito.")
else:
    print("Error al crear el transformador Multirouter:", response.text)

# Enviar la solicitud HTTP para crear el conector MongoDB
response = requests.post(kafka_connect_url, json=mongodb_sink_config)

# Verificar si la solicitud fue exitosa
if response.status_code == 201:
    print("Conector MongoDB creado con éxito.")
else:
    print("Error al crear el conector MongoDB:", response.text)