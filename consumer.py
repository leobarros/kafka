from kafka import KafkaConsumer
import json

# Defina os parâmetros de conexão do Kafka
bootstrap_servers = ['localhost:9092']
topic_name = 'produtos'

# Crie um consumidor Kafka
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=bootstrap_servers,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Leia as mensagens do tópico de produtos
for mensagem in consumer:
    print(mensagem.value['produto'])

# Feche o consumidor Kafka
consumer.close()
