from kafka import KafkaProducer
import json

# Defina os parâmetros de conexão do Kafka
bootstrap_servers = ['localhost:9092']
topic_name = 'produtos'

# Crie um produtor Kafka
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Crie uma lista de produtos
produtos = [
    {'nome': 'Produto 1', 'preco': 10.50},
    {'nome': 'Produto 2', 'preco': 20.00},
    {'nome': 'Produto 3', 'preco': 15.75}
]

# Envie cada produto como uma mensagem para o tópico Kafka
for produto in produtos:
    mensagem = {'produto': produto}
    producer.send(topic_name, value=mensagem)

# Feche o produtor Kafka
producer.close()
