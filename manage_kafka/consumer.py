from kafka import KafkaConsumer
from json import loads

# create our consumer to retrieve the message from the topics
data_stream_consumer = KafkaConsumer(
    bootstrap_servers="localhost:9092",    
    value_deserializer=lambda message: loads(message),
    auto_offset_reset="earliest" # This value ensures the messages are read from the beginning 
)

data_stream_consumer.subscribe(topics=["pinterest"])
# Loops through all messages in the consumer and prints them out individually
# for message in data_stream_consumer:
#     print(message)
for message in data_stream_consumer:
    print(message.value)
#     print(message.topic)
#     print(message.timestamp)