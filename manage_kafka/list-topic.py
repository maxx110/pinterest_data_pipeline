import kafka
admin_client = kafka.KafkaAdminClient(bootstrap_servers=['127.0.0.1:9092'])
#x=admin_client.list_topics()
x=admin_client.describe_topics(topics=["pinterest"])
print(x)
#het

