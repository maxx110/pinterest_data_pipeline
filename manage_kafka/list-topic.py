import kafka
admin_client = kafka.KafkaAdminClient(bootstrap_servers=['127.0.0.1:9092'])
x=admin_client.list_topics()
print(x)
#het
