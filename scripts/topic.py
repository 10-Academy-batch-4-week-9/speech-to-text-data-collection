from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
admin_client = KafkaAdminClient(bootstrap_servers="b-1.demo-cluster-1.9q7lp7.c1.kafka.eu-west-1.amazonaws.com:9092", client_id='group4')

topic_list = []
topic_list.append(NewTopic(name="group4_text_corpus", num_partitions=1, replication_factor=2))
topic_list.append(NewTopic(name="group4_audio", num_partitions=1, replication_factor=2))
admin_client.create_topics(new_topics=topic_list, validate_only=False)

