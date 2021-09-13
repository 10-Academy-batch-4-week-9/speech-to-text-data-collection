from kafka import KafkaConsumer

# To consume latest messages and auto-commit offsets

consumer = KafkaConsumer('group4_text_corpus',
                         auto_offset_reset='earliest',
                         enable_auto_commit=True,
                         group_id='my-grouprerwert3443rtwe',
                         bootstrap_servers=['b-1.demo-cluster-1.9q7lp7.c1.kafka.eu-west-1.amazonaws.com:9092'])

for message in consumer:
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value.decode()))