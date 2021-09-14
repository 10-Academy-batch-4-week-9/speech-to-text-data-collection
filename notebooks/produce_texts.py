import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError

df = pd.read_csv('Amharic News Dataset.csv')

df.dropna(inplace=True)

producer = KafkaProducer(bootstrap_servers=['b-1.demo-cluster-1.9q7lp7.c1.kafka.eu-west-1.amazonaws.com:9092'])

for index, row in df.head(20).iterrows():
    future = producer.send('group4_text_corpus', row['headline'].encode(), key=str(index).encode())
    print(index, row['headline'])

# Block for 'synchronous' sends
try:
    record_metadata = future.get(timeout=10)
except KafkaError:
    # Decide what to do if produce request failed...
    log.exception()
    pass



