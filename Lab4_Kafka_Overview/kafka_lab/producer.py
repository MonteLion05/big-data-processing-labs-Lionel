# %%
import socket
import time
from confluent_kafka import Producer

# %%
conf = {'bootstrap.servers': 'localhost:9092',
        'client.id': socket.gethostname()}

producer = Producer(conf)

# %%
topic = 'book_lines'
book_file = 'C:/Users/lione/Documents/ECE/ING5/Big Data Processing/Lab4_Kafka_Overview/book.txt'  # adapte avec le nom exact de ton fichier téléchargé

# %%
def delivery_report(err, msg):
    """Callback appelé une fois que Kafka a confirmé (ou échoué) la livraison."""
    if err is not None:
        print(f"Delivery failed: {err}")
    # on ne print pas chaque succès pour ne pas noyer la sortie

# %%
with open(book_file, 'r', encoding='utf-8') as f:
    for line_num, line in enumerate(f, start=1):
        line = line.rstrip('\n')
        if line.strip() == '':
            continue  # skip empty lines, optionnel

        producer.produce(
            topic=topic,
            value=line,
            callback=delivery_report
        )
        producer.poll(0)  # laisse le temps au callback de se déclencher

        if line_num % 100 == 0:
            print(f"Sent line {line_num}: {line[:50]}")

producer.flush()
print("Finished sending all lines.")