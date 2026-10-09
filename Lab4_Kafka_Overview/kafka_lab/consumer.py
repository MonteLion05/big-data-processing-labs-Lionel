# %%
from confluent_kafka import Consumer
import string

# %%
conf = {'bootstrap.servers': 'localhost:9092',
        'group.id': 'foo',
        'auto.offset.reset': 'smallest'}

consumer = Consumer(conf)

# %%
topic = 'book_lines'
consumer.subscribe([topic])

# %%
punctuation = set(string.punctuation)

def clean_line(line):
    line = line.lower()
    line = ''.join(ch for ch in line if ch not in punctuation)
    return line.strip()

# %%
# Configuration
MAX_EMPTY_POLLS = 10
MAX_ERRORS = 5
empty_polls = 0
error_count = 0

with open('cleaned_output.txt', 'w', encoding='utf-8') as out_file:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            empty_polls += 1
            if empty_polls >= MAX_EMPTY_POLLS:
                print("Closing: No new messages received.")
                break
            continue

        if msg.error():
            error_count += 1
            print(f"Consumer error: {msg.error()}")
            if error_count >= MAX_ERRORS:
                print("Closing: Too many consecutive errors.")
                break
            continue

        empty_polls = 0
        error_count = 0

        raw_line = msg.value().decode('utf-8')
        cleaned = clean_line(raw_line)

        if cleaned:  # skip blank lines after cleaning
            out_file.write(cleaned + '\n')
            print(f"Cleaned: {cleaned}")

consumer.close()
print("Finished writing cleaned_output.txt")