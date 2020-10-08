import re, logging
from kafka import KafkaConsumer
import re, logging, os, sys
from tqdm import tqdm
import imapy
import json

if __name__ == '__main__':

    consumer = KafkaConsumer('email')
    for message in consumer:
        print(json.loads(message.value.decode('utf-8')))