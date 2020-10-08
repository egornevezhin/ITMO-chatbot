from kafka import KafkaProducer
import re, logging, os, sys, time
from tqdm import tqdm
import imapy
import json
import argparse
import math


root = logging.getLogger()
root.setLevel(logging.WARNING)

handler = logging.StreamHandler(sys.stdout)
handler_file = logging.FileHandler('log.txt')
handler.setLevel(logging.WARNING)
formatter = logging.Formatter('%(levelname)s: %(asctime)s - %(message)s')
handler.setFormatter(formatter)
handler_file.setFormatter(formatter)
root.addHandler(handler)
root.addHandler(handler_file)


def get_data_from_email(emails):
    global producer, logging

    for email in emails:
        try:
            producer.send('email', json.dumps({
                'id': email.uid,
                'from': email['from_email'],
                'subject': email['subject'],
                'date': email['date'],
                'text': email['text'][0]['text_normalized'],
                'to': re.findall(r'(\S+@\S*)', email['headers']['to'][0])[0].replace('>', '').replace('<', '')
            }).encode('utf-8'))

        except IndexError as e:
            logging.error(f'Ошибка с {email.uid} сообщением, {e}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', type=int, help='Start number of messages.', required=True)
    parser.add_argument('--end', type=int, help='End number of messages.', required=True)
    parser.add_argument('--folder', type=str, help='Name of the folder.', required=True)

    args = parser.parse_args()

    login = os.getenv('login')
    password = os.getenv('password')

    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    box = imapy.connect(host='imap.mail.ru',
                        username=login,
                        password=password,
                        ssl=True)

    count = 10

    emails_count = box.folder(args.folder).info()['total']

    for i in tqdm(range(args.start, math.ceil(args.end // count)), desc=f'{args.folder} ---- ', leave=True):
        if count * (i + 1) < args.end:
            emails = box.folder(args.folder).emails(i, count * (i + 1))
        else:
            emails = box.folder(args.folder).emails(i, args.end)
        get_data_from_email(emails)
