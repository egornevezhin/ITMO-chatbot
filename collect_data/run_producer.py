import os
import sys
import imapy
import logging
import math
from multiprocessing import Pool
from tqdm import tqdm
import subprocess


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


def run_producer(args):
    p = subprocess.Popen(f'python producer.py --start {args[0]} --end {args[1]} --folder "{args[2]}"', stdout=subprocess.PIPE, shell=True)

    (output, err) = p.communicate()

    # This makes the wait possible
    p_status = p.wait()


if __name__ == '__main__':
    login = os.getenv('login')
    password = os.getenv('password')

    box = imapy.connect(host='imap.mail.ru',
                        username=login,
                        password=password,
                        ssl=True)

    folders = box.folders()

    ignored_folders = ('ПРИКАЗЫ',
                       'Спам',
                       'Корзина',
                       'Черновики',
                       'Банковские карты',
                       'ДЕЛОПРОИЗВОДСТВО',
                       'Archive',
                       'АРХИB',)

    # формирование пула
    logging.warning(f'Используются всего {len(folders)} папок')

    tasks = []

    for folder in folders:
        if folder.split('/')[0] in ignored_folders:
            continue
        emails_count = box.folder(folder).info()['total']
        logging.warning(f'В папке {folder} всего {emails_count} сообщений.')
        group_count = 1000
        for i in range(math.ceil(emails_count / group_count)):
            if (i + 1) * group_count < emails_count:
                tasks.append((i * group_count, (i + 1) * group_count, folder))
            else:
                tasks.append((i * group_count, emails_count, folder))


    logging.warning(f'Всего {len(tasks)} заданий')

    with Pool(processes=12) as p:
        with tqdm(total=len(tasks), desc='Общий прогресс  ---- ', leave=True) as pbar:
            for _ in p.imap_unordered(run_producer, tasks):
                pbar.update()




    # отправленная почта
