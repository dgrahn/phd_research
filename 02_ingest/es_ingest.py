from collections import defaultdict
from datetime import datetime
from functools import partial
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from multiprocessing import Pool, Lock, Value
from pathlib import Path
import argparse
import pandas as pd
import subprocess

def wall(message):
    subprocess.run(f'wall "{message}"', shell=True)

START = None
counter = None
ES = None

def init(counter_arg, host):
    print('Initializing process...')
    global counter
    counter = counter_arg

    global START
    START = datetime.now()

    global ES
    ES = Elasticsearch(host)

def process(n_files, index_name, csv):
    try:
        global counter
        with counter.get_lock():
            index = counter.value
            counter.value += 1

        perc = index / n_files * 100.0
        elapsed = datetime.now() - START
        per = elapsed / (index + 1e-10)
        rem = (n_files - index) * per

        print(f'{index:8,d} / {n_files:,d} -- {perc:3.2f}% [{elapsed}, {per.total_seconds():.5f} it., {rem} rem.] -- {csv.name}')

        if index % 10_000 == 0 and index != 0:
            subprocess.run(f'wall "{index} / {n_files} - {rem} rem."', shell=True)
    except Exception as e:
        print(e)

    df = pd.read_csv(csv)
    df = df.fillna('<<UNK>>')

    # Generate bigrams
    texts = defaultdict(lambda: 0)
    types = defaultdict(lambda: 0)

    a = df.iloc[0]
    for i in range(1, df.shape[0] - 1):
        b = df.iloc[i]
        texts[f'{a.token_text}<<>>{b.token_text}'] += 1
        types[f'{a.token_type}<<>>{b.token_type}'] += 1
        a = b

    # Extract variables
    filename = df.file_name.iloc[0]
    dataset = df.dataset.iloc[0]

    # Build the record
    record = {
        'filename': filename,
        'dataset': dataset,
        'num_tokens': df.shape[0],
        'token_text': [ { 'key': k, 'val': v } for k, v in df.token_text.value_counts().to_dict().items() ],
        'token_type': [ { 'key': k, 'val': v } for k, v in df.token_type.value_counts().to_dict().items() ],
        'bigram_text': [ { 'key': k, 'key': v } for k, v in texts.items() ],
        'bigram_type': [ { 'val': k, 'val': v } for k, v in types.items() ],
    }

    global ES
    try: 
        result = ES.index(
            index = index_name,
            id = df.uuid.iloc[0],
            body = record
        )
        if result['result'] == 'updated':
            raise Exception('duplicate id?')
        print(result)
    except Exception as e:
        print('-' * 10)
        print(result)
        raise e

def main(args):
    index_name = 'tokens-' + args.input.stem.lower()
    files = list(args.input.rglob('*.csv'))
    n_files = len(files)

    # Process the files
    counter = Value('i', 0)
    process_partial = partial(process, n_files, index_name)

    print('Creating Pool...')
    with Pool(args.num_processes, initializer=init, initargs=(counter, args.host)) as p:
        print('Processing...')
        p.map(process_partial, files)

    # wall(f'Done ingesting {n_files:,d} for {args.input}')

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', type=Path, default='/data/datasets/csv/TBO')
    parser.add_argument('--host', type=str, default='http://localhost:9200')
    parser.add_argument('--num_processes', type=int, default=16)

    args = parser.parse_args()

    try:
        main(args)
    except Exception as e:
        wall(f'Error ingesting {args.input}')
        raise(e)