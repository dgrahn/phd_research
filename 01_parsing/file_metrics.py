from collections import defaultdict
from datetime import datetime
from functools import partial
from multiprocessing import Pool, Lock, Value
from pathlib import Path
import argparse
import math
import pandas as pd
import subprocess

def wall(message):
    subprocess.run(f'wall "{message}"', shell=True)

START = None
counter = None

def init(counter_arg):
    print('Initializing process...')
    global counter
    counter = counter_arg

    global START
    START = datetime.now()

def build_record(csv):
    # Read the file
    df = pd.read_csv(csv)
    df = df.fillna('<<UNK>>')

    if df.shape[0] == 0: return None

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
    return {
        'id': df.uuid.iloc[0],
        'filename': filename,
        'dataset': dataset,
        'num_tokens': df.shape[0],
        'token_text': df.token_text.value_counts().to_dict(),
        'token_type': df.token_type.value_counts().to_dict(),
        'bigram_text': texts,
        'bigram_type': types,
    }

import json

def write(record, filename):
    with open(filename, 'a+') as f:
        f.write(json.dumps(record) + '\n')

def process(n_files, output, max_lines, csv):

    record = build_record(csv)

    # Get the lock
    global counter
    with counter.get_lock():

        # Increment the counter
        index = counter.value
        counter.value += 1

        # Write the file
        if record:
            filename = args.output.joinpath(f'{math.floor(index / max_lines):04d}.jsonl')
            write(record, filename)

    try:
        perc = index / n_files * 100.0
        elapsed = datetime.now() - START
        per = elapsed / (index + 1e-10)
        rem = (n_files - index) * per

        print(f'{index:8,d} / {n_files:,d} -- {perc:3.2f}% [{elapsed}, {per.total_seconds():.5f} it., {rem} rem.] -- {csv.name}')

        if index % 50_000 == 0 and index != 0:
            wall(f'{index} / {n_files} - {rem} rem.')
    except Exception as e:
        print(e)

def main(args):
    # Join the dataset name
    args.input = args.input.joinpath(args.dataset)
    args.output = args.output.joinpath(args.dataset)

    # Make the output directory
    args.output.mkdir(exist_ok=True)

    # Count the files
    files = list(args.input.rglob('*.csv'))
    n_files = len(files)

    # Process the files
    counter = Value('i', 0)
    process_partial = partial(process, n_files, args.output, args.max_lines)

    print('Creating Pool...')
    with Pool(args.num_processes, initializer=init, initargs=(counter,)) as p:
        print('Processing...')
        p.map(process_partial, files)

    # wall(f'Done ingesting {n_files:,d} for {args.input}')

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', type=str)
    parser.add_argument('--input', type=Path, default='/data/datasets/csv')
    parser.add_argument('--output', type=Path, default='/data/datasets/jsonl')
    parser.add_argument('--max_lines', type=int, default=100_000)
    parser.add_argument('--num_processes', type=int, default=16)

    args = parser.parse_args()

    try:
        main(args)
    except Exception as e:
        wall(f'Error ingesting {args.input}')
        raise(e)