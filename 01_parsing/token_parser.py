from datetime import datetime, timedelta
from functools import partial
from multiprocessing import Pool, Lock, Value
from pathlib import Path
from util import FileCache
import argparse
import pandas as pd
import re
import subprocess
import sys
import uuid

START = None
counter = None

def init(args):
    global counter
    counter = args

    global START
    START = datetime.now()

TOKEN_LINE = re.compile("\[@(\d+),(\d+):(-?\d+)='(.*)',<(.+)>,(?:channel=(\d+),)?(\d+):(\d+)]")

def parse_tokens(dataset, file_name, full_line):
    line = TOKEN_LINE.split(full_line.strip())

    token_num = int(line[1])
    char_start = int(line[2])
    char_end = int(line[3])
    token_text = line[4]
    token_type = line[5].replace("'", "")
    channel = line[6] or None
    line_num = int(line[7])
    line_char = int(line[8])

    return (
        uuid.uuid4(),
        dataset, file_name, token_num,
        char_start, char_end, token_text,
        token_type,
        channel,
        line_num, line_char
    )

def parse(dataset, max_index, cache, files):
    src_file, dst_file = files
    try:
        global counter
        with counter.get_lock():
            index = counter.value
            counter.value += 1

        perc = index / max_index * 100.0
        elapsed = datetime.now() - START
        per = elapsed / (index + 1e-10)
        rem = (max_index - index) * per

        print(f'{index:8,d} / {max_index:,d} -- {perc:3.2f}% [{elapsed}, {per.total_seconds():.5f} it., {rem} rem.] -- {src_file.name}')

        if index % 100_000 == 0:
            subprocess.run(f'wall "{index} / {max_index} - {rem} rem."', shell=True)

    except Exception as e:
        print(e)
    
    try:
        data = []

        with open(src_file, 'r') as tokens:
            for line in tokens:
                try:
                    tokens = parse_tokens(dataset, src_file.name, line)
                    data.append(tokens)
                except Exception as e:
                    cache.fail(src_file)
                    return
                
        df = pd.DataFrame(data, columns=[
            'uuid',
            'dataset', 'file_name', 'token_num',
            'char_start', 'char_end', 'token_text',
            'token_type',
            'channel',
            'line', 'line_char'
        ])

        dst_file.parent.mkdir(exist_ok=True, parents=True)
        df.to_csv(dst_file, index=False)

    except Exception as e:
        print(e)
        try:
            cache.fail(src_file)
        except Exception as e:
            print(e)
            raise e


def main(args):
    start = datetime.now()
    dataset = args.input.stem
    failed = []

    cache = FileCache(
        cache=Path('tokens.cache'),
        input = args.input,
        in_extension = '.tokens',
        output = args.output,
        out_extension = '.csv'
    )

    if args.create_index:
        cache.clear()
        cache.create(start)
    else:
        cache.load()

    cache.filter(start)
    cache.save()

    n_files = len(cache.files)
    print(f'There are {n_files:,d} to be processed.')

    # Process the files
    counter = Value('i', 0)

    print('Creating partial...')
    parse_partial = partial(parse, dataset, n_files, cache)

    print('Creating Pool...')
    with Pool(args.num_processes, initializer=init, initargs=(counter,)) as p:
        print('Processing...')
        p.map(parse_partial, cache.files)

    subprocess.run(f'wall "Done converting {args.input} to csv."', shell=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', type=Path, default='/data/datasets/tokens/WILD')
    parser.add_argument('--output', type=Path, default='/data/datasets/csv/WILD')
    parser.add_argument('--num_processes', type=int, default=20)
    parser.add_argument('--create_index', action='store_true')

    args = parser.parse_args()
    main(args)
