from datetime import datetime, timedelta
from multiprocessing import Pool, Lock, Value
from pathlib import Path
from util import FileCache
import argparse
import pandas as pd
import subprocess
import sys

ANTLR_PATH = Path().joinpath('antlr_cpp').absolute() # Must be absolute
FILES_PATH = Path('files.txt')

START = None
counter = None

def init(args):
    global counter
    counter = args

    global START
    START = datetime.now()

def process(input_file):
    return subprocess.check_output(
        [ 
            f'java -Xmx4096M org.antlr.v4.gui.TestRig CPP14 translationUnit -tokens {input_file.absolute()}',
            # 'CPP14', 'translationUnit', '-tokens',
            # str(input_file.absolute()),
        ],
        shell = True,
        stderr = subprocess.DEVNULL,
        cwd = ANTLR_PATH,
        timeout = 30,
    )

def tokenize(c_file, token_file, max_index, cache):   
    try:
        global counter
        with counter.get_lock():
            index = counter.value
            counter.value += 1

        perc = index / max_index * 100.0
        elapsed = datetime.now() - START
        per = elapsed / (index + 1e-10)
        rem = (max_index - index) * per

        print(f'{index:8,d} / {max_index:,d} -- {perc:3.2f}% [{elapsed}, {per.total_seconds():.5f} it., {rem} rem.] -- {c_file.name}', end='')

        if index % 10_000 == 0 and index != 0:
            subprocess.run(f'wall "{index} / {max_index} - {rem} rem."', shell=True)

    except:
        pass

    token_file.parent.mkdir(parents=True, exist_ok=True)

    try:
        tokens = process(c_file)
        tokens = tokens.decode('utf-8').split('\r\n')
        with open(token_file, 'w') as output:
            output.write('\n'.join(tokens))

    except subprocess.TimeoutExpired:
        print('Timed Out:', c_file)
        cache.timeout(c_file)

    except subprocess.CalledProcessError as e:
        print('Failed:', c_file)
        cache.fail(c_file)

    except Exception as e:
        print('Unknown:', c_file)
        cache.fail(c_file)
        raise e
    
def filter_files(start, unfiltered, failed):
    # Filter the files
    filtered = []

    # Convert failed to a dictionary for faster lookups
    print('Failed:', len(failed))
    failed = { f: True for f in failed }

    print('Filtering files...')
    for i, (c_file, token_file) in enumerate(unfiltered):
        if i % 100_000 == 0:
            print(f'\t{i:,d} -- {datetime.now() - start}')
            start = datetime.now()
        if Path(token_file).exists(): continue
        if c_file in failed:
            continue

        filtered.append((Path(c_file), Path(token_file)))
    
    return filtered

def main(args):
    # Count files
    start = datetime.now()

    cache = FileCache(
        cache=Path('tokens.cache'),
        input = args.input,
        in_extension = '.c*',
        output = args.output,
        out_extension = '.tokens'
    )

    if args.create_index:
        cache.clear()
        cache.create(start)
    else:
        cache.load()

    cache.filter(start)
    cache.save()

    failed = open('failed.txt').readlines() + open('timeout.txt').readlines()

    n_files = len(cache.files)
    print(f'There are {n_files:,d} to be processed.')

    # Process the files
    counter = Value('i', 0)

    with Pool(args.num_processes, initializer=init, initargs=(counter,)) as p:
        p.starmap(tokenize, [
            (f[0], f[1], n_files, cache) for f in cache.files
        ])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', type=Path, default='/data/datasets/input/WILD')
    parser.add_argument('--output', type=Path, default='/data/datasets/tokens/WILD')
    parser.add_argument('--num_processes', type=int, default=20)
    parser.add_argument('--create_index', action='store_true')

    args = parser.parse_args()
    main(args)