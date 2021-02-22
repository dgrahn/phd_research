import argparse
import subprocess
import sys
from pathlib import Path
from func_timeout import func_set_timeout, FunctionTimedOut
from datetime import datetime, timedelta
from multiprocessing import Pool, Lock, Value

ANTLR_PATH  = Path().joinpath('antlr_cpp').absolute() # Must be absolute
START = datetime.now()

counter = None

def init(args):
    global counter
    counter = args


@func_set_timeout(30)
def process(input_file):
    return subprocess.check_output(
        [ 
            # java org.antlr.v4.gui.TestRig %*
            'grun', 'CPP14', 'translationUnit', '-tokens',
            str(input_file.absolute()),
        ],
        shell = True,
        stderr = False,
        cwd = ANTLR_PATH,
    )

def tokenize(output_dir, c_file, max_index):
    try:
        global counter
        with counter.get_lock():
            index = counter.value
            counter.value += 1

        perc = index / max_index * 100.0
        elapsed = datetime.now() - START
        per = elapsed / (index + 1e-10)
        rem = (max_index - index) * per

        print(f'{index:8,d} / {max_index:,d} -- {perc:3.2f}% [{elapsed}, {per.total_seconds():.5f} it., {rem} rem.] -- {c_file.name}')
    except:
        pass

    output_file = output_dir.joinpath(c_file.parent).joinpath(c_file.stem + '.tokens')
    if output_file.exists(): return
    output_file.parent.mkdir(parents=True, exist_ok=True)

    try:
        tokens = process(c_file)
        tokens = tokens.decode('utf-8').split('\r\n')[2:]
        with open(output_file, 'w') as output:
            output.write('\n'.join(tokens))
    except FunctionTimedOut:
        print('***** Timed Out *****')
    except Exception as e:
        print(e)


def main(args):
    print('Counting files...')
    files = list(args.input.rglob('*.c*'))
    n_files = len(files)
    print(f'There are {n_files:,d} C files.')

    counter = Value('i', 0)

    with Pool(args.num_processes, initializer=init, initargs=(counter,)) as p:
        p.starmap(tokenize, [
            (args.output, f, n_files) for f in files
        ])




if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', type=Path, default='/data/datasets/WILD')
    parser.add_argument('--output', type=Path, default='/data/datasets/tokens/WILD')
    parser.add_argument('--num_processes', type=int, default=2)

    args = parser.parse_args()
    main(args)

# input_file = Path().joinpath('WILD').joinpath('old_func.c')
# print(input_file.absolute())

# output = subprocess.check_output(
#     [ 
#         'grun', 'CPP14', 'translationUnit', '-tokens',
#         str(input_file.absolute()),
#     ],
#     shell = True,
#     cwd = Path().joinpath('antlr_cpp').absolute(),
# )
# # output = subprocess.check_output(
# #     [
# #         'grun', '-help',# CPP14 translationUni -tokens',
# #         # str(input_file.absolute())
# #     ],
# #     # cwd = Path().joinpath('antlr_cpp').absolute()
# # )

# print('Output:', output, type(output))