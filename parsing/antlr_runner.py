import argparse
import subprocess
import sys
from pathlib import Path
from func_timeout import func_set_timeout, FunctionTimedOut
from datetime import datetime, timedelta
from multiprocessing import Pool, Lock, Value

ANTLR_PATH = Path().joinpath('antlr_cpp').absolute() # Must be absolute
START = None
counter = None

def init(args):
    global counter
    counter = args

    global START
    START = datetime.now()

@func_set_timeout(30)
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
    )

def tokenize(c_file, token_file, max_index):   
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

    except FunctionTimedOut:
        print('Timed Out:', c_file)
        subprocess.run(f'echo "{c_file}" >> timeout.txt', shell=True)

    except subprocess.CalledProcessError as e:
        print('Failed:', c_file)
        subprocess.run(f'echo "{c_file}" >> failed.txt', shell=True)

    except Exception as e:
        print('Unknown:', c_file)
        subprocess.run(f'echo "{c_file}" >> errors.txt', shell=True)
        raise e


def main(args):
    # Count files
    print('Counting files...')
    files = []

    start = datetime.now()
    for i, c_file in enumerate(args.input.rglob('*.c*')):
        if i % 100_000 == 0:
            print(f'\t{i:,d} -- {datetime.now() - start}')
            start = datetime.now()

        parent = c_file.parent.relative_to(args.input)
        token_file = args.output.joinpath(parent).joinpath(c_file.stem + '.tokens')

        if token_file.exists(): continue
        files.append((c_file, token_file))

    n_files = len(files)
    print(f'There are {n_files:,d} to be processed.')

    # Process the files
    counter = Value('i', 0)

    with Pool(args.num_processes, initializer=init, initargs=(counter,)) as p:
        p.starmap(tokenize, [
            (f[0], f[1], n_files) for f in files
        ])
    
    # subprocess.run(f'wall "Parsing complete for {args.input}"', shell=True)




if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', type=Path, default='/data/datasets/input/WILD')
    parser.add_argument('--output', type=Path, default='/data/datasets/tokens/WILD')
    parser.add_argument('--num_processes', type=int, default=20)

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
