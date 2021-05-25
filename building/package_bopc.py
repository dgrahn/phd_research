import argparse
from pathlib import Path
from tqdm import tqdm
import pandas as pd


def main(args):
    data = []

    print('Getting files...')
    c_files = list(args.input.rglob('*.c*'))

    print('Sorting...')
    c_files = sorted(c_files)

    # FIXME sort and progress bar
    file_num = 0

    print('Processing...')
    for i, c_file in tqdm(enumerate(c_files)):
        if not c_file.is_file(): continue

        repo = '/'.join(c_file.relative_to(args.input).parts[:2])
        path = c_file.relative_to(args.input.joinpath(repo))
        name = c_file.name
        size = c_file.stat().st_size

        with open(c_file, encoding='latin1') as f:
            data.append({
                'repo': repo,
                'path': str(path),
                'name': name,
                'size': size,
                'contents': f.read()
            })

        if i % args.chunk == 0 and i != 0:
            df = pd.DataFrame(data)
            df.to_parquet(args.output.format(file_num=file_num))
            file_num += 1
            data = []
    
    df = pd.DataFrame(data)
    df.to_parquet(args.output.format(file_num=file_num))
    file_num += 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', type=Path, default='/data/datasets/input/WILD')
    parser.add_argument('--output', type=str, default='/data/datasets/bopc/bopc_{file_num:04d}.parquet')
    parser.add_argument('--chunk', type=int, default=99_999)

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
