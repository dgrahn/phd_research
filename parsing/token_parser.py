from pathlib import Path
from tqdm import tqdm
import argparse
import pandas as pd
import re

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
        dataset, file_name, token_num,
        char_start, char_end, token_text,
        token_type,
        channel,
        line_num, line_char
    )

def save_chunk(data, output_dir, num):
    output = output_dir.joinpath(f'{num:05d}.csv')

    df = pd.DataFrame(data, columns=[
        'dataset', 'file_name', 'token_num',
        'char_start', 'char_end', 'token_text',
        'token_type',
        'channel',
        'line', 'line_char'
    ])
    # print(output)
    df.to_csv(output, index=False)

    return [], num + 1

def count_files(input):
    return sum([
        len(list(dataset.iterdir()))
        for dataset in input.iterdir()
    ])

def main(args):
    for dataset in args.input.iterdir():
        if dataset.name != 'WILD': continue

        print(f'Processing {dataset.name}')

        tokens_files = list(dataset.glob('**/*.tokens'))
        pbar = tqdm(total=len(tokens_files))

        dataset_out = args.output.joinpath(dataset.name)
        dataset_out.mkdir(parents=True, exist_ok=True)

        data = []
        file_num = 0

        for tokens_path in tokens_files:
            pbar.set_description(str(tokens_path))

            if len(data) >= args.chunk_size:
                data, file_num = save_chunk(data, dataset_out, file_num)

            file_name = str(tokens_path.relative_to(dataset))
    
            with open(tokens_path, 'r') as tokens:
                for line in tokens:
                    try:
                        tokens = parse_tokens(dataset.name, file_name, line)
                        data.append(tokens)
                    except Exception as e:
                        print('\nLine:', line)
                        raise e

            pbar.update(1)

        save_chunk(data, dataset_out, file_num)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', type=Path, default='tokens/')
    parser.add_argument('--output', type=Path, default='csv/')
    parser.add_argument('--chunk_size', type=int, default=1_000_000)

    args = parser.parse_args()
    main(args)