from collections import defaultdict
from pathlib import Path
from reader import JsonlReader
from tqdm import tqdm
import argparse
import numpy as np
import pandas as pd

def main(args):
    reader = JsonlReader(args)

    hists = defaultdict(lambda: defaultdict(lambda: 0))

    for data in tqdm(reader):
        for key, count in data[args.key].items():
            if args.ratio:
                ratio = np.round(count / data['num_tokens'], 4)
                hists[key][ratio] += 1
            else:
                hists[key][count] += 1
    
    # Convert to pandas and sort
    df = pd.DataFrame(columns=sorted(hists.keys()))

    for key, counts in hists.items():
        for num, count in counts.items():
            df.at[num, key] = count

    # Sort by index and
    df = df.sort_index(ascending=True)
    df['num'] = df.index
    df = df.fillna(0)

    # Save to CSV
    dir_name = args.key + ('-ratio' if args.ratio else '')
    csv = args.output.joinpath(dir_name).joinpath(args.dataset + '.csv')
    csv.parent.mkdir(exist_ok=True, parents=True)
    df.to_csv(csv, index=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', type=str, required=True)
    parser.add_argument('--key', type=str, required=True)
    parser.add_argument('-ratio', action='store_true')
    parser.add_argument('--input', type=Path, default='/data/datasets/jsonl')
    parser.add_argument('--output', type=Path, default='/data/datasets/metrics/by-file/')
    args = parser.parse_args()
    print(args)

    try:
        main(args)
    except Exception as e:
        # wall(f'Error ingesting {args.input}')
        raise(e)