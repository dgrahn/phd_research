from collections import defaultdict
from pathlib import Path
from reader import JsonlReader
from tqdm import tqdm
import argparse
import pandas as pd

def main(args):
    reader = JsonlReader(args)
    args.output.mkdir(exist_ok=True, parents=True)

    tokens_hist = defaultdict(lambda: 0)

    for data in tqdm(reader):
        if args.dataset == 'NTR' and data['filename'].startswith('._'): continue
        for key, value in data[args.key].items():
            tokens_hist[key] += value
    
    # Convert to pandas and sort
    tokens_hist = pd.DataFrame.from_dict(tokens_hist, orient='index', columns=['count'])
    tokens_hist[args.key] = tokens_hist.index
    tokens_hist = tokens_hist.sort_values(by='count')
    tokens_hist = tokens_hist[[args.key, 'count']]

    # Save to CSV
    csv = args.output.joinpath(args.key).joinpath(args.dataset + '.csv')
    csv.parent.mkdir(exist_ok=True, parents=True)
    tokens_hist.to_csv(csv, index=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', type=str, required=True)
    parser.add_argument('--key', type=str, required=True)
    parser.add_argument('--input', type=Path, default='/data/datasets/jsonl')
    parser.add_argument('--output', type=Path, default='/data/datasets/metrics/by_dataset')
    args = parser.parse_args()

    try:
        main(args)
    except Exception as e:
        raise(e)