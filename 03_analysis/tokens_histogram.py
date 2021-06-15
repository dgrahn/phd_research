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
        num_tokens = data['num_tokens']
        tokens_hist[num_tokens] += 1
    
    # Convert to pandas and sort
    tokens_hist = pd.DataFrame.from_dict(tokens_hist, orient='index', columns=['file_count'])
    tokens_hist['tokens_count'] = tokens_hist.index
    tokens_hist = tokens_hist.sort_values(by='tokens_count')

    # Save to CSV
    csv = args.output.joinpath(args.dataset + '.csv')
    tokens_hist.to_csv(csv, index=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', type=str)
    parser.add_argument('--input', type=Path, default='/data/datasets/jsonl')
    parser.add_argument('--output', type=Path, default='/data/datasets/metrics/tokens_histogram')
    args = parser.parse_args()

    try:
        main(args)
    except Exception as e:
        # wall(f'Error ingesting {args.input}')
        raise(e)