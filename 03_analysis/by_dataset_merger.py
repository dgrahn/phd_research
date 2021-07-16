from pathlib import Path
import argparse
import pandas as pd
from simple_term_menu import TerminalMenu

def select(title, options):
    terminal_menu = TerminalMenu(options, title=title)
    return options[terminal_menu.show()]

def main(args):
    metric = select(
        'Select a metric:',
        [ f.name for f in args.input.iterdir() ]
    )

    args.input = args.input.joinpath(metric)
    metric_files = sorted(list(f for f in args.input.rglob('*.csv') if f.stem != metric))
    print('Files:', metric_files)

    df = pd.read_csv(metric_files[0])
    join_column = select('Select a join column:', df.columns)
    metric_column = select('Select a metric column:', df.columns)
    drop_num = int(input('Drop:?'))

    df = None

    for metric_file in metric_files:
        print(f'Loading {metric_file.stem}...', end=' ')
        df2 = pd.read_csv(metric_file)
        print('Done')

        if drop_num != 0:
            num = df2.shape[0]
            df2 = df2.loc[df2[metric_column] > drop_num]
            print(metric_file.stem, num, df2.shape[0])

        df2 = df2.rename(columns={ metric_column: metric_file.stem })

        if df is None:
            df = df2
        else:
            df = df.join(df2.set_index(join_column), on=join_column, how='outer')
        print('Running Total:', df.shape[0])
    
    # Fill with 0s
    if select('Fill with 0s?:', [ 'Yes', 'No' ]) == 'Yes':
        df = df.fillna(0)
    
    # Move join column to start
    col = df.pop(join_column)
    df.insert(0, col.name, col)

    print('Total:', df.shape[0])
    df.to_csv(args.input.joinpath(metric + '.csv'), index=False)

    print(df.describe())
    print(df.columns)
    print(df.shape)

    

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--metric', type=str, default=None)
    parser.add_argument('--join_column', type=str, default=None)
    parser.add_argument('--input', type=Path, default='/data/datasets/metrics/by_dataset')
    args = parser.parse_args()

    try:
        main(args)
    except Exception as e:
        # wall(f'Error ingesting {args.input}')
        raise(e)