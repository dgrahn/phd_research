from datetime import datetime
from pathlib import Path
import argparse
import asyncio
import asyncpg
import subprocess
import multiprocessing

def wall(message):
    subprocess.run(f'wall "{message}"', shell=True)

# async def run():
#     con = await asyncpg.connect(user='dangrahn', password='licorice')
#     result = await con.copy_to_table(
#         'tokens', source='test.csv',
#         format='csv', header=True, query='update')
#     print(result)
# asyncio.get_event_loop().run_until_complete(run())

async def ingest(start, i, n_files, pool, csv):
    async with pool.acquire() as con:
        try:
            result = await con.copy_to_table(
                'tokens', source=csv,
                format='csv', header=True)
        except asyncpg.exceptions.UniqueViolationError:
            # We'll just assume this file has already been ingested.
            pass


    if i == 0: return
    perc = i / n_files * 100.0
    elapsed = datetime.now() - start
    per = elapsed / (i + 1e-10)
    rem = (n_files - i) * per
    print(f'{i:8,d} / {n_files:,d} -- {perc:3.2f}% [{elapsed}, {per.total_seconds():.5f} it., {rem} rem.] -- {csv.name}')

    if i % 10_000 == 0:
        wall(f'pg ingest: {i} / {n_files} - {rem} rem.')


async def main(args):
    print('Collecting files...')
    csv_files = list(args.input.rglob('*.csv'))
    n_files = len(csv_files)
    print(f'There are {n_files:,d} files to ingest.')

    start = datetime.now()

    async with asyncpg.create_pool(user=args.user, password=args.password) as pool:
        await asyncio.gather(*[
            ingest(start, i, n_files, pool, csv)
            for i, csv in enumerate(csv_files)
        ])
    
    wall(f'Done ingesting {n_files:,d} for {args.input}')

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', type=Path, default='/data/datasets/csv/TBO')
    parser.add_argument('--user', type=str, default='dangrahn')
    parser.add_argument('--password', type=str)

    args = parser.parse_args()

    try:
        asyncio.get_event_loop().run_until_complete(main(args))
    except Exception as e:
        wall(f'Error ingesting {args.input}')
        raise(e)