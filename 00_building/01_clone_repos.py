from pathlib import Path
from tqdm import tqdm
import pandas as pd
import subprocess
from datetime import datetime

print('Starting at:', datetime.now())

repos = pd.read_csv('repos.csv')
base = Path('repos')
base.mkdir(exist_ok=True)

for i, row in tqdm(repos.iterrows(), total=repos.shape[0]):
    try:
        # Starting repo path
        repo = base.joinpath(row.full_name)

        if repo.exists():
            print(f'\nAlready downloaded ({i} / {repos.shape[0]}). Continuing...')
            continue

        # Progress notifications
        if i % 500 == 0:
            subprocess.run(
                f'wall "{i} / {repos.shape[0]} - {datetime.now()}"',
                shell=True)

        # Make the repo
        repo.mkdir(parents=True, exist_ok=True)
        path = str(repo)

        # Clone the repo
        print('')
        subprocess.run(
            f'git clone git@github.com:{row.full_name}.git {path}',
            shell=True,
            check=True)

        # Delete non-c files
        subprocess.run(
            f"find {path} -type f ! -regex '.*\.\(c\|cpp\)' -delete",
            shell=True,
            stdout=subprocess.DEVNULL)

        # Delete empty directories
        subprocess.run(
            f'find {path}/** -type d -empty -delete',
            shell=True,
            stdout=subprocess.DEVNULL)

    except Exception as e:
        subprocess.run(
            f'wall "{row.full_name} - {e}"',
            shell=True)
        # raise e
