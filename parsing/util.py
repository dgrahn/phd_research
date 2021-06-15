from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import subprocess
from dataclasses import dataclass
import shutil

@dataclass
class FileCache:
    cache: Path
    input: Path
    in_extension: str
    output: Path
    out_extension: str

    def __post_init__(self):
        self._failed_path = self.cache.joinpath('failed.txt')
        self._timeout_path = self.cache.joinpath('timeout.txt')
    
    def _load_failed(self):
        self.failed = []

        if self._failed_path.exists():
            with open(self._failed_path) as f:
                self.failed += f.readlines()
        
        if self._timeout_path.exists():
            with open(self._timeout_path) as f:
                self.failed += f.readlines()

    def create(self, start):
        self.files = []

        print('Loading files...')
        for i, src_file in enumerate(self.input.rglob('*' + self.in_extension)):
            if i % 100_000 == 0:
                print(f'\t{i:,d} -- {datetime.now() - start}')
                start = datetime.now()

            parent = src_file.parent.relative_to(self.input)
            dst_file = self.output.joinpath(parent).joinpath(src_file.stem + self.out_extension)
            self.files.append((src_file, dst_file))
        
        print(f'\tThere are {len(self.files):,d} files in the index.')

    def fail(self, path):
        subprocess.run(f'echo "{path}" >> {self._failed_path}', shell=True)
    
    def timeout(self, path):
        subprocess.run(f'echo "{path}" >> {self._timeout_path}', shell=True)

    def save(self):
        self.cache.mkdir(exist_ok=True, parents=True)
        df = pd.DataFrame(self.files, columns=['src_file', 'dst_file'])
        df.to_csv(self.cache.joinpath('index.csv'), index=False)

    def clear(self):
        if self.cache.exists():
            shutil.rmtree(self.cache)
    
    def load(self):
        print('Loading Index...')
        df = pd.read_csv(self.cache.joinpath('index.csv'), nrows=1_000_000)
        self.files = [ (Path(src), Path(dst)) for src, dst in df.values ]
        print(f'\tThere are {len(self.files):,d} files in the index.')

    def filter(self, start):
        self._load_failed()
        print('Failed:', len(self.failed))

        # Convert failed to a dictionary for faster lookups
        failed = { f.strip(): True for f in self.failed }
        filtered = []

        print('Filtering files...')
        for i, (src_file, dst_file) in enumerate(self.files):
            if i % 100_000 == 0:
                print(f'\t{i:,d} -- {datetime.now() - start}')
                start = datetime.now()
            if Path(dst_file).exists(): continue
            if str(src_file) in failed: continue

            filtered.append((src_file, dst_file))
        
        self.files = filtered


def filter_files(start, unfiltered, failed):
    # Filter the files
    filtered = []

    # Convert failed to a dictionary for faster lookups
    print('Failed:', len(failed))
    failed = { f: True for f in failed }

    print('Filtering files...')
    for i, (src_file, dst_file) in enumerate(unfiltered):
        if i % 100_000 == 0:
            print(f'\t{i:,d} -- {datetime.now() - start}')
            start = datetime.now()
        if Path(dst_file).exists(): continue
        if src_file in failed: continue

        filtered.append((Path(src_file), Path(dst_file)))
    
    return filtered

def create_index(start, input_path, pattern, output_path, extension):
    # Get the token files
    files = []

    print('Loading files...')
    for i, src_file in enumerate(input_path.rglob(pattern)):
        if i % 100_000 == 0:
            print(f'\t{i:,d} -- {datetime.now() - start}')
            start = datetime.now()

        parent = src_file.parent.relative_to(input_path)
        dst_file = output_path.joinpath(parent).joinpath(src_file.stem + extension)
        files.append((src_file, dst_file))

    return files

def save_index(files, filename='index.csv'):
    df = pd.DataFrame(files, columns=['src_file', 'dst_file'])
    df.to_csv(filename, index=False)