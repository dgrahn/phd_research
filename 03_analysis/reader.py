import json

class JsonlReader:
    def __init__(self, args):
        self.input = args.input.joinpath(args.dataset)
        self.files = list(self.input.rglob('*.jsonl'))
        print(f'There are {len(self.files):,d} files to process')
    
    def __iter__(self):
        for filename in self.files:
            with open(filename, 'r') as f:
                while True:
                    line = f.readline()
                    if not line: break

                    try:
                        yield json.loads(line)
                    except json.decoder.JSONDecodeError as e:
                        print(e)
                        print(line)
    
    def __len__(self):
        if not self.files: return 0

        with open(self.files[0], 'r') as f:
            full_count = len(f.readlines())
        
        if len(self.files) == 1: return full_count

        with open(self.files[-1], 'r') as f:
            last_count = len(f.readlines())
        
        return full_count * (len(self.files) - 1) + last_count
