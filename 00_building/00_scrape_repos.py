#FIXME Document
import pandas as pd
import requests
import time

TOKEN = 'b67b7c4345e89878c18b568617326ab6b558183d'

class RepoScraper:
    def __init__(self, lang, min_stars=100):
        self.lang = lang
        self.min_stars = min_stars

    def get_next(self, max_stars, page):
        # FIXME Handle more than 100 repos with the same number of stars
        q = f'language:{self.lang} sort:stars stars:<={max_stars:d}'
        print(q, 'page:', page)

        response = requests.get(
            'https://api.github.com/search/repositories',
            headers = { 'Authorization': f'token {TOKEN}' },
            params = { 'q': q, 'per_page': 100, 'page': page },
        )

        if response.status_code != 200:
            raise Exception(f'{response.status_code}: {response.json()}')
        return response.json()['items']
    
    def scan(self):
        max_stars = 100_000_000
        page = 0

        while max_stars >= self.min_stars:
            start = time.time()
            next_stars = 0

            for repo in self.get_next(max_stars, page):
                yield repo
                next_stars = repo['stargazers_count']
            
            if next_stars == max_stars:
                page += 1
                if page > 10:
                    print('Too many for page')
                    return
            else:
                max_stars = next_stars

def get_row(repo):
    return {
        'language': repo['language'],
        'full_name': repo['full_name'],
        'fork': repo['fork'],
        'size': repo['size'],
        'stars': repo['stargazers_count'],
    }

c_scraper = RepoScraper(lang='c', min_stars=10)
cpp_scraper = RepoScraper(lang='c++', min_stars=10)

rows = []

for repo in c_scraper.scan():
    rows.append(get_row(repo))

for repo in cpp_scraper.scan():
    rows.append(get_row(repo))

df = pd.DataFrame(rows)
df = df.drop_duplicates()
df.to_csv('repos.csv', index=False)