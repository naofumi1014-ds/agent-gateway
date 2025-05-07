import requests


def html_crawl(url) -> str:
    response = requests.get(url)
    if response.status_code == 200:
        return response.text
    return None