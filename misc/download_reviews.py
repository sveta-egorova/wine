import requests
import json

headers_test = {
    # 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    # 'Accept': 'application/json',
    # 'accept-encoding': 'gzip, deflate, br',
    # 'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
    # 'Content-Type': 'application/json',
    # 'User-Agent': 'curl/7.71.1',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.193 Safari/537.36',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'en-GB,en;q=0.9',

}


def download():
    page = "https://www.vivino.com/api/wines/1123733/reviews?year=2018&per_page=10&page=2"
    # page = f'https://www.vivino.com/api/wines/{wine_id_list[0]}/latest_reviews?year=N.V.&per_page=10&page={page}'
    print(page)

    # proxies = {
    #   "http": "http://scraperapi:b65a0deee126a85a36e64532b1d7ebeb@proxy-server.scraperapi.com:8001",
    #   "https": "http://scraperapi:b65a0deee126a85a36e64532b1d7ebeb@proxy-server.scraperapi.com:8001"}

    s = requests.Session()
    s.headers.update({
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.193 Safari/537.36'
    })
    s.headers.update({
        'Accept': 'application/json',
        'Content-Type': 'application/json',
    })

    response = s.get(page)
    #     response_pattern = r'2.'
    print(response.status_code)
    print(response.headers)
    # print(response.content)
    if response.status_code // 100 == 2:
        json_str = response.content
        #         print(response.content)
        json_obj = json.loads(json_str)
        for review in json_obj['reviews']:
            reviews_df = reviews_df.append(review, ignore_index=True)
    else:
        print(response.content)


if __name__ == '__main__':
    download()
