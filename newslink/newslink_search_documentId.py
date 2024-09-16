import json
import requests
import time
import os
import fire
from datetime import date, timedelta


search_api = "https://api.newslink.sg/search/api/public/v1/searchsvc"
documentIds = []


def post(search_body, file, pageSize):
    response = requests.post(url=search_api, json=search_body)
    if response.status_code != 200:
        # print(json.dumps(search_body), flush=True)
        print('date:{} to {} not 200 error. try again'.format(search_body["dateRange"]["fromDate"], search_body["dateRange"]["toDate"]), flush=True)
        post(search_body, file, pageSize)
    else:
        data = json.loads(response.text)
        if "content" in data.keys() and data['content']:
            new_ids = [item["documentid"] for item in data['content']]
            documentIds.extend(new_ids)
            file.write('\n'.join(new_ids)+'\n')
            print('date:{} to {} add {} more, get {} in total.'.format(
                search_body["dateRange"]["fromDate"], search_body["dateRange"]["toDate"], len(new_ids), len(documentIds)), flush=True)
            if len(new_ids)==pageSize:
                search_body['pageNo']+=1
                post(search_body, file, pageSize)
        else:
            print('date:{} to {}  no content.'.format(search_body["dateRange"]["fromDate"], search_body["dateRange"]["toDate"]), flush=True)


def main(pageSize=500):
    with open(file="/home/xuanlong/web_crawl/newslink_ids/article_ids.txt", mode="a", encoding="utf8") as file:
        for year in range(2023, 2024):
            for month in range(1, 13):
                if date(year, month, 1)<date(2023, 10, 1):
                    continue
                if date(year, month, 1)>=date(2023, 11, 1):
                    continue
                fromDate = str(date(year, month, 1))
                if month==12:
                    toDate = str(date(year+1, 1, 1)-timedelta(1))
                else:
                    toDate = str(date(year, month+1, 1)-timedelta(1))

                search_body = {
                    "dateRange": {
                        "fromDate": fromDate,
                        "toDate": toDate
                    },
                    "digitalType": "article",
                    "pageNo": 1,
                    "publication": [],
                    "pageSize": pageSize,
                    "sourceType": "article",
                    "company": []
                }
                post(search_body, file, pageSize)


if __name__ == "__main__":
    print(os.getpid(), flush=True)
    start_time = time.time()
    fire.Fire(main)
    print(f"--- {time.time() - start_time} seconds ---")
