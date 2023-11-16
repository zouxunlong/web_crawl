import json
import requests
import time
import os
from datetime import date, timedelta

print(os.getpid(), flush=True)

start_time = time.time()

search_api = "https://api.newslink.sg/search/api/public/v1/searchsvc"

documentIds = []


def post(search_body):
    response = requests.post(url=search_api, json=search_body)
    if response.status_code != 200:
        print(json.dumps(search_body), flush=True)
    else:
        data = json.loads(response.text)
        if "content" in data.keys() and data['content']:
            new_ids = [item["documentid"] for item in data['content']]
            documentIds.extend(new_ids)
            open(file="documentIds_BT.txt", mode="a", encoding="utf8").write('\n'.join(new_ids)+'\n')
            print('date:{} add {} more'.format(search_body["dateRange"]["fromDate"], len(new_ids)), flush=True)

# def main1():
    # with open("/home/xuanlong/web_crawl/search_documentId2.log") as file_in:
    #     for i, line in enumerate(file_in):
    #         if line.startswith("{"):
    #             search_body = json.loads(line)
    #             post(search_body, i)
    # print('ThreadPool closed.....', flush=True)


def main():
    for i in range(12372):
        Date = str(date(1990,1,1)+timedelta(i))
        search_body = {
            "dateRange": {
                "fromDate": Date,
                "toDate": Date
            },
            "sortBy": "publicationdate",
            "sortOrder": "asc",
            "publication": [
                "BT"
            ],
            "pageSize": 1000,
            "sourceType": "article",
            "digitalType": "article",
            "pageNo": 1,
            "company": [],
            "section": []
        }
        post(search_body)
        


main()
print("--- %s seconds ---" % (time.time() - start_time))
