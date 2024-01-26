import json
import requests
import time
import os
import fire
from datetime import date, timedelta


search_api = "https://api.newslink.sg/search/api/public/v1/searchsvc"

documentIds = []


def post(search_body, file):
    response = requests.post(url=search_api, json=search_body)
    if response.status_code != 200:
        print(json.dumps(search_body), flush=True)
    else:
        data = json.loads(response.text)
        if "content" in data.keys() and data['content']:
            new_ids = [item["documentid"] for item in data['content']]
            documentIds.extend(new_ids)
            file.write('\n'.join(new_ids)+'\n')
            print('date:{} to {} add {} more, get {} in total.'.format(
                search_body["dateRange"]["fromDate"], search_body["dateRange"]["toDate"], len(new_ids), len(documentIds)), flush=True)
        else:
            print('date:{} to {}  no content.'.format(search_body["dateRange"]["fromDate"], search_body["dateRange"]["toDate"]), flush=True)


# def main():
#     with open(file="documentIds.txt", mode="a", encoding="utf8") as file,\
#      open("/home/xuanlong/web_crawl/documentIds.log") as file_in:
#         for i, line in enumerate(file_in):
#             if line.startswith("{"):
#                 search_body = json.loads(line)
#                 post(search_body, file)


def main(
    source : str,
):
    with open(file="{}.txt".format(source), mode="a", encoding="utf8") as file:
        for year in range(2011, 2025):
            for month in range(1, 13):
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
                    "sortBy": "publicationdate",
                    "sortOrder": "asc",
                    "publication": [
                        source
                    ],
                    "pageSize": 2000,
                    "sourceType": "online_article",
                    "digitalType": "article",
                    "pageNo": 1,
                    "company": [],
                    "section": []
                }
                post(search_body, file)



if __name__ == "__main__":
    print(os.getpid(), flush=True)
    start_time = time.time()
    fire.Fire(main)
    print("--- %s seconds ---" % (time.time() - start_time))
