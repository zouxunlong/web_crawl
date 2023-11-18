import json
import requests
import time
import os
from datetime import date, timedelta

print(os.getpid(), flush=True)

start_time = time.time()

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
            print('date:{} add {} more, get {} in total.'.format(search_body["dateRange"]["fromDate"], len(new_ids), len(documentIds)), flush=True)
        else:
            print('date:{} no content.'.format(search_body["dateRange"]["fromDate"]), flush=True)


# def main():
#     with open(file="documentIds.txt", mode="a", encoding="utf8") as file,\
#      open("/home/xuanlong/web_crawl/documentIds.log") as file_in:
#         for i, line in enumerate(file_in):
#             if line.startswith("{"):
#                 search_body = json.loads(line)
#                 post(search_body, file)
        


def main():
    with open(file="ME.txt", mode="w", encoding="utf8") as file:
        for i in range(15):
            # fromDate = str(date(1989,7,1)+timedelta(i))
            # toDate = str(date(1989,7,1)+timedelta(i))
            fromDate = str(date(2008+i,1,1))
            toDate = str(date(2008+i+1,1,1)+timedelta(i))
            search_body = {
                "dateRange": {
                    "fromDate": fromDate,
                    "toDate": toDate
                },
                "sortBy": "publicationdate",
                "sortOrder": "asc",
                "publication": [
                    "ME"
                ],
                "pageSize": 9999,
                "sourceType": "article",
                "digitalType": "article",
                "pageNo": 1,
                "company": [],
                "section": []
            }
            post(search_body, file)
        


main()
print("--- %s seconds ---" % (time.time() - start_time))
