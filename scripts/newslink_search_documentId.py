import copy
import json
import requests
import time
from concurrent.futures import ThreadPoolExecutor
import os

print(os.getpid(), flush=True)

start_time = time.time()

search_api = "https://api.newslink.sg/search/api/public/v1/searchsvc"

documentIds=[]

def post(search_body, i):
    response = requests.post(url=search_api, json=search_body)
    if response.status_code!=200:
        print(json.dumps(search_body), flush=True)
    else:
        data=json.loads(response.text)
        if "content" in data.keys() and data['content']:
            new_ids=[item["documentid"] for item in data['content']]
            documentIds.extend(new_ids)
            open(file="documentIds3.txt",mode="a",encoding="utf8").write('\n'.join(new_ids)+'\n')
            print(len(documentIds), flush=True)
    print('task for i:{} complete.....'.format(i), flush=True)

def main():

    with ThreadPoolExecutor(max_workers=1) as pool:
        with open("/home/xuanlong/web_crawl/search_documentId2.log") as file_in:
            for i, line in enumerate(file_in):
                if line.startswith("{"):
                    search_body=json.loads(line)
                    pool.submit(post, search_body, i)
    print('ThreadPool closed.....', flush=True)

main()
print("--- %s seconds ---" % (time.time() - start_time))
