import os
import json


# for root_dir, dirs, files in os.walk("/home/xuanlong/web_crawl/data/newslink"):
#     files.sort()
#     for file in files:
#         with open(os.path.join(root_dir,file)) as file_in:
#             for line in file_in:
#                 item=json.loads(line)
#                 print(os.path.join(root_dir,file), item["language"])
#                 break


with open("/home/xuanlong/web_crawl/data/newslink/BM_ms.jsonl") as file_in:
    for line in file_in:
        item=json.loads(line)
        print(item)
        break
