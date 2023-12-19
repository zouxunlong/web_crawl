import json

dict={}

with open("/home/xuanlong/web_crawl/data/newslink/BT.jsonl") as documents:
    for i, line in enumerate(documents):
        doc=json.loads(line)
        dict[doc["documentid"]]=doc
    print(i)

with open("/home/xuanlong/web_crawl/data/newslink/BT.jsonl", "w", encoding="utf8") as file:
    for i, document in enumerate(dict.values()):
        file.write(json.dumps(document)+"\n")
    print(i)
print("all finished")




