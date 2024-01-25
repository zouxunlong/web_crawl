import json

ids=set()

with open("/home/xuanlong/web_crawl/data/newslink/ME_en.jsonl") as documents:
    for i, line in enumerate(documents):
        doc=json.loads(line)
        ids.add(doc["documentid"])

with open("/home/xuanlong/web_crawl/newslink/ME_used.txt", "w", encoding="utf8") as file:
    
    file.write("\n".join(ids))

print("all finished")




