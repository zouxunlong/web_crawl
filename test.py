import json


# lines=open("/home/xuanlong/web_crawl/data/newslink/articles/ST_en.jsonl").readlines()
# n=0
# with open("./straitstimes.jsonl", "w", encoding="utf8") as f:
#     for i, line in enumerate(lines):
#         item=json.loads(line)
#         if "byline" in item.keys() and not item["byline"]=="":
#             n+=1
#             if n<600000:
#                 continue
#             item["byline"]=item["byline"].split(",")[0]
#             f.write(json.dumps(item, ensure_ascii=False)+"\n")
#     print("{}".format(i), flush=True)



with open("./straitstimes.jsonl") as f,  open("./straitstimes2.jsonl", "w", encoding="utf8") as f_out:
    for i, line in enumerate(f):
        item=json.loads(line)
        try:
            f_out.write(json.dumps({"headline": item["headline"], "text": item["bodyarticle"], "date": item["publicationdate"], "author": item["byline"]}, ensure_ascii=False)+"\n")
        except Exception as e:
            print(e, flush=True)
    print(i, flush=True)
