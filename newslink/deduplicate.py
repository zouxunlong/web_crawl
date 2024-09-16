import json, os

for root, dirs, files in os.walk("/home/xuanlong/web_crawl/data/newslink"):
    for file in files:
        ids=set()
        docs=[]
        with open(os.path.join(root, file)) as documents:
            for i, line in enumerate(documents):
                doc=json.loads(line)
                id=doc["documentid"]
                if not id in ids:
                    ids.add(id)
                    docs.append(doc)
                if i%10000==0:
                    print(i, flush=True)
            print(i+1, flush=True)
            print(len(ids), flush=True)
            print(len(docs), flush=True)

        with open(os.path.join("/home/xuanlong/web_crawl/data/newslink_deduped", file), "w", encoding="utf8") as file_out:
            for doc in docs:
                file_out.write(json.dumps(doc, ensure_ascii=False)+"\n")

        print("finished {}".format(file), flush=True)
    print("all finished")




