import json, os

for root, dirs, files in os.walk("/home/xuanlong/web_crawl/data/newslink"):
    for file in files:
        ids=set()
        with open(os.path.join(root, file)) as documents:
            for i, line in enumerate(documents):
                doc=json.loads(line)
                ids.add(doc["documentid"])
            print("{} unique ids out from {} jsonl".format(len(ids), i+1), flush=True)

        with open("/home/xuanlong/web_crawl/newslink_ids/used_ids/{}".format(file.replace(".jsonl", ".txt")), "w", encoding="utf8") as file_out:
            file_out.write("\n".join(ids)+"\n")

        print("complete {}". format(file))




