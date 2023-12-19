import json
import os

for root_dir, dirs, files in os.walk("/home/xuanlong/web_crawl/data/newslink"):
    files.sort()
    for file in files:
        file_path=os.path.join(root_dir, file)
        pub=file.split(".")[0]
        with open(file_path) as file_in, open(file_path.replace(".jsonl", "_new.jsonl"), "w", encoding="utf8") as file_out:
            for i, line in enumerate(file_in):
                doc=json.loads(line)
                if doc["publication"] in [pub]:
                    file_out.write(line)
                if i%50000==0:
                    print(i,flush=True)
        print("{} finished".format(file_path), flush=True)


print("all finished", flush=True)


