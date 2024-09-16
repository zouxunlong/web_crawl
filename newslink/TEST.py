
import os, json

mapping={}

for root, dirs, files in os.walk("/home/xuanlong/web_crawl/newslink_ids/used_ids"):
    for file in files:
        source, lang, type=file.split(".")
        mapping[source]=lang

open("mapping.json", "w").write(json.dumps(mapping, indent=4))

for root, dirs, files in os.walk("/home/xuanlong/web_crawl/newslink_ids/source_ids"):
    for file in files:
        source, type=file.split(".")
        lang=mapping[source]
        os.rename(os.path.join(root, file), os.path.join(root, ".".join([source, lang, type])))
