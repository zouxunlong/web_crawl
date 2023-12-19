import os

for root_dir, dirs, files in os.walk("/home/xuanlong/web_crawl/data/newslink"):
    files.sort()
    for file in files:
        path=os.path.join(root_dir, file)
        lines=open(path).readlines()
        print("{}: {}".format(path, len(lines)))


