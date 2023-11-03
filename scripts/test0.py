


list=open("/home/xuanlong/web_crawl/documentIds.txt").readlines()

list.sort()
with open(file="/home/xuanlong/web_crawl/documentIds_sorted.txt", mode="w", encoding="utf8") as file:
    for line in list:
        file.write(line)
