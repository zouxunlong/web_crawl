

with open("/home/xuanlong/web_crawl/newslink_ids/online_article_ids.txt") as documents:
    for i, line in enumerate(documents):
        source=line.split("_")[1]
        open("/home/xuanlong/web_crawl/newslink_ids/online/{}.txt".format(source), "a", encoding="utf8").write(line)
        if i%10000==0:
            print(i, flush=True)

print("all finished")




