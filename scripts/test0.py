


documentIds=open("/home/xuanlong/web_crawl/documentIds.txt").readlines()
used_documentIds=open("/home/xuanlong/web_crawl/used_documentIds.txt").readlines()

set1 = set(documentIds)
set2 = set(used_documentIds)
res = list(set1 - set2)

open(file="/home/xuanlong/web_crawl/documentIds_remain0.txt",mode="w",encoding="utf8").write("".join(res))

