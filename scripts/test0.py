


documentIds=open("/home/xuanlong/web_crawl/documentIds.txt").readlines()
used_documentIds0=open("/home/xuanlong/web_crawl/used_documentIds.txt").readlines()
used_documentIds1=open("/home/xuanlong/web_crawl/used_documentIds1.txt").readlines()
used_documentIds=used_documentIds0+used_documentIds1
set1 = set(documentIds)
set2 = set(used_documentIds)

res = list(set1 - set2)

open(file="/home/xuanlong/web_crawl/documentIds_remain.txt",mode="w",encoding="utf8").write("".join(res))

