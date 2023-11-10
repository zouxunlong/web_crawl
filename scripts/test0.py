

documentIds = open("/home/xuanlong/web_crawl/documentIds.txt").readlines()
documentIds_used = open("/home/xuanlong/web_crawl/documentIds_used.txt").readlines()
documentIds_remain=set(documentIds)-set(documentIds_used)
documentIds_remain=sorted(documentIds_remain, reverse=True)


open("/home/xuanlong/web_crawl/documentIds_remain.txt","w",encoding="utf8").write("".join(documentIds_remain))
