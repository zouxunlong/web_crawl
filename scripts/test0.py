

documentIds_remain = open("/home/xuanlong/web_crawl/documentIds_remain.txt").readlines()
documentIds_used1 = open("/home/xuanlong/web_crawl/documentIds_used1.txt").readlines()
documentIds_remain=set(documentIds_remain)-set(documentIds_used1)
documentIds_remain=sorted(documentIds_remain, reverse=True)


open("/home/xuanlong/web_crawl/documentIds_remain.txt","w",encoding="utf8").write("".join(documentIds_remain))
