

ids=open("/home/xuanlong/web_crawl/documentIds_BT_rest.txt").readlines()
ids_used=open("/home/xuanlong/web_crawl/documentIds_BT_used.txt").readlines()

ids_set=set(ids)
ids_used_set=set(ids_used)

ids_rest=ids_set-ids_used_set

open("/home/xuanlong/web_crawl/documentIds_BT_rest.txt", "w", encoding="utf8").write("".join(ids_rest))


