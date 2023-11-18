
# import os
# ids=open("/home/xuanlong/web_crawl/documentIds.txt").readlines()
# length=0
# for rootdir, dirs, files in os.walk("/home/xuanlong/web_crawl/newslink"):
#     for file in files:
#         ids_set=open(os.path.join(rootdir, file)).readlines()
#         ids_set=set(ids_set)
#         open(os.path.join(rootdir, file), "w", encoding="utf8").write("".join(ids_set))
#         length+=len(ids_set)

# print(len(ids))
# print(length)

ids_set=open("/home/xuanlong/web_crawl/documentIds_ST.txt").readlines()
print(len(ids_set))

ids_set=set(ids_set)
print(len(ids_set))

open("/home/xuanlong/web_crawl/newslink/documentIds.txt", "w", encoding="utf8").write("".join(ids_set))
