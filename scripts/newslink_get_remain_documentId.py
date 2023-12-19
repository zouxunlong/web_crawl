
def remove_used(domain):
    ids=open("/home/xuanlong/web_crawl/newslink/{}_rest.txt".format(domain)).readlines()
    ids_used=open("/home/xuanlong/web_crawl/newslink/{}_used.txt".format(domain)).readlines()
    ids_set=set(ids)
    ids_used_set=set(ids_used)
    ids_rest=ids_set-ids_used_set
    open("/home/xuanlong/web_crawl/newslink/{}_rest.txt".format(domain), "w", encoding="utf8").write("".join(ids_rest))
    open("/home/xuanlong/web_crawl/newslink/{}_used.txt".format(domain), "w", encoding="utf8").write("")


if __name__=="__main__":
    remove_used("ZB")

