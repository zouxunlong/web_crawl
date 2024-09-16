import os


def remove_used(file):
    ids=open("/home/xuanlong/web_crawl/newslink_ids/rest_ids/{}".format(file)).readlines()
    print(len(ids), flush=True)

    ids_set=set(ids)
    print(len(ids_set), flush=True)

    ids_used=open("/home/xuanlong/web_crawl/newslink_ids/used_ids/{}".format(file)).readlines()
    print(len(ids_used), flush=True)

    ids_used_set=set(ids_used)
    print(len(ids_used_set), flush=True)

    ids_rest=ids_set-ids_used_set
    print(len(ids_rest), flush=True)

    open("/home/xuanlong/web_crawl/newslink_ids/rest_ids/{}".format(file), "w", encoding="utf8").write("".join(ids_rest))
    open("/home/xuanlong/web_crawl/newslink_ids/used_ids/{}".format(file), "w", encoding="utf8").write("".join([]))


if __name__=="__main__":
    for root, dirs, files in os.walk("/home/xuanlong/web_crawl/newslink_ids/rest_ids"):
        for file in files:
            # if not file in ["TCO.en.txt"]:
            #     continue
            remove_used(file)
            print("complete {}". format(file), flush=True)

