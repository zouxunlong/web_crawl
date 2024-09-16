import json
import scrapy
from scrapy.http import Request
from scrapy.crawler import CrawlerProcess
from scrapy.exceptions import CloseSpider
from cookies import COOKIES
import fire


class newslink_Spider(scrapy.Spider):

    name = 'newslink'
    allowed_domains = ['api.newslink.sg']
    handle_httpstatus_list = [401]
    article_api = "https://api.newslink.sg/user/api/user/v1/download"

    def __init__(self, file_ids):
        self.documentIds = open(file="/home/xuanlong/web_crawl/newslink_ids/rest_ids/{}".format(file_ids)).readlines()
        self.cookies = COOKIES
        self.digitalType = "online_article" if file_ids.split(".")[0].endswith("O") else "article"
        self.file_ids = file_ids

    def start_requests(self):
        for documentId in self.documentIds:
            article_body = {
                "digitalType": "article",
                "sourceType": self.digitalType,
                "documentId": documentId.strip(),
                "keywords": ""
            }
            yield Request(self.article_api, callback=self.parse, method="POST", cookies=self.cookies, body=json.dumps(article_body), dont_filter=True)

    def parse(self, response):
        if response.status == 401:
            raise CloseSpider("401 Access token expired")
        if response.status == 500:
            raise CloseSpider("500 Internal Server Error")
        data = json.loads(response.text)
        if "singleDocument" in data.keys() and data['singleDocument']:
            yield data['singleDocument']
            open(file="/home/xuanlong/web_crawl/newslink_ids/used_ids/{}".format(self.file_ids), mode="a", encoding="utf8").write(data['singleDocument']['documentid']+'\n')


def main(
    file_ids : str
):
    import os
    print(os.getpid(), flush=True)
    process = CrawlerProcess(
        settings={
            "FEEDS": {
                '/home/xuanlong/web_crawl/data/newslink/{}'.format(file_ids.replace(".txt", ".jsonl")): {
                    "format": "jsonlines",
                    "overwrite": False,
                    "encoding": "utf8",
                },
            },
            "AUTOTHROTTLE_ENABLED": True,
            # "DOWNLOAD_DELAY" : 0.05,
            "LOG_LEVEL": "INFO",
            "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.88 Safari/537.36",
        }
    )
    process.crawl(newslink_Spider, file_ids)
    process.start()


if __name__ == "__main__":
    fire.Fire(main)
