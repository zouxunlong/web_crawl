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

    def __init__(self, source):
        self.documentIds = open(file="/home/xuanlong/web_crawl/newslink_ids/online/{}_rest.txt".format(source)).readlines()
        self.cookies = COOKIES
        self.source = source

    def start_requests(self):
        for documentId in self.documentIds:
            article_body = {
                "digitalType": "article",
                "documentId": documentId.strip(),
                "sourceType": "online_article",
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
            open(file="/home/xuanlong/web_crawl/newslink_ids/online/{}_used.txt".format(self.source), mode="a", encoding="utf8").write(data['singleDocument']['documentid']+'\n')


def main(
    source : str,
):
    import os
    print(os.getpid(), flush=True)
    process = CrawlerProcess(
        settings={
            "FEEDS": {
                '/home/xuanlong/web_crawl/data/newslink/online/{}.jsonl'.format(source): {
                    "format": "jsonlines",
                    "overwrite": False,
                    "encoding": "utf8",
                },
            },
            # "AUTOTHROTTLE_ENABLED": True,
            "DOWNLOAD_DELAY" : 0.03,
            "LOG_LEVEL": "INFO",
            "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.88 Safari/537.36",
        }
    )
    process.crawl(newslink_Spider, source)
    process.start()


if __name__ == "__main__":
    fire.Fire(main)
