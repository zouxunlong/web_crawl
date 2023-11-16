import json
import scrapy
from scrapy.http import Request
from scrapy.crawler import CrawlerProcess
from scrapy.exceptions import CloseSpider


class newslink_Spider(scrapy.Spider):

    name = 'newslink'
    allowed_domains = ['api.newslink.sg']
    handle_httpstatus_list = [401, 500]
    article_api = "https://api.newslink.sg/user/api/user/v1/download"

    def __init__(self):
        self.documentIds = open(file="documentIds_BT_rest.txt").readlines()
        self.cookies = {
            "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdWQiOlsic2VhcmNoIiwidXNlciIsImluZm8iXSwidXNlcl9uYW1lIjoiaTJyYXN0YXIiLCJzY29wZSI6WyJSRUFEIiwiV1JJVEUiXSwiZXhwIjoxNzAwMTI4MzIwLCJhdXRob3JpdGllcyI6WyJVU0VSIl0sImp0aSI6Ik5pbW1KY04yRjRNaEFUa19yTTR0QzQzMUhkayIsImNsaWVudF9pZCI6Im5ld3NsaW5rX3dlYiJ9.MhG7bDA3J0vUOo3xzf1Z1U5laSzE5rLuvzgFurLWlLs",
            "id_token": "eyJhbGciOiJIUzI1NiJ9.eyJqdGkiOiI4MDE5N2FlMS0yNmExLTRlMDItYWNkNS04Y2ExNzlmNDY4NGEiLCJpYXQiOjE3MDAxMjExMjAsInN1YiI6ImkycmFzdGFyIiwidXNlcl9pbmZvIjp7InNlYXJjaF9wcm9maWxlX2luZm8iOiJnZW5lcmFsc2VhcmNoZW58cHJvZmlsZXwxIiwiaGlkZV9wYW5lbCI6ZmFsc2UsInVzZXJfdHlwZSI6ImdvdmVybm1lbnQiLCJoaWdobGlnaHRfdGV4dCI6dHJ1ZSwicHViX2xpc3QiOltdLCJ2aXNpdG9yX2lkIjoiNFVjampISjBDR0lnQzExS1BvRHpjZ1VrOTE0Q0NteEdEQkpmNTJsamU5Yz0iLCJpc19zZWFtbGVzcyI6ZmFsc2UsImNvbnRlbnRfYWxsb3dlZCI6IkFydGljbGVzLEltYWdlcyxQREYsT25saW5lLEFOTixNYWdhemluZSIsInNlYXJjaF90eXBlIjoiZ2VuZXJhbF9zZWFyY2gsIiwic2VhcmNoX2xhbmciOiJlbiIsImhpZGVfbGlua3MiOmZhbHNlLCJwYXlfYWxlcnQiOmZhbHNlfSwiaXNzIjoiTkVXU0xJTksiLCJleHAiOjE3MDAxMjgzMTl9.7izTzLBLk0-622laCGMKy56WO-pYDCzU7Cn-ugiBOnU",
        }

    def start_requests(self):
        for documentId in self.documentIds:
            article_body = {
                "digitalType": "article",
                "documentId": documentId.strip(),
                "sourceType": "article",
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
            open(file="documentIds_BT_used.txt", mode="a", encoding="utf8").write(data['singleDocument']['documentid']+'\n')


def main():
    import os
    print(os.getpid(), flush=True)
    process = CrawlerProcess(
        settings={
            "FEEDS": {
                '/home/xuanlong/web_crawl/data/newslink/BT.jsonl': {
                    "format": "jsonlines",
                    "overwrite": False,
                    "encoding": "utf8",
                },
            },
            # "AUTOTHROTTLE_ENABLED": True,
            "DOWNLOAD_DELAY" : 0.1,
            "LOG_LEVEL": "INFO",
            "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.88 Safari/537.36",
        }
    )
    process.crawl(newslink_Spider)
    process.start()


if __name__ == "__main__":
    main()
