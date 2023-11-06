import json
import scrapy
from scrapy.http import Request
from scrapy.crawler import CrawlerProcess


class newslink_Spider(scrapy.Spider):

    name = 'newslink'
    allowed_domains = ['api.newslink.sg']
    article_api = "https://api.newslink.sg/user/api/user/v1/download"

    def __init__(self):
        self.documentIds = open(file="documentIds_remain.txt").readlines()
        self.cookies = {
            "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdWQiOlsic2VhcmNoIiwidXNlciIsImluZm8iXSwidXNlcl9uYW1lIjoiaTJyYXN0YXIiLCJzY29wZSI6WyJSRUFEIiwiV1JJVEUiXSwiZXhwIjoxNjk5MjI5ODc1LCJhdXRob3JpdGllcyI6WyJVU0VSIl0sImp0aSI6IlpOZFVMU3hNYS1rb1VOMkkyaVJ6akZMeDd0OCIsImNsaWVudF9pZCI6Im5ld3NsaW5rX3dlYiJ9.5hmhwhmpBSmU6SHIa1TBPo1EEOmmT_Q7vfRQz0LNmZo",
            "id_token": "eyJhbGciOiJIUzI1NiJ9.eyJqdGkiOiI3ZDQwZmQ4MC0wMjQxLTQ4ZmUtOGQ0Zi1iNDJiY2ZkYzY2NWQiLCJpYXQiOjE2OTkyMjI2NzUsInN1YiI6ImkycmFzdGFyIiwidXNlcl9pbmZvIjp7InNlYXJjaF9wcm9maWxlX2luZm8iOiJnZW5lcmFsc2VhcmNoZW58cHJvZmlsZXwxIiwiaGlkZV9wYW5lbCI6ZmFsc2UsInVzZXJfdHlwZSI6ImdvdmVybm1lbnQiLCJoaWdobGlnaHRfdGV4dCI6dHJ1ZSwicHViX2xpc3QiOltdLCJ2aXNpdG9yX2lkIjoiNFVjampISjBDR0lnQzExS1BvRHpjZ1VrOTE0Q0NteEdEQkpmNTJsamU5Yz0iLCJpc19zZWFtbGVzcyI6ZmFsc2UsImNvbnRlbnRfYWxsb3dlZCI6IkFydGljbGVzLEltYWdlcyxQREYsT25saW5lLEFOTixNYWdhemluZSIsInNlYXJjaF90eXBlIjoiZ2VuZXJhbF9zZWFyY2gsIiwic2VhcmNoX2xhbmciOiJlbiIsImhpZGVfbGlua3MiOmZhbHNlLCJwYXlfYWxlcnQiOmZhbHNlfSwiaXNzIjoiTkVXU0xJTksiLCJleHAiOjE2OTkyMjk4NzR9.pGVx9xK-FaVEqaZ6JNQTc_sbnK4Gg6K6hKUYlfFAFUs",
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
        data = json.loads(response.text)
        if "singleDocument" in data.keys() and data['singleDocument']:
            yield data['singleDocument']
            open(file="used_documentIds1.txt", mode="a", encoding="utf8").write(
                data['singleDocument']['documentid']+'\n')


def main():
    process = CrawlerProcess(
        settings={
            "FEEDS": {
                '/home/xuanlong/web_crawl/data/newslink/ST.jsonl': {
                    "format": "jsonlines",
                    "overwrite": False,
                    "encoding": "utf8",
                },
            },
            "AUTOTHROTTLE_ENABLED" : True,
            # "DOWNLOAD_DELAY" : 0.5,
            "LOG_LEVEL": "INFO",
            "DOWNLOADER_MIDDLEWARES": {
                "middlewares.WebCrawlDownloaderMiddleware": 543,
            },        }
    )
    process.crawl(newslink_Spider)
    process.start()


if __name__ == "__main__":
    main()
