from twisted.internet import reactor
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
from time import localtime, strftime
from bs4 import BeautifulSoup



class SG_Spider(CrawlSpider):
    name = "sg_spider"

    def __init__(self, domain):
        self.domain = domain
        self.start_urls = ["https://www."+domain]
        self.rules = [Rule(LinkExtractor(allow=[domain[:-3],],  deny=(r"\.pdf$",)), callback='parse_item', follow=True)]
        super()._compile_rules()

    def parse_item(self, response):

        try:
            if BeautifulSoup(response.text, "html.parser").find():

                html=response.text

                yield {
                    'url': response.url,
                    'time': strftime("%Y-%m-%d %H:%M:%S", localtime()),
                    'html': html,
                }
        except AttributeError as e:
            print(e, response.url, flush=True)


    def closed(self, reason):
        self.crawler.stats.set_value("spider_name", self.name)


def main():

    settings = {
        "FEEDS": {
            '../data/massive_crawl/%(domain)s.jsonl': {
                'format': 'jsonlines',
                "overwrite": True,
                "encoding": "utf8",
            }
        },
        "DOWNLOADER_MIDDLEWARES": {
            "crawler_sg_middleware.RandomUserAgentMiddleware": 543,
            "scrapy.downloadermiddlewares.useragent.UserAgentMiddleware": None,
        },
        "LOG_LEVEL": "INFO",
        "DEPTH_PRIORITY": 1,
        "SCHEDULER_DISK_QUEUE": "scrapy.squeues.PickleFifoDiskQueue",
        "SCHEDULER_MEMORY_QUEUE": "scrapy.squeues.FifoMemoryQueue",
        "ROBOTSTXT_OBEY": False,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 200,
        "CONCURRENT_REQUESTS": 200,
        "COOKIES_ENABLED": False,
        "RETRY_ENABLED": False,
        "DOWNLOAD_TIMEOUT": 15,
        "REACTOR_THREADPOOL_MAXSIZE": 20,
        "REQUEST_FINGERPRINTER_IMPLEMENTATION": "2.7",
    }

    configure_logging(settings)
    runner = CrawlerRunner(settings)

    lines=open(file='./domains_count_en.txt', mode='r', encoding='utf8').readlines()
    domains=[line.split("|||")[1][:-11] for line in lines]

    for domain in domains[:100]:
        runner.crawl(SG_Spider, domain=domain)
    d = runner.join()
    d.addBoth(lambda _: reactor.stop())

    reactor.run()


if __name__ == "__main__":
    main()
    print("finished all", flush=True)
