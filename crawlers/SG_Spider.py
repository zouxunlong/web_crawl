import fire
from simhash import Simhash
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.crawler import CrawlerProcess
import justext


class SG_Spider(CrawlSpider):

    name = "sg_spider"
    allowed_domains = ["citynomads.com"]
    start_urls = ["http://citynomads.com"]
    rules = (Rule(LinkExtractor(allow=("https://citynomads.com",)), callback='parse_item', follow=True),)
    ids=set()

    def parse_item(self, response):

        file_type = response.headers[b'Content-Type']
        file_type = file_type.split(b';')[0].split(b'/')[1]
        if file_type in [b'html']:

            url = response.url
            html = response.text
            paragraphs = [paragraph.text for paragraph in justext.justext(html, justext.get_stoplist("English")) if not paragraph.is_boilerplate]
            text='\n'.join(paragraphs).strip()

            if text:

                id=str(Simhash(text, f=72, reg=r'[\S]').value)

                if id not in self.ids:
                    self.ids.add(id)
                    yield {
                        'url': url,
                        'language_type': 'en',
                        'source': "citynomads.com",
                        'text': text,
                        '_id': id,
                    }
        

def main():
    import os
    print(os.getpid(), flush=True)
    process = CrawlerProcess(
        settings={
            "REQUEST_FINGERPRINTER_IMPLEMENTATION" : "2.7",
            "FEEDS": {
                '/home/xuanlong/web_crawl/data/en_citynomads.jsonl': {
                    "format": "jsonlines",
                    "overwrite": True,
                    "encoding": "utf8",
                },
            },
            # "DOWNLOAD_DELAY" : 0.03,
            "LOG_LEVEL": "INFO",
            "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.88 Safari/537.36",
        }
    )
    process.crawl(SG_Spider)
    process.start()


if __name__ == "__main__":
    fire.Fire(main)
