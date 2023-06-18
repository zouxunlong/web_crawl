import scrapy
from datetime import date, time, datetime, timezone, timedelta
from scrapy.crawler import CrawlerProcess
from pathlib import Path
import plac
import os
import json

class StraitsTimesSpider(scrapy.Spider):
    name = 'straitstimes'
    allowed_domains = ['straitstimes.com']
    start_urls = [
        'https://www.straitstimes.com/singapore/latest',
        'https://www.straitstimes.com/asia/latest',
        'https://www.straitstimes.com/world/latest',
        'https://www.straitstimes.com/multimedia/latest',
        'https://www.straitstimes.com/tech/latest',
        'https://www.straitstimes.com/sport/latest',
        'https://www.straitstimes.com/business/latest',
        'https://www.straitstimes.com/life/latest',
        ]

    def __init__(self, start_date, end_date):
        self.start_date = datetime.combine(
            start_date, time(), timezone(timedelta(hours=8)))
        self.end_date = datetime.combine(
            end_date, time(), timezone(timedelta(hours=8)))

    def parse(self, response):
        category = response.xpath('//h2[@class="web-category-name latest"]/a/text()').get()
        articles = response.xpath('//div[@class="card-body"]')
        for article in articles:

            date = article.xpath('.//@data-created-timestamp').get()
            date = datetime.fromtimestamp(int(date)).replace(tzinfo=timezone(timedelta(hours=8)))

            if date < self.start_date:
                return
            elif date < self.end_date:
                yield scrapy.Request(
                    url=response.urljoin(article.xpath('.//@href').get()),
                    callback=self.parse_article,
                    meta={"data": {"date": str(date.date()), "category": category}}
                )

        next_page_link = response.xpath('//a[@title="Go to next page"]/@href').get()
        if next_page_link:
            yield scrapy.Request(response.urljoin(next_page_link), callback=self.parse)


    def parse_article(self, response):
        article_data = response.meta["data"]
        article_data["text"] = ''

        texts = response.xpath('//div[@class="clearfix text-formatted field field--name-field-paragraph-text field--type-text-long field--label-hidden field__item"]/p')
        for text in texts:
            t = ''.join(text.xpath(".//text()").extract()) + "\n"
            article_data["text"] += t

        yield article_data


    def warn_on_generator_with_return_value_stub(spider, callable):
        pass

    scrapy.utils.misc.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
    scrapy.core.scraper.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub


@plac.pos("output_path", "output file path", type=Path)
@plac.pos("start_date", "yyyy-mm-dd, include", type=date.fromisoformat)
@plac.pos("end_date", "yyyy-mm-dd, exclude", type=date.fromisoformat)
def main(output_path='straitstimes_en.jl', start_date=(date.today() - timedelta(1)), end_date=date.today()):

    os.chdir(os.path.dirname(__file__))

    process = CrawlerProcess(
        settings={
            "FEEDS": {
                output_path: {
                    "format": "jsonlines",
                    "overwrite": True,
                    "encoding": "utf8",
                },
            },
            "LOG_LEVEL": "INFO",
            "USER_AGENT": "PostmanRuntime/7.28.4",
        }
    )
    process.crawl(StraitsTimesSpider, start_date=start_date, end_date=end_date)
    crawler = list(process.crawlers)[0]
    process.start()

    stats = crawler.stats.get_stats()

    metadata = {}
    metadata['start_date'] = str(start_date)
    metadata['end_date'] = str(end_date)
    metadata['articles'] = stats['item_scraped_count'] if 'item_scraped_count' in stats.keys() else 0

    metadata_file_path = os.path.splitext(output_path)[0]+'.meta.json'

    with open(metadata_file_path, "w") as metafile:
        metafile.write(json.dumps(metadata))

if __name__ == "__main__":
    plac.call(main)
