import scrapy
from datetime import date, time, datetime, timezone, timedelta
from pathlib import Path
from scrapy.crawler import CrawlerProcess
import plac
import os
import json

class DinamaniTaSpider(scrapy.Spider):
    name = 'dinamani_ta'
    allowed_domains = ['dinamani.com']
    start_urls = ['https://www.dinamani.com/latest-news']

    def __init__(self, start_date, end_date):
        self.start_date = datetime.combine(
            start_date, time(), timezone(timedelta(hours=8)))
        self.end_date = datetime.combine(
            end_date, time(), timezone(timedelta(hours=8)))

    def parse(self, response):
        articles = response.xpath('//div[@class="section-list-item "]')
        for article in articles:
            article_data = {}

            date = article.xpath('.//p[@class="post_time"]/text()').get()
            date = datetime.strptime(date, "%d-%m-%Y").replace(tzinfo=timezone(timedelta(hours=8)))
            article_data["date"] = date.date()

            if date >= self.end_date:
                continue
            if date < self.start_date:
                return
            yield scrapy.Request(
                url=article.xpath('.//@href').get(),
                callback=self.parse_article,
                meta={"data": article_data}
            )

        next_page_link = response.xpath('//a[text()=">"]/@href').get()
        if next_page_link:
            yield scrapy.Request(response.urljoin(next_page_link), callback=self.parse)


    def parse_article(self, response):
        article_data = response.meta["data"]

        article_data["text"] = ''

        texts = response.xpath('//div[@id="storyContent"]/p')
        for text in texts:
            t = ''.join(text.xpath(".//text()").extract()) + "\n"
            article_data["text"] += t

        if article_data["text"]:
            yield article_data

    def warn_on_generator_with_return_value_stub(spider, callable):
        pass

    scrapy.utils.misc.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
    scrapy.core.scraper.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub


@plac.pos("output_path", "output file path", type=Path)
@plac.pos("start_date", "yyyy-mm-dd, include", type=date.fromisoformat)
@plac.pos("end_date", "yyyy-mm-dd, exclude", type=date.fromisoformat)
def main(output_path='dinamani_ta.jl', start_date=(date.today() - timedelta(1)), end_date=date.today()):

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
    process.crawl(DinamaniTaSpider, start_date=start_date, end_date=end_date)
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
