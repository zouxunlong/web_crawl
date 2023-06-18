import scrapy
from datetime import date, time, datetime, timezone, timedelta
from scrapy.crawler import CrawlerProcess
from pathlib import Path
import plac
import os
import json

class HinduTamilSpider(scrapy.Spider):
    name = 'hindutamil'
    allowed_domains = ['hindutamil.in']
    start_urls = [
        'https://www.hindutamil.in/news/tamilnadu',
        'https://www.hindutamil.in/news/india',
        'https://www.hindutamil.in/news/world',
        'https://www.hindutamil.in/news/business',
        'https://www.hindutamil.in/news/sports',
        'https://www.hindutamil.in/news/spiritual',
        'https://www.hindutamil.in/news/social-media',
        'https://www.hindutamil.in/news/technology'
    ]

    def __init__(self, start_date, end_date):
        self.start_date = datetime.combine(
            start_date, time(), timezone(timedelta(hours=8)))
        self.end_date = datetime.combine(
            end_date, time(), timezone(timedelta(hours=8)))

    def parse(self, response):
        articles = response.xpath('//div[@class="card"]')
        category = response.xpath('//h1[@class="m-0"]/text()').get().strip()
        for article in articles:
            date = article.xpath('.//a[@class="date link-grey"]/text()').get()
            date = datetime.strptime(date, "%d %b, %Y").replace(tzinfo=timezone(timedelta(hours=5.5)))
            article_data = {"date": str(date.date()), "category": category, "text": ''}

            if date < self.start_date:
                return
            elif date < self.end_date:
                yield scrapy.Request(
                    url=article.xpath('.//@href').get(),
                    callback=self.parse_article,
                    meta={"data": article_data}
                )

        next_page_link = response.xpath('//a[@title="Next"]/@href').get()
        if next_page_link:
            yield scrapy.Request(next_page_link, callback=self.parse)


    def parse_article(self, response):
        article_data = response.meta["data"]
        title_lines = response.xpath('//h1[@class="art-title"]/font/font')
        texts = response.xpath('//div[@id="pgContentPrint"]//p')
        all_texts = title_lines + texts
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
def main(output_path='hindutamil_ta.jl', start_date=(date.today() - timedelta(1)), end_date=date.today()):

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
    process.crawl(HinduTamilSpider, start_date=start_date, end_date=end_date)
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
