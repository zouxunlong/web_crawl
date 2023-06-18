import scrapy
from datetime import date, time, datetime, timezone, timedelta
from scrapy.crawler import CrawlerProcess
from pathlib import Path
import plac
import os
import json


class TheNationalSpider(scrapy.Spider):
    name = 'thenational'
    allowed_domains = ['thenational.scot']
    start_urls = []

    def __init__(self, start_date, end_date):
        self.start_date = datetime.combine(
            start_date, time(), timezone(timedelta(hours=8)))
        self.end_date = datetime.combine(
            end_date, time(), timezone(timedelta(hours=8)))
        base_url = 'https://www.thenational.scot/archive/'
        incremented_date = self.start_date
        while incremented_date < self.end_date:
            day = incremented_date.strftime('%Y/%m/%d')
            self.start_urls.append(base_url + day)
            incremented_date += timedelta(days=1)

    def parse(self, response):
        day_articles = response.xpath(
            '//li[@class="archive-module-list__article-list-item"]')
        for article in day_articles:
            url = article.xpath('.//@href').get()
            yield scrapy.Request(url=response.urljoin(url), callback=self.parse_article)

    def parse_article(self, response):
        date = response.xpath('//@datetime').get()
        try:
            date = datetime.strptime(
                date, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone(timedelta(hours=8)))
        except TypeError as err:
            print(err, flush=True)
            return
        if date > self.end_date or date < self.start_date:
            return
        article_data = {"date": date.date()}
        title = response.xpath(
            '//h1[@class="mar-article__headline mar-mb-0"]/text()').get()
        article_data["text"] = title + '\n'

        texts = response.xpath('//div[@class="article-body"]//p')
        for text in texts:
            t = ''.join(text.xpath(".//text()").extract())
            article_data["text"] += t

        yield article_data

    def warn_on_generator_with_return_value_stub(spider, callable):
        pass

    scrapy.utils.misc.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
    scrapy.core.scraper.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub


@plac.pos("output_path", "output file path", type=Path)
@plac.pos("start_date", "yyyy-mm-dd, include", type=date.fromisoformat)
@plac.pos("end_date", "yyyy-mm-dd, exclude", type=date.fromisoformat)
def main(output_path='thenational.jl', start_date=(date.today() - timedelta(1)), end_date=date.today()):

    import os

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
            "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:99.0) Gecko/20100101 Firefox/99.0",
        }
    )

    process.crawl(TheNationalSpider,
                  start_date=start_date, end_date=end_date)
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
