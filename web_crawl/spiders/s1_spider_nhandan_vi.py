import scrapy
from datetime import date, time, datetime, timezone, timedelta
from pathlib import Path
from scrapy.crawler import CrawlerProcess
import plac
import os
import json

class NhandanSpider(scrapy.Spider):
    name = 'nhandan'
    allowed_domains = ['nhandan.vn']
    start_urls = ['https://nhandan.vn/api/morenews-latest-0-0.html?phrase=']

    def __init__(self, start_date, end_date):
        self.start_date = datetime.combine(
            start_date, time(), timezone(timedelta(hours=8)))
        self.end_date = datetime.combine(
            end_date, time(), timezone(timedelta(hours=8)))
        self.i=0

    def parse(self, response):
        articles = json.loads(response.body)['data']['articles']
        sel=scrapy.Selector(text=articles)
        selected_xpath = sel.xpath('//article')
        for article in selected_xpath:

            article_data = {}
            date = article.xpath(
                './/a[@class="text text2"]/text()')[-1].get().strip()
            date = datetime.strptime(
                date, "%d/%m/%Y %H:%M").replace(tzinfo=timezone(timedelta(hours=8)))
            article_data["date"] = str(date.date())
            
            if date >= self.end_date:
                continue
            if date < self.start_date:
                return
            url=article.xpath('./h3/a/@href').get()
            yield scrapy.Request(
                url=url,
                callback=self.parse_article,
                meta={"data": article_data}
            )
        self.i+=1
        if self.i<100:
            yield scrapy.Request('https://nhandan.vn/api/morenews-latest-0-{}.html?phrase='.format(str(self.i)), callback=self.parse)



    def parse_article(self, response):
        article_data = response.meta["data"]
        article_data["text"] = ''

        texts = response.xpath('//h1')
        texts += response.xpath('//div[@class="article__body cms-body"]/p')
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
def main(output_path='nhandan_vi.jl', start_date=(date.today() - timedelta(3)), end_date=date.today()):

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
            "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Gecko/20100101 Firefox/99.0 AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.88 Safari/537.36",
        }
    )
    process.crawl(NhandanSpider, start_date=start_date, end_date=end_date)
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
