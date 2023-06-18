import scrapy
from datetime import date, time, datetime, timezone, timedelta
from pathlib import Path
from scrapy.crawler import CrawlerProcess
import plac
import os
import json

class ChinaNewsSpider(scrapy.Spider):
    name = 'chinanews_zh'
    allowed_domains = ['chinanews.com.cn']
    start_urls = []

    def __init__(self, start_date, end_date):
        self.start_date = datetime.combine(
            start_date, time(), timezone(timedelta(hours=8)))
        self.end_date = datetime.combine(
            end_date, time(), timezone(timedelta(hours=8)))

        date = self.start_date
        date_str = datetime.strftime(date, '%Y/%m%d')

        while date < self.end_date:
            self.start_urls.append(f'https://www.chinanews.com.cn/scroll-news/{date_str}/news.shtml')
            date += timedelta(days=1)
            date_str = datetime.strftime(date, '%Y/%m%d')

    def parse(self, response):
        response_url = response.request.url
        date_string = response_url[41:50]
        date = datetime.strptime(date_string, "%Y/%m%d").replace(tzinfo=timezone(timedelta(hours=8)))
        articles = response.xpath('//div[@class="content_list"]//li')
        for article in articles:
            category = article.xpath('./div[@class="dd_lm"]/a/text()').get()
            url = article.xpath('./div[@class="dd_bt"]/a/@href').get()
            yield scrapy.Request(
                url=response.urljoin(url),
                callback=self.parse_article,
                meta={"data": {
                    'date': str(date.date()),
                    'category': category
                }}
            )


    def parse_article(self, response):
        article_data = response.meta["data"]
        article_data["text"] = ''

        texts = response.xpath('//div[@class="left_zw"]/p')
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
def main(output_path='chinanews_zh.jl', start_date=(date.today() - timedelta(1)), end_date=date.today()):

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
            "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.88 Safari/537.36",
        }
    )
    process.crawl(ChinaNewsSpider, start_date=start_date, end_date=end_date)
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

