import scrapy
from datetime import date, time, datetime, timezone, timedelta
from pathlib import Path
from scrapy.crawler import CrawlerProcess
import plac
import os
import json

class ChinaDailySpider(scrapy.Spider):
    name = 'chinadaily_zh'
    allowed_domains = ['chinadaily.com.cn']
    start_urls = [
        'https://cn.chinadaily.com.cn/gtx/5d63917ba31099ab995dbb29/worldnews',
        'https://cn.chinadaily.com.cn/gtx/5d63917ba31099ab995dbb29/happening',
        'https://china.chinadaily.com.cn/5bd5639ca3101a87ca8ff636',
        'https://world.chinadaily.com.cn/5bd55927a3101a87ca8ff614'
        ]

    def __init__(self, start_date, end_date):
        self.start_date = datetime.combine(
            start_date, time(), timezone(timedelta(hours=8)))
        self.end_date = datetime.combine(
            end_date, time(), timezone(timedelta(hours=8)))


    def parse(self, response):

        articles = response.xpath('//div[contains(@class, "busBox1-two")]')
        for article in articles:
            yield scrapy.Request(
                url='https:' + article.xpath("(.//@href)").get(),
                callback=self.parse_article,
            )

        next_page_link = 'https:' + response.xpath('//a[@class="pagestyle"]/@href').get()
        yield scrapy.Request(next_page_link, callback=self.parse)


    def parse_article(self, response):

        date_string = response.xpath('//div[@class="fenx"]/div[2]/text()').get()
        date = datetime.strptime(date_string[:16], "%Y-%m-%d %H:%M").replace(tzinfo=timezone(timedelta(hours=8)))
        if date >= self.end_date:
            return
        elif date < self.start_date:
            return

        title = response.xpath('//h1[@class="dabiaoti"]/text()').get()
        article_data = {"date": str(date.date()), "text": title + '\n'}

        texts = response.xpath('//div[@id="Content"]/p')
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
def main(output_path='chinadaily_zh.jl', start_date=(date.today() - timedelta(1)), end_date=date.today()):

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
    process.crawl(ChinaDailySpider, start_date=start_date, end_date=end_date)
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

