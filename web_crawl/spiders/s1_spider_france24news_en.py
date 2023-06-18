import scrapy
from datetime import date, time, datetime, timezone, timedelta
from scrapy.crawler import CrawlerProcess
from pathlib import Path
import plac
import os
import json

class France24NewsSpider(scrapy.Spider):
    name = 'france24news'
    allowed_domains = ['france24.com']
    start_urls = []

    def __init__(self, start_date, end_date):
        self.start_date = datetime.combine(
            start_date, time(), timezone(timedelta(hours=8)))
        self.end_date = datetime.combine(
            end_date, time(), timezone(timedelta(hours=8)))

        base_url = 'https://france24.com/en/archives/'
        incremented_date = self.start_date
        while incremented_date < self.end_date:
            day = incremented_date.strftime("%Y/%m/%d-%B-%Y")
            self.start_urls.append(base_url + day)
            incremented_date += timedelta(days=1)


    def parse(self, response):

        day_articles = response.xpath('//li[@class="o-archive-day__list__entry"]/a/@href').getall()

        for url in day_articles:
            yield scrapy.Request(url='https://france24.com' + url, callback=self.parse_article)


    def parse_article(self, response):

        date = response.xpath('//time/@datetime').get()
        article_data = {"date": date[:10]}

        article_data["text"] = ''

        texts = response.xpath('//*[@class="t-content__body u-clearfix"]/p')

        for text in texts:
            t = ''.join(text.xpath(".//text()").extract()) + "\n"
            t = t.replace("Take international news everywhere with you! Download the France 24 app", '').replace("Advertising", '')
            t = t.replace("Daily newsletter", '').replace("Receive essential international news every morning", '')
            article_data["text"] += t
        
        if article_data['text']:
            yield article_data


    def warn_on_generator_with_return_value_stub(spider, callable):
        pass

    scrapy.utils.misc.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
    scrapy.core.scraper.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub


@plac.pos("output_path", "output file path", type=Path)
@plac.pos("start_date", "yyyy-mm-dd, include", type=date.fromisoformat)
@plac.pos("end_date", "yyyy-mm-dd, exclude", type=date.fromisoformat)
def main(output_path='france24news.jl', start_date=(date.today() - timedelta(1)), end_date=date.today()):

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
    process.crawl(France24NewsSpider, start_date=start_date, end_date=end_date)
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
