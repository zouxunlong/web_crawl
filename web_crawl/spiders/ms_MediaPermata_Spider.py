import scrapy
from datetime import date, time, datetime, timezone, timedelta
from pathlib import Path
from scrapy.crawler import CrawlerProcess
import plac
import os
import json


class ms_MediaPermata_Spider(scrapy.Spider):
    name = 'ms_mediapermata'
    allowed_domains = ['mediapermata.com.bn']
    start_urls = [
        'https://mediapermata.com.bn/category/nasional/',
        'https://mediapermata.com.bn/category/borneo/',
        'https://mediapermata.com.bn/category/dunia/',
        'https://mediapermata.com.bn/category/rencana/',
        'https://mediapermata.com.bn/category/asean/',
        'https://mediapermata.com.bn/category/asia-pasifik/',
        'https://mediapermata.com.bn/category/bisnes-it/',
        'https://mediapermata.com.bn/category/sukan/'
    ]

    def __init__(self, start_date, end_date):
        self.start_date = datetime.combine(
            start_date, time(), timezone(timedelta(hours=8)))
        self.end_date = datetime.combine(
            end_date, time(), timezone(timedelta(hours=8)))

    def parse(self, response):
        articles = response.xpath(
            '//div[@class="td_module_flex td_module_flex_1 td_module_wrap td-animation-stack "]')
        for article in articles:
            url = article.xpath('.//@href').get()
            date = article.xpath('.//@datetime').get()
            date = datetime.strptime(date, "%Y-%m-%dT%H:%M:%S%z")
            article_data = {"date": str(date.date())}

            if date < self.start_date:
                return
            elif date < self.end_date:
                yield scrapy.Request(url=url,
                                     callback=self.parse_article,
                                     meta={"data": article_data}
                                     )

        next_page_link = response.xpath(
            '//i[@class="td-icon-menu-right"]/parent::a/@href').get()
        if next_page_link:
            yield scrapy.Request(next_page_link, callback=self.parse)

    def parse_article(self, response):
        article_data = response.meta["data"]
        article_data["text"] = ''
        texts = response.xpath(
            '//div[@class="tdb-block-inner td-fix-index"]/p')
        # Some articles require subscription and won't show text. I don't want to include them in my data.
        # The premium boolean checks if it is premium before I yield.
        premium = True
        for text in texts:
            if not text.xpath('.//node()'):
                continue
            t = ''.join(text.xpath(".//text()").extract()) + "\n"
            article_data["text"] += t
            premium = False

        if not premium:
            yield article_data

    def warn_on_generator_with_return_value_stub(spider, callable):
        pass

    scrapy.utils.misc.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
    scrapy.core.scraper.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub


@plac.pos("output_path", "output file path", type=Path)
@plac.pos("start_date", "yyyy-mm-dd, include", type=date.fromisoformat)
@plac.pos("end_date", "yyyy-mm-dd, exclude", type=date.fromisoformat)
def main(output_path='mediapermata_ms.jl', start_date=(date.today() - timedelta(1)), end_date=date.today()):
# def main(output_path='mediapermata_ms.jl', start_date=datetime.strptime('02012022', "%d%m%Y").date(), end_date=datetime.strptime('05012022', "%d%m%Y").date()):

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
    process.crawl(ms_MediaPermata_Spider, start_date=start_date, end_date=end_date)
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
