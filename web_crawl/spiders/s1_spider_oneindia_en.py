import scrapy
import json
import plac
import os
from datetime import date, time, datetime, timezone, timedelta
from scrapy.crawler import CrawlerProcess
from pathlib import Path

class OneIndiaEnSpider(scrapy.Spider):
    name = 'oneindia_en'
    allowed_domains = ['oneindia.com']
    start_urls = [
            'https://www.oneindia.com/india/?ref_medium=Desktop&ref_source=OI-EN&ref_campaign=menu-header',
            'https://www.oneindia.com/international/?ref_medium=Desktop&ref_source=OI-EN&ref_campaign=menu-header',
            'https://www.oneindia.com/business/?ref_medium=Desktop&ref_source=OI-EN&ref_campaign=menu-header',
            'https://www.oneindia.com/feature/?ref_medium=Desktop&ref_source=OI-EN&ref_campaign=menu-header'
    ]

    def __init__(self, start_date, end_date):
        self.start_date = datetime.combine(
            start_date, time(), timezone(timedelta(hours=8)))
        self.end_date = datetime.combine(
            end_date, time(), timezone(timedelta(hours=8)))


    def parse(self, response):
        category = response.xpath('//h1[@class="heading"]/text()').get().strip()
        articles = response.xpath('//div[@class="oi-cityblock-list"]/ul/li[@class="clearfix oilistcontainer"]')
        for article in articles:
            date = article.xpath('.//div[@class="cityblock-time oi-datetime-cat"]/text()').get()
            date_string = ' '.join(date.split()[1:-1])
            date = datetime.strptime(date_string, "%B %d, %Y, %H:%M").replace(tzinfo=timezone(timedelta(hours=5.5)))
            article_data = {"category": category, "date": str(date.date())}
            if date < self.start_date:
                return
            elif date < self.end_date:
                yield scrapy.Request(
                    url=response.urljoin(article.xpath(".//@href").get()),
                    callback=self.parse_article,
                    meta={"data": article_data}
                )
        next_page_link = response.xpath('//a[@class="oi-city-next"]/@href').get()
        if next_page_link:
            yield scrapy.Request(response.urljoin(next_page_link), callback=self.parse)


    def parse_article(self, response):
        article_data = response.meta["data"]
        article_data["text"] = ''
        texts_1 = response.xpath('//div[@class="oi-article-lt"]/p')
        # texts_2 = response.xpath('//div[@class="listicalSliderContainer"]//p')
        for text in texts_1:
            t = ''.join(text.xpath(".//text()").extract()).replace('\n', ' ') + "\n"
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
def main(output_path='oneindia_en.jl', start_date=(date.today() - timedelta(1)), end_date=date.today()):

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
            "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.88 Safari/537.36",
        }
    )
    process.crawl(OneIndiaEnSpider, start_date=start_date, end_date=end_date)
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

