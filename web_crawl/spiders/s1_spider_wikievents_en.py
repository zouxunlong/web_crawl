import scrapy
import json
import plac
import os
from datetime import date, time, datetime, timezone, timedelta
from scrapy.crawler import CrawlerProcess
from pathlib import Path


class CurrentEventsSpider(scrapy.Spider):

    name = "wikievents"
    allowed_domains = ["en.wikipedia.org"]

    def __init__(self, start_date, end_date):
        self.start_date = datetime.combine(
            start_date, time(), timezone(timedelta(hours=8)))
        self.end_date = datetime.combine(
            end_date, time(), timezone(timedelta(hours=8)))

        self.start_urls = []
        base_url = "https://en.wikipedia.org/wiki/Portal:Current_events"
        incremented_date = self.start_date

        while incremented_date <= self.end_date:
            self.start_urls.append(
                f"{base_url}/{incremented_date.strftime('%B')}_{incremented_date.year}")
            incremented_date = datetime(incremented_date.year + int(incremented_date.month / 12),
                                        ((incremented_date.month % 12) + 1), 1, tzinfo=timezone(timedelta(hours=8)))

    def parse(self, response):

        days_on_page = response.xpath(
            '//div[@class="current-events-main vevent"]')

        for day_content in days_on_page:
            article = {}
            day_date_id = day_content.xpath(".//@id").get()
            date = datetime.strptime(day_date_id, "%Y_%B_%d").astimezone(
                timezone(timedelta(hours=8)))

            article["date"] = str(date.date())
            
            if date >= self.start_date and date < self.end_date:
                text = ""
                paragraphs = day_content.xpath(".//div[2]/*")
                for paragraph in paragraphs:
                    t = "".join(paragraph.xpath(".//text()").extract()) + "\n"
                    text += t
            
                if text:
                    article["text"] = text
                    yield article


    def warn_on_generator_with_return_value_stub(spider, callable):
        pass

    scrapy.utils.misc.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
    scrapy.core.scraper.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub


@plac.pos("output_path", "output file path", type=Path)
@plac.pos("start_date", "yyyy-mm-dd, include", type=date.fromisoformat)
@plac.pos("end_date", "yyyy-mm-dd, exclude", type=date.fromisoformat)
def main(output_path='wikievents.jl', start_date=(date.today() - timedelta(1)), end_date=date.today()):

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
    
    process.crawl(CurrentEventsSpider,
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
