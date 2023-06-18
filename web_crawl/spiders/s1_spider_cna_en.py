import scrapy
import json
import plac
import os
from datetime import date, time, datetime, timezone, timedelta
from scrapy.crawler import CrawlerProcess
from pathlib import Path


class CNASpider(scrapy.Spider):

    name = "cna"
    allowed_domains = ["www.channelnewsasia.com"]

    def __init__(self, start_date, end_date):
        self.start_date = datetime.combine(
            start_date, time(), timezone(timedelta(hours=8)))
        self.end_date = datetime.combine(
            end_date, time(), timezone(timedelta(hours=8)))

    start_urls = ["https://www.channelnewsasia.com/api/v1/infinitelisting/94f7cd75-c28b-4c0a-8d21-09c6ba3dd3fc?_format=json&viewMode=infinite_scroll_listing&page=%d" %
                  n for n in range(1, 100)]

    def parse(self, response):
        data = json.loads(response.body)

        for item in data["result"]:

            date = datetime.strptime(
                item["release_date"], "%Y-%m-%dT%H:%M:%S%z")

            if date >= self.start_date and date < self.end_date:

                article = {}
                article["date"] = str(date.date())
                article["author"] = item["author"]
                article["title"] = item["title"]
                article["absolute_url"] = item["absolute_url"]

                yield scrapy.Request(
                    url=item["absolute_url"],
                    callback=self.parse_article,
                    meta={"article": article},
                )

    def parse_article(self, response):
        article = response.meta["article"]
        article["text"] = ""
        paragraphs = response.xpath(
            '//*[@id="block-mc-cna-theme-mainpagecontent"]/article[1]/div[1]/div[4]/div[1]/section[1]//div/div/div/div/p'
        )

        for paragraph in paragraphs:
            t = "".join(paragraph.xpath(".//text()").extract()) + "\n"
            article["text"] += t

        if article["text"]:
            yield article

    def warn_on_generator_with_return_value_stub(spider, callable):
        pass
    scrapy.utils.misc.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
    scrapy.core.scraper.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub


@plac.pos("output_path", "output file path", type=Path)
@plac.pos("start_date", "yyyy-mm-dd, include", type=date.fromisoformat)
@plac.pos("end_date", "yyyy-mm-dd, exclude", type=date.fromisoformat)
def main(output_path='cna.jl', start_date=(date.today() - timedelta(1)), end_date=date.today()):

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
    process.crawl(CNASpider, start_date=start_date, end_date=end_date)
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
