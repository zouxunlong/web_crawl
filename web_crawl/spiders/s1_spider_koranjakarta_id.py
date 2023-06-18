import scrapy
from datetime import date, time, datetime, timezone, timedelta
from pathlib import Path
from scrapy.crawler import CrawlerProcess
import plac
import os
import json


class KoranJakartaIdSpider(scrapy.Spider):
    name = 'koranjakarta_id'
    allowed_domains = ['koran-jakarta.com']
    start_urls = []

    def __init__(self, start_date, end_date):
        self.start_date = datetime.combine(
            start_date, time(), timezone(timedelta(hours=8)))
        self.end_date = datetime.combine(
            end_date, time(), timezone(timedelta(hours=8)))
        this_date = start_date
        while this_date < end_date:
            this_date_string = datetime.strftime(this_date, "%d+%B+%Y")
            self.start_urls.append(f'https://koran-jakarta.com/terbaru?date={this_date_string}&page=1')
            this_date += timedelta(days=1)

    def parse(self, response):
        articles = response.xpath('//article')
        if not articles:
            return
        response_url = response.request.url
        date_chunk = response_url.split('=')[-2]
        date = datetime.strptime(date_chunk, "%d+%B+%Y&page").replace(tzinfo=timezone(timedelta(hours=8)))
        for article in articles:
            article_data = {"date": str(date.date())}
            yield scrapy.Request(
                url=article.xpath(".//@href").get(),
                callback=self.parse_article,
                meta={"data": article_data}
            )
        next_page_link = response_url[:-1] + str(1 + int(response_url[-1]))
        if next_page_link and len(next_page_link) > 1:
            yield scrapy.Request(next_page_link, callback=self.parse)


    def parse_article(self, response):
        article_data = response.meta["data"]
        article_data["text"] = ''
        texts = response.xpath('//div[contains(@class, "article-description")]/p')
        if not texts:
            return
        for text in texts:
            t = ''.join(text.xpath(".//text()").extract()).replace('\t', '') + "\n"
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
# def main(output_path='koranjakarta_id.jl', start_date=(date.today() - timedelta(1)), end_date=date.today()):
def main(output_path='koranjakarta_id.jl', start_date=datetime.strptime('02012022', "%d%m%Y").date(), end_date=datetime.strptime('05012022', "%d%m%Y").date()):

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
    process.crawl(KoranJakartaIdSpider, start_date=start_date, end_date=end_date)
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
