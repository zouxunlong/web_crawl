import scrapy
from datetime import date, time, datetime, timezone, timedelta
from pathlib import Path
from scrapy.crawler import CrawlerProcess
import plac
import os
import json

class TheekkathirTmSpider(scrapy.Spider):
    name = 'theekkathir_ta'
    allowed_domains = ['theekkathir.in']
    start_urls = ['https://theekkathir.in/News/GetNewsListByCategory?CategoryName=world&PageNo=1']
    base_url = 'https://theekkathir.in/News/GetNewsListByCategory?CategoryName=world&PageNo='
    page = 1
    tamil_months = {
        "ஜனவரி": '1',
        "பிப்ரவரி": '2',
        "மார்ச்": '3',
        "ஏப்ரல்": '4',
        "மே": '5',
        "ஜூன்": '6',
        "ஜூலை": '7',
        "ஆகஸ்ட்": '8',
        "செப்டம்பர்": '9',
        "அக்டோபர்": '10',
        "நவம்பர்": '11',
        "டிசம்பர்": '12',
    }

    def __init__(self, start_date, end_date):
        self.start_date = datetime.combine(
            start_date, time(), timezone(timedelta(hours=8)))
        self.end_date = datetime.combine(
            end_date, time(), timezone(timedelta(hours=8)))

    def parse(self, response):
        articles = response.xpath('//article')
        for article in articles:
            date = article.xpath('.//a[@class="zm-date"]//text()').get()
            date_list = date.split()
            date_list[0] = self.tamil_months[date_list[0]]
            date = ' '.join(date_list)
            date = datetime.strptime(date, "%m %d, %Y").replace(tzinfo=timezone(timedelta(hours=8)))
            if date >= self.end_date:
                continue
            if date < self.start_date:
                return
            category = article.xpath('.//div[@class="zm-category"]//text()').get()
            article_data = {"date": date.date(), "category": category}
            url = article.xpath('.//h2[@class="zm-post-title"]/a/@href').get()
            yield scrapy.Request(
                url=response.urljoin(url),
                callback=self.parse_article,
                meta={"data": article_data}
            )

        self.page += 1
        next_page_link = self.base_url + str(self.page)
        yield scrapy.Request(url=next_page_link, callback=self.parse)


    def parse_article(self, response):
        article_data = response.meta["data"]
        article_data["text"] = ''

        texts = response.xpath('//div[@class="zm-post-content"]/*')
        if not texts:
            texts = response.xpath('//div[@class="zm-post-content"]/*/*')
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
def main(output_path='theekkathir_ta.jl', start_date=(date.today() - timedelta(1)), end_date=date.today()):
    
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
    process.crawl(TheekkathirTmSpider, start_date=start_date, end_date=end_date)
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
