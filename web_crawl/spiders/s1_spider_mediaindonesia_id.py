import scrapy
from datetime import date, time, datetime, timezone, timedelta
from pathlib import Path
from scrapy.crawler import CrawlerProcess
import plac
import os
import json


class MediaIndonediaSpider(scrapy.Spider):
    name = 'mediaindonesia'
    allowed_domains = ['mediaindonesia.com']
    start_urls = ['https://mediaindonesia.com/read/terkini']
    translate_month = {
        'Januari': 'Jan',
        'Februari': 'Feb',
        'Maret': 'Mar',
        'April': 'Apr',
        'Mei': 'May',
        'Juni': 'Jun',
        'Juli': 'Jul',
        'Agustus': 'Aug',
        'September': 'Sep',
        'Oktober': 'Oct',
        'November': 'Nov',
        'Desember': 'Dec'
    }

    def __init__(self, start_date, end_date):
        self.start_date = datetime.combine(
            start_date, time(), timezone(timedelta(hours=8)))
        self.end_date = datetime.combine(
            end_date, time(), timezone(timedelta(hours=8)))

    def parse(self, response):
        articles = response.xpath('//div[@class="article-big"]')
        # if articles:
        #     check_date = articles[-1].xpath(
        #         './/span[@class="meta"]/a[2]/text()').get().split()
        #     check_date[2] = self.translate_month[check_date[2]]
        #     check_date = ' '.join(check_date[1:4])
        #     check_date = datetime.strptime(check_date, "%d %b %Y,").replace(
        #         tzinfo=timezone(timedelta(hours=8)))
        #     if check_date <= self.end_date:
        #         for article in articles:
        #             date = article.xpath(
        #                 './/span[@class="meta"]/a[2]/text()').get().split()
        #             date[2] = self.translate_month[date[2]]
        #             date = ' '.join(date[1:4])
        #             date = datetime.strptime(date, "%d %b %Y,").replace(
        #                 tzinfo=timezone(timedelta(hours=8)))

        #             if date >= self.end_date:
        #                 continue
        #             if date < self.start_date:
        #                 return

        #             article_data = {"date": str(date.date())}
        #             category = article.xpath(
        #                 './/span[@class="meta"]/a[1]//text()').get()
        #             article_data["category"] = category
        #             url=article.xpath('.//@href').get()
        #             yield scrapy.Request(
        #                 url=url,
        #                 callback=self.parse_article,
        #                 meta={"data": article_data}
        #             )
        if articles:
            check_date = articles[-1].xpath(
                './/span[@class="meta"]/a[2]/text()').get().split()
            check_date[2] = self.translate_month[check_date[2]]
            check_date = ' '.join(check_date[1:4])
            check_date = datetime.strptime(check_date, "%d %b %Y,").replace(
                tzinfo=timezone(timedelta(hours=8)))
            if check_date <= self.end_date:
                for article in articles:
                    url = article.xpath('.//@href').get()
                    date = article.xpath(
                        './/span[@class="meta"]/a[2]/text()').get().split()
                    date[2] = self.translate_month[date[2]]
                    date = ' '.join(date[1:4])
                    date = datetime.strptime(date, "%d %b %Y,").replace(
                        tzinfo=timezone(timedelta(hours=8)))

                    if date < self.start_date:
                        return
                    elif date < self.end_date:
                        article_data = {"date": str(date.date())}
                        category = article.xpath(
                            './/span[@class="meta"]/a[1]//text()').get()
                        article_data["category"] = category
                        yield scrapy.Request(url=url, callback=self.parse_article, meta={"data": article_data})
                
        next_page_link = response.xpath('//a[@rel="next"]/@href').get()
        if next_page_link:
            yield scrapy.Request(next_page_link, callback=self.parse)

    def parse_article(self, response):
        article_data = response.meta["data"]
        article_data["text"] = ''

        texts = response.xpath('//div[@itemprop="articleBody"]/p')
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
def main(output_path='mediaindonesia_id.jl', start_date=datetime.strptime('02012022', "%d%m%Y").date(), end_date=datetime.strptime('05012022', "%d%m%Y").date()):

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
    process.crawl(MediaIndonediaSpider,
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
