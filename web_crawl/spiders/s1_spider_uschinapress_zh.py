import scrapy
from datetime import date, time, datetime, timezone, timedelta
from scrapy.crawler import CrawlerProcess
from pathlib import Path
import plac
import os
import json

class USChinaPressSpider(scrapy.Spider):
    name = 'uschinapress_zh'
    allowed_domains = ['uschinapress.com']
    start_urls = ['https://cms.offshoremedia.net/front/list/getHotRecommended?pageNum=1&pageSize=500&blockId=681488843383377920']
    base_url_1 = 'https://cms.offshoremedia.net/front/list/getHotRecommended?pageNum='
    base_url_2 = '&pageSize=20&blockId=681488843383377920'
    page = 1

    def __init__(self, start_date, end_date):
        self.start_date = datetime.combine(
            start_date, time(), timezone(timedelta(hours=8)))
        self.end_date = datetime.combine(
            end_date, time(), timezone(timedelta(hours=8)))

    def parse(self, response):
        json_resp = json.loads(response.body)
        for item in json_resp['info']['contentList']:
            date = item["contentCreateTime"]
            date = datetime.fromtimestamp(int(str(date)[:10])).replace(tzinfo=timezone(timedelta(hours=8)))
            if date >= self.end_date:
                continue
            if date < self.start_date:
                # if date + timedelta(days=1) < self.start_date: #Allow a little leeway in case data's chronological ordering is slightly wrong.
                #     return
                continue
            article_data = {"date": date.date()}
            yield scrapy.Request(
                url=item["contentStaticPage"],
                callback=self.parse_article,
                meta={"data": article_data}
            )
        self.page += 1
        next_page_link = self.base_url_1 + str(self.page) + self.base_url_2
        yield scrapy.Request(next_page_link, callback=self.parse)


    def parse_article(self, response):
        article_data = response.meta["data"]
        article_data["category"] = response.xpath('//div[@class="source"]/span[last()]//text()').get()
        article_data["text"] = response.xpath('//div[@class="content_title"]/text()').get() + '\n'

        texts = response.xpath('//div[@class="graphicArticle"]/p')
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
def main(output_path='uschinapress_zh.jl', start_date=(date.today() - timedelta(2)), end_date=(date.today() - timedelta(1))):

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
    
    process.crawl(USChinaPressSpider,
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
