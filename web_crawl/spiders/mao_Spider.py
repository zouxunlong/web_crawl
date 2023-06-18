from datetime import date, time, datetime, timezone, timedelta
import scrapy
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
from twisted.internet import reactor
from scrapy.utils.project import get_project_settings


class mao_Spider(scrapy.Spider):
    name = 'mao'
    allowed_domains = ['www.marxists.org']
    start_urls = ['https://www.marxists.org/chinese/maozedong/index.htm']

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.start_time = datetime.combine(
            start_date, time(), timezone(timedelta(hours=8)))
        self.end_time = datetime.combine(
            end_date, time(), timezone(timedelta(hours=8)))

    def parse(self, response):
        links = response.xpath("//blockquote/div/pre[position()<9]/a")
        for link in links:
            title = link.xpath('./text()').get()
            url = link.xpath('.//@href').get()
            yield response.follow(
                url=url,
                callback=self.parse_article,
                cb_kwargs={"title": title}
            )

    def parse_article(self, response, *args, **kwargs):
        print(response.request.headers["User-Agent"])
        title = kwargs["title"]
        texts = response.xpath('//text()').getall()
        text = "\n".join("".join(texts).split())
        item = {
            "title": title,
            "text": text
        }

        yield item


def main():
    settings = get_project_settings()
    configure_logging(settings)
    runner = CrawlerRunner(settings)
    d = runner.crawl(mao_Spider,
                     start_date=(date.today() - timedelta(1)),
                     end_date=date.today())
    d.addBoth(lambda _: reactor.stop())
    reactor.run()


if __name__ == "__main__":
    main()
    print("finished all", flush=True)
