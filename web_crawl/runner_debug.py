from datetime import date, timedelta
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
from twisted.internet import reactor
from scrapy.utils.project import get_project_settings


def main_runner(spider_name):
    settings = get_project_settings()
    configure_logging(settings)
    runner = CrawlerRunner(settings)
    d = runner.crawl(spider_name, start_date=date.today() - timedelta(1), end_date=date.today())
    d.addBoth(lambda _: reactor.stop())
    reactor.run()


if __name__ == "__main__":
    main_runner("th_koratdaily")
    print("finished all", flush=True)
