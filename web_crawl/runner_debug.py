from datetime import date, timedelta
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging, logger
from twisted.internet import reactor
from scrapy.utils.project import get_project_settings


def main_runner(spider_name):
    settings = get_project_settings()
    settings.delete("LOG_FILE")
    configure_logging(settings)
    runner = CrawlerRunner(settings)
    d = runner.crawl(spider_name, start_date=date.today() - timedelta(2), end_date=date.today() - timedelta(1))
    d.addBoth(lambda _: reactor.stop())
    reactor.run()
    logger.info("finished all")


if __name__ == "__main__":
    main_runner("en_Weekender")
    print("finished all", flush=True)
