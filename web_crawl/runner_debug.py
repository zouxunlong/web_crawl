from datetime import date, timedelta
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging, logger
from twisted.internet import reactor
from scrapy.utils.project import get_project_settings

settings = get_project_settings()
settings.delete("LOG_FILE")
configure_logging(settings)

def crawl():
    runner = CrawlerRunner(settings)
    runner.crawl("en_Theindependent", start_date=date.today() - timedelta(2), end_date=date.today() - timedelta(1))
    runner.crawl("zh_ABC", start_date=date.today() - timedelta(2), end_date=date.today() - timedelta(1))
    d=runner.join()
    d.addBoth(lambda _: reactor.stop())
    reactor.run()

if __name__ == "__main__":
    crawl()
    logger.info("finished all.............................")

