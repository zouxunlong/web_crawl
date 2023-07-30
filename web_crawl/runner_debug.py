from datetime import date, timedelta
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging, logger
from twisted.internet import reactor
from scrapy.utils.project import get_project_settings


def crawl():
    settings = get_project_settings()
    settings.delete("LOG_FILE")
    configure_logging(settings)
    runner = CrawlerRunner(settings)
    runner.crawl("zh_Chinanews", start_date=date.today() - timedelta(3), end_date=date.today() - timedelta(2))
    runner.crawl("id_koranjakarta", start_date=date.today() - timedelta(3), end_date=date.today() - timedelta(2))
    runner.crawl("en_france24news", start_date=date.today() - timedelta(3), end_date=date.today() - timedelta(2))
    runner.crawl("en_thenational", start_date=date.today() - timedelta(3), end_date=date.today() - timedelta(2))
    d=runner.join()
    d.addBoth(lambda _: reactor.stop())
    reactor.run()

if __name__ == "__main__":
    crawl()
    logger.info("finished all.............................")

