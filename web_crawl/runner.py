from datetime import date, timedelta
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
from twisted.internet import reactor
from scrapy.utils.project import get_project_settings


def main():
    settings = get_project_settings()
    configure_logging(settings)
    runner = CrawlerRunner(settings)
    d = runner.crawl("en_TheMoscowTimes",
                     start_date=(date.today() - timedelta(5)),
                     end_date=date.today())
    d.addBoth(lambda _: reactor.stop())
    reactor.run()


if __name__ == "__main__":
    main()
    print("finished all", flush=True)

