from datetime import date, datetime, time, timedelta
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging, logger
from twisted.internet import reactor
from scrapy.utils.project import get_project_settings
import os


def schedule_next_crawl(null):
    tomorrow = datetime.combine(date.today() + timedelta(1), time())
    sleep_time = (tomorrow - datetime.now()).total_seconds()
    print("{} scheduled".format(str(tomorrow)), flush=True)
    reactor.callLater(sleep_time, crawl)

def crawl():
    
    settings = get_project_settings()
    settings.set('LOG_FILE', "./log/daily/{}.log".format(str(date.today())))
    configure_logging(settings)
    print("{} start.....".format(str(date.today())), flush=True)

    runner = CrawlerRunner(settings)
    runner.crawl("en_ABC", start_date=date.today() - timedelta(2), end_date=date.today() - timedelta(1))
    runner.crawl("en_BBC", start_date=date.today() - timedelta(2), end_date=date.today() - timedelta(1))
    runner.crawl("en_Bernama", start_date=date.today() - timedelta(2), end_date=date.today() - timedelta(1))
    runner.crawl("en_Chinadaily", start_date=date.today() - timedelta(2), end_date=date.today() - timedelta(1))
    runner.crawl("en_CNA", start_date=date.today() - timedelta(2), end_date=date.today() - timedelta(1))
    runner.crawl("en_france24news", start_date=date.today() - timedelta(2), end_date=date.today() - timedelta(1))
    runner.crawl("en_koreaherald", start_date=date.today() - timedelta(2), end_date=date.today() - timedelta(1))
    runner.crawl("en_MoscowTimes", start_date=date.today() - timedelta(2), end_date=date.today() - timedelta(1))
    runner.crawl("en_Mothership", start_date=date.today() - timedelta(2), end_date=date.today() - timedelta(1))
    runner.crawl("en_straitstimes", start_date=date.today() - timedelta(2), end_date=date.today() - timedelta(1))
    runner.crawl("en_techcrunch", start_date=date.today() - timedelta(2), end_date=date.today() - timedelta(1))
    runner.crawl("en_theguardian", start_date=date.today() - timedelta(2), end_date=date.today() - timedelta(1))
    runner.crawl("en_thenational", start_date=date.today() - timedelta(2), end_date=date.today() - timedelta(1))
    runner.crawl("id_kompas", start_date=date.today() - timedelta(2), end_date=date.today() - timedelta(1))
    runner.crawl("id_koranjakarta", start_date=date.today() - timedelta(2), end_date=date.today() - timedelta(1))
    runner.crawl("id_mediaindonesia", start_date=date.today() - timedelta(2), end_date=date.today() - timedelta(1))
    runner.crawl("ms_Bernama", start_date=date.today() - timedelta(2), end_date=date.today() - timedelta(1))
    runner.crawl("ms_Brudirect", start_date=date.today() - timedelta(2), end_date=date.today() - timedelta(1))
    runner.crawl("ms_Berita", start_date=date.today() - timedelta(2), end_date=date.today() - timedelta(1))
    runner.crawl("ta_dinamani", start_date=date.today() - timedelta(2), end_date=date.today() - timedelta(1))
    runner.crawl("ta_hindutamil", start_date=date.today() - timedelta(2), end_date=date.today() - timedelta(1))
    runner.crawl("ta_oneindia", start_date=date.today() - timedelta(2), end_date=date.today() - timedelta(1))
    runner.crawl("ta_Seithi", start_date=date.today() - timedelta(2), end_date=date.today() - timedelta(1))
    runner.crawl("vi_nguoiviet", start_date=date.today() - timedelta(2), end_date=date.today() - timedelta(1))
    runner.crawl("vi_tuoitre", start_date=date.today() - timedelta(2), end_date=date.today() - timedelta(1))
    runner.crawl("zh_CCTV", start_date=date.today() - timedelta(2), end_date=date.today() - timedelta(1))
    runner.crawl("zh_Chinadaily", start_date=date.today() - timedelta(2), end_date=date.today() - timedelta(1))
    runner.crawl("zh_Chinanews", start_date=date.today() - timedelta(2), end_date=date.today() - timedelta(1))
    runner.crawl("zh_Sina", start_date=date.today() - timedelta(2), end_date=date.today() - timedelta(1))
    runner.crawl("zh_uschinapress", start_date=date.today() - timedelta(2), end_date=date.today() - timedelta(1))
    runner.crawl("zh_voachinese", start_date=date.today() - timedelta(2), end_date=date.today() - timedelta(1))
    runner.crawl("zh_zaobao", start_date=date.today() - timedelta(2), end_date=date.today() - timedelta(1))
    runner.crawl("zh_8world", start_date=date.today() - timedelta(2), end_date=date.today() - timedelta(1))
    deferred = runner.join()
    deferred.addCallback(schedule_next_crawl)
    logger.info("crawl end once.............................")
    return deferred


if __name__ == "__main__":
    print(os.getpid(), flush=True)
    crawl()
    reactor.run()
    print("finished all", flush=True)

