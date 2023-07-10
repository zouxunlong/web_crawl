from datetime import date, timedelta
from scrapy.crawler import CrawlerRunner, CrawlerProcess
from scrapy.utils.log import configure_logging
from twisted.internet import reactor
from scrapy.utils.project import get_project_settings
import plac

# @plac.pos("spider_name", "spider_name", type=str)
def main_runner(spider_name="en_Chinadaily"):
    settings = get_project_settings()
    configure_logging(settings)
    runner = CrawlerRunner(settings)
    d = runner.crawl(spider_name, start_date=date.today() - timedelta(3650), end_date=date.today())
    d.addBoth(lambda _: reactor.stop())
    reactor.run()


def main_process(start_date):
    settings = get_project_settings()
    process = CrawlerProcess(settings)
    # process.crawl("en_CNA", start_date=start_date, end_date=date.today())
    # process.crawl("en_CNN", start_date=start_date, end_date=date.today())
    # process.crawl("en_BBC", start_date=start_date, end_date=date.today())
    # process.crawl("en_Chinadaily", start_date=start_date, end_date=date.today())
    # process.crawl("en_Bernama", start_date=start_date, end_date=date.today())
    # process.crawl("en_france24news", start_date=start_date, end_date=date.today())
    # process.crawl("en_koreaherald", start_date=start_date, end_date=date.today())
    # process.crawl("en_TheMoscowTimes", start_date=start_date, end_date=date.today())
    # process.crawl("en_oneindia", start_date=start_date, end_date=date.today())
    # process.crawl("en_straitstimes", start_date=start_date, end_date=date.today())
    # process.crawl("en_techcrunch", start_date=start_date, end_date=date.today())
    # process.crawl("en_theguardian", start_date=start_date, end_date=date.today())
    # process.crawl("en_thenational", start_date=start_date, end_date=date.today())
    # process.crawl("ms_Brudirect", start_date=start_date, end_date=date.today())
    # process.crawl("ta_dinamani", start_date=start_date, end_date=date.today())
    # process.crawl("ta_hindutamil", start_date=start_date, end_date=date.today())
    # process.crawl("ta_oneindia", start_date=start_date, end_date=date.today())
    # process.crawl("ta_theekkathir", start_date=start_date, end_date=date.today())
    # process.crawl("vi_nguoiviet", start_date=start_date, end_date=date.today())
    # process.crawl("vi_nhandan", start_date=start_date, end_date=date.today())
    # process.crawl("vi_tuoitre", start_date=start_date, end_date=date.today())
    # process.crawl("zh_ABC", start_date=start_date, end_date=date.today())
    # process.crawl("zh_uschinapress", start_date=start_date, end_date=date.today())
    # process.crawl("zh_BBC", start_date=start_date, end_date=date.today())
    # process.crawl("zh_voachinese", start_date=start_date, end_date=date.today())
    # process.crawl("zh_zaobao", start_date=start_date, end_date=date.today())

# AUTOTHROTTLE_ENABLED = True

    # process.crawl("zh_CCTV", start_date=start_date, end_date=date.today())
    # process.crawl("zh_Chinadaily", start_date=start_date, end_date=date.today())
    # process.crawl("zh_Chinanews", start_date=start_date, end_date=date.today())
    # process.crawl("id_Bisnis", start_date=start_date, end_date=date.today())
    # process.crawl("id_koranjakarta", start_date=start_date, end_date=date.today())
    # process.crawl("id_mediaindonesia", start_date=start_date, end_date=date.today())
    # process.crawl("ms_Bernama", start_date=start_date, end_date=date.today())
    # process.crawl("ta_BBC", start_date=start_date, end_date=date.today())

    process.start()


if __name__ == "__main__":
    # plac.call(main_runner)
    main_runner()
    print("finished all", flush=True)
