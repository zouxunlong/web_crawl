from datetime import date, timedelta
from scrapy.crawler import CrawlerRunner, CrawlerProcess
from scrapy.utils.log import configure_logging
from twisted.internet import reactor
from scrapy.utils.project import get_project_settings


def main_runner():
    settings = get_project_settings()
    configure_logging(settings)
    runner = CrawlerRunner(settings)
    d = runner.crawl("en_TheMoscowTimes",
                     start_date=(date.today() - timedelta(5)),
                     end_date=date.today())
    d.addBoth(lambda _: reactor.stop())
    reactor.run()


def main_process():
    settings = get_project_settings()
    process = CrawlerProcess(settings)
    process.crawl("en_BBC", start_date=(date.today() - timedelta(5)), end_date=date.today())
    process.crawl("en_Bernama", start_date=(date.today() - timedelta(5)), end_date=date.today())
    process.crawl("en_Chinadaily", start_date=(date.today() - timedelta(5)), end_date=date.today())
    process.crawl("en_CNA", start_date=(date.today() - timedelta(5)), end_date=date.today())
    process.crawl("en_france24news", start_date=(date.today() - timedelta(5)), end_date=date.today())
    process.crawl("en_koreaherald", start_date=(date.today() - timedelta(5)), end_date=date.today())
    process.crawl("en_TheMoscowTimes", start_date=(date.today() - timedelta(5)), end_date=date.today())
    process.crawl("en_oneindia", start_date=(date.today() - timedelta(5)), end_date=date.today())
    process.crawl("en_straitstimes", start_date=(date.today() - timedelta(5)), end_date=date.today())
    process.crawl("en_techcrunch", start_date=(date.today() - timedelta(5)), end_date=date.today())
    process.crawl("en_theguardian", start_date=(date.today() - timedelta(5)), end_date=date.today())
    process.crawl("en_thenational", start_date=(date.today() - timedelta(5)), end_date=date.today())
    process.crawl("id_Bisnis", start_date=(date.today() - timedelta(5)), end_date=date.today())
    process.crawl("id_koranjakarta", start_date=(date.today() - timedelta(5)), end_date=date.today())
    process.crawl("id_mediaindonesia", start_date=(date.today() - timedelta(5)), end_date=date.today())
    process.crawl("ms_Bernama", start_date=(date.today() - timedelta(5)), end_date=date.today())
    process.crawl("ms_Brudirect", start_date=(date.today() - timedelta(5)), end_date=date.today())
    process.crawl("ta_BBC", start_date=(date.today() - timedelta(5)), end_date=date.today())
    process.crawl("ta_dinamani", start_date=(date.today() - timedelta(5)), end_date=date.today())
    process.crawl("ta_hindutamil", start_date=(date.today() - timedelta(5)), end_date=date.today())
    process.crawl("ta_oneindia", start_date=(date.today() - timedelta(5)), end_date=date.today())
    process.crawl("ta_theekkathir", start_date=(date.today() - timedelta(5)), end_date=date.today())
    process.crawl("vi_nguoiviet", start_date=(date.today() - timedelta(5)), end_date=date.today())
    process.crawl("vi_nhandan", start_date=(date.today() - timedelta(5)), end_date=date.today())
    process.crawl("vi_tuoitre", start_date=(date.today() - timedelta(5)), end_date=date.today())
    process.crawl("zh_ABC", start_date=(date.today() - timedelta(5)), end_date=date.today())
    process.crawl("zh_BBC", start_date=(date.today() - timedelta(5)), end_date=date.today())
    process.crawl("zh_CCTV", start_date=(date.today() - timedelta(5)), end_date=date.today())
    process.crawl("zh_Chinadaily", start_date=(date.today() - timedelta(5)), end_date=date.today())
    process.crawl("zh_Chinanews", start_date=(date.today() - timedelta(5)), end_date=date.today())
    process.crawl("zh_uschinapress", start_date=(date.today() - timedelta(5)), end_date=date.today())
    process.crawl("zh_voachinese", start_date=(date.today() - timedelta(5)), end_date=date.today())
    process.crawl("zh_zaobao", start_date=(date.today() - timedelta(5)), end_date=date.today())
    process.start()


if __name__ == "__main__":
    main_process()
    print("finished all", flush=True)
