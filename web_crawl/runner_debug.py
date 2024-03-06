from datetime import date, timedelta
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging, logger
from twisted.internet import reactor
from scrapy.utils.project import get_project_settings
import fire


def crawl(
    start_date: date = date(2014, 1, 1),
    end_date: date = date.today() - timedelta(1),
    name: str = "ms_Utusanborneo"
):

    settings = get_project_settings()
    settings.set('LOG_FILE', "./log/{}_debug.log".format(name))
    configure_logging(settings)
    logger.info("crawl start once.............................")

    runner = CrawlerRunner(settings)
    # runner.crawl("en_ABC", start_date=start_date, end_date=end_date)
    # runner.crawl("en_BBC", start_date=start_date, end_date=end_date)
    # runner.crawl("en_Bernama", start_date=start_date, end_date=end_date)
    # runner.crawl("en_Chinadaily", start_date=start_date, end_date=end_date)
    # runner.crawl("en_CNA", start_date=start_date, end_date=end_date)
    # runner.crawl("en_france24news", start_date=start_date, end_date=end_date)
    # runner.crawl("en_koreaherald", start_date=start_date, end_date=end_date)
    # runner.crawl("en_MoscowTimes", start_date=start_date, end_date=end_date)
    # runner.crawl("en_Mothership", start_date=start_date, end_date=end_date)
    # runner.crawl("en_straitstimes", start_date=start_date, end_date=end_date)
    # runner.crawl("en_techcrunch", start_date=start_date, end_date=end_date)
    # runner.crawl("en_theguardian", start_date=start_date, end_date=end_date)
    # runner.crawl("en_thenational", start_date=start_date, end_date=end_date)
    # runner.crawl("id_kompas", start_date=start_date, end_date=end_date)
    # runner.crawl("id_detik", start_date=start_date, end_date=end_date)
    # runner.crawl("id_tempo", start_date=start_date, end_date=end_date)
    runner.crawl(name, start_date=start_date, end_date=end_date)
    # runner.crawl("id_cnnindonesia", start_date=start_date, end_date=end_date)
    # runner.crawl("id_koranjakarta", start_date=start_date, end_date=end_date)
    # runner.crawl("id_mediaindonesia", start_date=start_date, end_date=end_date)
    # runner.crawl("ms_Bernama", start_date=start_date, end_date=end_date)
    # runner.crawl("ms_Brudirect", start_date=start_date, end_date=end_date)
    # runner.crawl("ms_Berita", start_date=start_date, end_date=end_date)
    # runner.crawl("ta_Seithi", start_date=start_date, end_date=end_date)
    # runner.crawl("ta_dinamani", start_date=start_date, end_date=end_date)
    # runner.crawl("ta_hindutamil", start_date=start_date, end_date=end_date)
    # runner.crawl("ta_oneindia", start_date=start_date, end_date=end_date)
    # runner.crawl("vi_nguoiviet", start_date=start_date, end_date=end_date)
    # runner.crawl("vi_nhandan", start_date=start_date, end_date=end_date)
    # runner.crawl("vi_tuoitre", start_date=start_date, end_date=end_date)
    # runner.crawl("zh_CCTV", start_date=start_date, end_date=end_date)
    # runner.crawl("zh_Chinadaily", start_date=start_date, end_date=end_date)
    # runner.crawl("zh_Chinanews", start_date=start_date, end_date=end_date)
    # runner.crawl("zh_Sina", start_date=start_date, end_date=end_date)
    # runner.crawl("zh_uschinapress", start_date=start_date, end_date=end_date)
    # runner.crawl("zh_voachinese", start_date=start_date, end_date=end_date)
    # runner.crawl("zh_zaobao", start_date=start_date, end_date=end_date)
    # runner.crawl("zh_8world", start_date=start_date, end_date=end_date)
    d = runner.join()
    d.addBoth(lambda _: reactor.stop())
    logger.info("crawl end once.............................")
    reactor.run()


if __name__ == "__main__":
    import os
    print(os.getpid(), flush=True)

    fire.Fire(crawl)

    logger.info("finished all.............................")
