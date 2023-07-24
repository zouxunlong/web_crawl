import time
from datetime import date, timedelta, datetime
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from scrapy.utils.log import logger
from multiprocessing import Process


def main_process(start_date, end_date):
    settings = get_project_settings()
    process = CrawlerProcess(settings)
    logger.info("process----------------------------start")
    process.crawl("en_ABC", start_date=start_date, end_date=end_date)
    process.crawl("en_BBC", start_date=start_date, end_date=end_date)
    process.crawl("en_Bernama", start_date=start_date, end_date=end_date)
    process.crawl("en_Chinadaily", start_date=start_date, end_date=end_date)
    process.crawl("en_CNA", start_date=start_date, end_date=end_date)
    process.crawl("en_CNN", start_date=start_date, end_date=end_date)
    process.crawl("en_france24news", start_date=start_date, end_date=end_date)
    process.crawl("en_koreaherald", start_date=start_date, end_date=end_date)
    process.crawl("en_MoscowTimes", start_date=start_date, end_date=end_date)
    process.crawl("en_Mothership", start_date=start_date, end_date=end_date)
    process.crawl("en_oneindia", start_date=start_date, end_date=end_date)
    process.crawl("en_straitstimes", start_date=start_date, end_date=end_date)
    process.crawl("en_techcrunch", start_date=start_date, end_date=end_date)
    process.crawl("en_theguardian", start_date=start_date, end_date=end_date)
    process.crawl("en_Theindependent", start_date=start_date, end_date=end_date)
    process.crawl("en_thenational", start_date=start_date, end_date=end_date)
    process.crawl("en_Weekender", start_date=start_date, end_date=end_date)
    process.crawl("id_koranjakarta", start_date=start_date, end_date=end_date)
    process.crawl("id_mediaindonesia", start_date=start_date, end_date=end_date)
    process.crawl("ms_Bernama", start_date=start_date, end_date=end_date)
    process.crawl("ms_Brudirect", start_date=start_date, end_date=end_date)
    process.crawl("ta_BBC", start_date=start_date, end_date=end_date)
    process.crawl("ta_dinamani", start_date=start_date, end_date=end_date)
    process.crawl("ta_hindutamil", start_date=start_date, end_date=end_date)
    process.crawl("ta_oneindia", start_date=start_date, end_date=end_date)
    process.crawl("ta_Theekkathir", start_date=start_date, end_date=end_date)
    process.crawl("vi_nguoiviet", start_date=start_date, end_date=end_date)
    process.crawl("vi_nhandan", start_date=start_date, end_date=end_date)
    process.crawl("vi_tuoitre", start_date=start_date, end_date=end_date)
    process.crawl("zh_ABC", start_date=start_date, end_date=end_date)
    process.crawl("zh_CCTV", start_date=start_date, end_date=end_date)
    process.crawl("zh_Chinadaily", start_date=start_date, end_date=end_date)
    process.crawl("zh_Chinanews", start_date=start_date, end_date=end_date)
    process.crawl("zh_Newsmarket", start_date=start_date, end_date=end_date)
    process.crawl("zh_Sina", start_date=start_date, end_date=end_date)
    process.crawl("zh_Twreporter", start_date=start_date, end_date=end_date)
    process.crawl("zh_uschinapress", start_date=start_date, end_date=end_date)
    process.crawl("zh_voachinese", start_date=start_date, end_date=end_date)
    process.crawl("zh_zaobao", start_date=start_date, end_date=end_date)
    process.start()
    logger.info("process----------------------------finished")


def main():
    while True:
        if datetime.now().hour in [0] and datetime.now().minute in range(5):
            logger.info("schedule----------------------------start")
            p = Process(target=main_process(date.today() - timedelta(2), date.today() - timedelta(1)))
            p.start()
            p.join()
            logger.info("schedule----------------------------finished")
        time.sleep(300)

if __name__ == '__main__':
    print("start schedule", flush=True)
    main()
    print("finished all", flush=True)
