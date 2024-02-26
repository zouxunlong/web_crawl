from datetime import time, datetime, timedelta
import scrapy


class id_Detik_Spider(scrapy.Spider):
    name = 'id_detik'
    allowed_domains = ['detik.com']

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.start_time = datetime.combine(start_date, time())
        self.end_time = datetime.combine(end_date, time())
        self.start_urls=[]
        self.base_urls = ['https://news.detik.com/indeks?date=',
                         'https://www.detik.com/edu/indeks?date=',
                         'https://finance.detik.com/indeks?date=',
                         'https://hot.detik.com/indeks?date=',
                         'https://inet.detik.com/indeks?date=',
                         'https://sport.detik.com/indeks?date=',
                         'https://oto.detik.com/indeks?date=',
                         'https://travel.detik.com/indeks?date=',
                         'https://sport.detik.com/sepakbola/indeks?date=',
                         'https://food.detik.com/indeks?date=',
                         'https://health.detik.com/indeks?date=',
                         'https://www.detik.com/jatim/indeks?date=',
                         'https://www.detik.com/jateng/indeks?date=',
                         ]
        incremented_date = self.start_date
        while incremented_date < self.end_date:
            day = incremented_date.strftime("%m/%d/%Y")
            self.start_urls.extend([base_url + day for base_url in self.base_urls])
            incremented_date += timedelta(days=1)


    def parse(self, response):
        articles = response.xpath('//article[@class="list-content__item"]')

        for article in articles:
            timestamp = article.xpath('.//div[@class="media__date"]/span/@d-time').get()
            date_time = datetime.fromtimestamp(int(timestamp))

            if date_time < self.start_time:
                return
            elif date_time >= self.end_time:
                continue

            date = str(date_time.date())
            url = article.xpath('.//h3/a[@class="media__link"]/@href').get()
            title = article.xpath('.//h3/a[@class="media__link"]/text()').get()

            yield scrapy.Request(url=url,
                                 callback=self.parse_article,
                                 cb_kwargs={"date": date, "title": title})

        next_page_link = response.xpath('//a[@class="pagination__item" and text()="Next"]/@href').get()
        if next_page_link:
            yield scrapy.Request(next_page_link, callback=self.parse)


    def parse_article(self, response, *args, **kwargs):
        date = kwargs["date"]
        title = kwargs["title"]
        text_nodes = response.xpath('//div[contains(@class, "detail__body-text")]/p')
        if text_nodes:
            texts=[''.join(text_node.xpath(".//text()").getall()).replace('\n', " ") for text_node in text_nodes if not text_node.xpath('.//script')]
        else:
            text_nodes = response.xpath('//div[contains(@class, "detail__body-text")]')
            texts=[''.join(text_node.xpath("./text()").getall()).replace('\n', " ") for text_node in text_nodes]
        text = "\n".join([t.strip() for t in texts if t.strip()]).replace(u'\xa0', " ").replace(u'\u3000', " ")
        if text and title:
            yield {"date": date,
                   "source": self.name,
                   "title": title.strip(),
                   "text": text.strip()}


    def warn_on_generator_with_return_value_stub(spider, callable):
        pass

    scrapy.utils.misc.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
    scrapy.core.scraper.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub

