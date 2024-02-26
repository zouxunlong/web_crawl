from datetime import time, datetime, timedelta
import scrapy


class id_Kompas_Spider(scrapy.Spider):
    name = 'id_kompas'
    allowed_domains = ['kompas.com']
    custom_settings = {"DOWNLOAD_DELAY": 1}

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.start_time = datetime.combine(start_date, time())
        self.end_time = datetime.combine(end_date, time())
        self.start_urls=[]
        self.base_url = 'https://indeks.kompas.com/?site=all&date='
        incremented_date = self.end_date-timedelta(days=1)
        while incremented_date >= self.start_date:
            day = incremented_date.strftime("%Y-%m-%d")
            self.start_urls.append(self.base_url + day)
            incremented_date -= timedelta(days=1)


    def parse(self, response):
        articles = response.xpath('//div[@class="articleItem"]')

        for article in articles:
            date_time_str = article.xpath('.//div[@class="articlePost-date"]/text()').get()
            date_time = datetime.strptime(date_time_str, "%d/%m/%Y")

            if date_time < self.start_time:
                return
            elif date_time >= self.end_time:
                continue

            date = str(date_time.date())
            url = article.xpath('./a[@class="article-link"]/@href').get()
            title = article.xpath('.//h2[@class="articleTitle"]/text()').get()

            yield scrapy.Request(url=url,
                                 callback=self.parse_article,
                                 cb_kwargs={"date": date, "title": title})

        next_page_link = response.xpath('//a[@class="paging__link paging__link--next"]/@href').get()
        if next_page_link:
            yield scrapy.Request(next_page_link, callback=self.parse)


    def parse_article(self, response, *args, **kwargs):
        date = kwargs["date"]
        title = kwargs["title"]
        text_nodes = response.xpath('//div[@class="clearfix"]/p')
        texts=[''.join(text_node.xpath(".//text()").getall()).replace('\n', " ") for text_node in text_nodes if not text_node.xpath('.//script')]
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

