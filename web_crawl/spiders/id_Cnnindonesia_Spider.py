from datetime import time, datetime, timedelta
import scrapy


class id_Cnnindonesia_Spider(scrapy.Spider):

    name = 'id_cnnindonesia'
    allowed_domains = ['cnnindonesia.com']

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.start_time = datetime.combine(start_date, time())
        self.end_time = datetime.combine(end_date, time())
        self.start_urls = []
        self.base_url = 'https://www.cnnindonesia.com/indeks/?date='
        incremented_date = self.start_date
        while incremented_date < self.end_date:
            day = incremented_date.strftime("%Y/%m/%d")
            self.start_urls.append(self.base_url + day)
            incremented_date += timedelta(days=1)


    def parse(self, response):
        articles = response.xpath('//div[@class="flex flex-col gap-5"]/article[@class="flex-grow"]')

        for article in articles:
            date_time_str = response.url[-10:]
            date_time = datetime.strptime(date_time_str, "%Y/%m/%d")

            if date_time < self.start_time:
                return
            elif date_time >= self.end_time:
                continue

            date = str(date_time.date())
            url = article.xpath('./a/@href').get()
            title = article.xpath('./a/span/h2/text()').get()

            yield scrapy.Request(url=url,
                                 callback=self.parse_article,
                                 cb_kwargs={"date": date, "title": title})

        next_page_link = response.xpath('//a[@class="inline-flex items-center justify-center w-[30px] h-[30px]"]/@href').get()
        if next_page_link:
            yield scrapy.Request(next_page_link, callback=self.parse)


    def parse_article(self, response, *args, **kwargs):
        date = kwargs["date"]
        title = kwargs["title"]
        text_nodes = response.xpath('//div[contains(@class, "detail-text")]/p')
        if text_nodes:
            texts=[''.join(text_node.xpath(".//text()").getall()).replace('\n', " ") for text_node in text_nodes if not text_node.xpath('.//script')]
        else:
            text_nodes = response.xpath('//div[contains(@class, "detail-text")]')
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
