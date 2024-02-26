from datetime import time, datetime, timedelta
import scrapy


class id_Beritabuana_Spider(scrapy.Spider):
    name = 'id_beritabuana'
    allowed_domains = ['beritabuana.co']

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.start_time = datetime.combine(start_date, time())
        self.end_time = datetime.combine(end_date, time())
        self.start_urls = ["https://beritabuana.co/page/{}/".format(i) for i in range(1,3020)]


    def parse(self, response):
        articles = response.xpath('//article')

        for article in articles:
            date_time_str = article.xpath('.//time[contains(@class,"published")]/text()').get()
            date_time = datetime.strptime(date_time_str, "%d/%m/%Y")

            if date_time < self.start_time:
                return
            elif date_time >= self.end_time:
                continue

            date = str(date_time.date())
            url = article.xpath('.//h2/a/@href').get()
            title = article.xpath('.//h2/a/text()').get()

            yield scrapy.Request(url=url,
                                 callback=self.parse_article,
                                 cb_kwargs={"date": date, "title": title})


    def parse_article(self, response, *args, **kwargs):
        date = kwargs["date"]
        title = kwargs["title"]
        text_nodes = response.xpath('//div[@class="entry-content entry-content-single"]/p')
        texts = [''.join(text_node.xpath(".//text()").getall()).replace('\n', " ")
                     for text_node in text_nodes if not text_node.xpath('.//script')]

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
