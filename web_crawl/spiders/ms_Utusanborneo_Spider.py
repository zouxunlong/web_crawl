from datetime import time, datetime
import scrapy


class ms_Utusanborneo_Spider(scrapy.Spider):
    name = 'ms_Utusanborneo'
    allowed_domains = ['utusanborneo.com.my']
    custom_settings = {"ROBOTSTXT_OBEY": False}
    start_urls = [
        'https://www.utusanborneo.com.my/sarawak',
        'https://www.utusanborneo.com.my/sabah',
        'https://www.utusanborneo.com.my/nasional',
        'https://www.utusanborneo.com.my/dunia',
        'https://www.utusanborneo.com.my/iban',
        'https://www.utusanborneo.com.my/sukan',
        'https://www.utusanborneo.com.my/ekonomi',
        'https://www.utusanborneo.com.my/mahkamah',
        'https://www.utusanborneo.com.my/rencana',
        'https://www.utusanborneo.com.my/horizon',
        'https://www.utusanborneo.com.my/hiburan',
        'https://www.utusanborneo.com.my/pru15',
        ]

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.start_time = datetime.combine(start_date, time())
        self.end_time = datetime.combine(end_date, time())

    def parse(self, response):
        articles = response.xpath('//div[@class="panel panel-default"]')
        for article in articles:
            date_time_str = article.xpath('.//time/@datetime').get()
            date_time = datetime.strptime(date_time_str, "%Y-%m-%d")
            if date_time < self.start_time:
                return
            elif date_time >= self.end_time:
                continue

            date = str(date_time.date())
            title = article.xpath(".//h4/a/text()").get()
            url = article.xpath('.//h4/a/@href').get()

            yield response.follow(url=url,
                                 callback=self.parse_article,
                                 cb_kwargs={"date": date, "title": title})

        next_page_link = response.xpath('//a[@title="Beredar ke halaman seterusnya"]/@href').get()
        if next_page_link:
            yield response.follow(next_page_link, callback=self.parse)


    def parse_article(self, response, *args, **kwargs):
        date = kwargs["date"]
        title = kwargs["title"].replace(u'\xa0', " ")
        text_nodes = response.xpath('//div[@class="field-items"]//p')
        texts = [''.join(text_node.xpath(".//text()").getall()).replace('\n', " ") for text_node in text_nodes if not text_node.xpath('.//script')]
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
