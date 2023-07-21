import scrapy
from datetime import time, datetime, timedelta


class en_KoreaHerald_Spider(scrapy.Spider):
    name = 'en_koreaherald'
    allowed_domains = ['koreaherald.com']
    start_urls = ['https://www.koreaherald.com/list.php?ct=020000000000']

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.start_time = datetime.combine(start_date, time())
        self.end_time = datetime.combine(end_date, time())
        self.base_url = 'https://www.koreaherald.com/list.php?ct=020000000000&np='
        self.page = 1

    def parse(self, response):
        articles = response.xpath('//div[@class="main_sec"]//li')
        for article in articles:
            date_time_str = articles[-1].xpath('.//span/text()').get()
            date_time = datetime.strptime(date_time_str, "%b %d, %Y")
            if date_time < self.start_time:
                return
            elif date_time >= self.end_time:
                continue

            date = str(date_time.date())
            title = article.xpath('.//div[@class="main_l_t1"]/text()').get()
            url = article.xpath('./a/@href').get()
            
            yield response.follow(url=url,
                                    callback=self.parse_article,
                                    cb_kwargs={"date": date, "title": title})

        self.page += 1
        next_page_link = self.base_url + str(self.page)
        if next_page_link:
            yield scrapy.Request(response.urljoin(next_page_link), callback=self.parse)

    def parse_article(self, response, *args, **kwargs):

        date = kwargs["date"]
        title = kwargs["title"]
        text_nodes = response.xpath('//div[@class="view_con_t"]/p')
        texts=[''.join(text_node.xpath(".//text()").getall()).replace('\n', " ") for text_node in text_nodes if not text_node.xpath('.//script')]
        text = "\n".join([t.strip() for t in texts if t.strip()]).replace(u'\xa0', " ").replace(u'\u3000', " ")
        if text:
            yield {"date": date,
                   "source": self.name,
                   "title": title,
                   "text": text}
        
    def warn_on_generator_with_return_value_stub(spider, callable):
        pass
    scrapy.utils.misc.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
    scrapy.core.scraper.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
