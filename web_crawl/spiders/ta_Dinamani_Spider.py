import scrapy
from datetime import time, datetime
import json

class ta_Dinamani_Spider(scrapy.Spider):
    name = 'ta_dinamani'
    allowed_domains = ['dinamani.com']
    start_urls = ['https://www.dinamani.com/api/v1/collections/latest-news?item-type=story&limit=100&offset=0']
    url_base = 'https://www.dinamani.com/api/v1/collections/latest-news?item-type=story&limit=100&offset='
    offset=0
    

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.start_time = datetime.combine(start_date, time())
        self.end_time = datetime.combine(end_date, time())

    def parse(self, response):

        data = json.loads(response.body)

        for article in data["items"]:

            timestamp = article["story"]["updated-at"]//1000
            date_time = datetime.fromtimestamp(timestamp)

            if date_time < self.start_time:
                return
            elif date_time >= self.end_time:
                continue

            date = str(date_time.date())
            title = article["story"]["headline"]
            url=article["story"]["url"]


            yield response.follow(url=url,
                                    callback=self.parse_article,
                                    cb_kwargs={"date": date, "title": title})

        self.offset+=100
        next_page_link = self.url_base+str(self.offset)
        if next_page_link:
            yield scrapy.Request(next_page_link, callback=self.parse)


    def parse_article(self, response, *args, **kwargs):

        date = kwargs["date"]
        title = kwargs["title"]

        text_nodes = response.xpath('//div[@data-test-id="text"]/p')
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
