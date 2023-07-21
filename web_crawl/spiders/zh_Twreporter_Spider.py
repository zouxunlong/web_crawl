import scrapy
from datetime import time, datetime
import json

class zh_Twreporter_Spider(scrapy.Spider):

    name = 'zh_Twreporter'

    allowed_domains = ['twreporter.org']
    start_urls = ['https://go-api.twreporter.org/v2/posts?limit=500&offset={}&sort=-published_date'.format(offset) for offset in range(0,4000,500)]

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.start_time = datetime.combine(start_date, time())
        self.end_time = datetime.combine(end_date, time())

    def parse(self, response):

        json_resp = json.loads(response.body)

        for item in json_resp['data']['records']:

            date_time_str = item['published_date']
            date_time = datetime.strptime(date_time_str[:-1], "%Y-%m-%dT%H:%M:%S")

            if date_time < self.start_time:
                return
            elif date_time >= self.end_time:
                continue

            url='https://www.twreporter.org/a/'+item["slug"]
            date = str(date_time.date())
            title = item['title']

            yield scrapy.Request(
                url=url,
                callback=self.parse_article,
                cb_kwargs={"date": date, "title": title})


    def parse_article(self, response, *args, **kwargs):

        date = kwargs["date"]
        title = kwargs["title"]

        text_nodes = response.xpath('//div[@class="article-page__ContentBlock-sc-1wuywdb-9 jONJYq"]//p')
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

