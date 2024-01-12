import json
import scrapy
from datetime import date, time, datetime, timedelta
from scrapy.crawler import CrawlerProcess


class ta_Seithi_Spider(scrapy.Spider):

    name = "ta_Seithi"
    allowed_domains = ["seithi.mediacorp.sg"]
    start_urls_0 = ["https://seithi.mediacorp.sg/api/v1/infinitelisting/786f6469-20ac-4ee4-a352-97e12efb9a39?_format=json&viewMode=infinite_scroll_listing&page=%d" %
                    n for n in range(2100)]
    start_urls_1 = ["https://seithi.mediacorp.sg/api/v1/infinitelisting/f48cc020-d24a-402c-956e-4ed5d5dbc703?_format=json&viewMode=infinite_scroll_listing&page=%d" %
                    n for n in range(1950)]
    start_urls_2 = ["https://seithi.mediacorp.sg/api/v1/infinitelisting/4eca4b28-b30a-4a69-886b-fe69d1110e70?_format=json&viewMode=infinite_scroll_listing&page=%d" %
                    n for n in range(4100)]
    start_urls = start_urls_0+start_urls_1+start_urls_2

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.start_time = datetime.combine(start_date, time())
        self.end_time = datetime.combine(end_date, time())

    def parse(self, response):

        data = json.loads(response.body)

        for article in data["result"]:

            date_time = datetime.strptime(
                article["release_date"], "%Y-%m-%dT%H:%M:%S%z").replace(tzinfo=None)

            if date_time < self.start_time:
                return
            elif date_time >= self.end_time:
                continue

            date = str(date_time.date())
            title = article["title"]
            url=article["absolute_url"]

            yield response.follow(url=url,
                                  callback=self.parse_article,
                                  cb_kwargs={"date": date, "title": title})


    def parse_article(self, response, *args, **kwargs):

        date = kwargs["date"]
        title = kwargs["title"]

        text_nodes = response.xpath('//div[@class="content-wrapper"]//div[@class="text-long"]')
        text_nodes_more = response.xpath('//div[@class="content-wrapper"]//div[@class="text-long"]/p')
        texts = ['\n'.join(text_node.xpath("./text()").getall()) for text_node in text_nodes if not text_node.xpath('.//script')]
        texts_more = [''.join(text_node.xpath("./text()").getall()).replace('\n', " ")
                 for text_node in text_nodes_more if not text_node.xpath('.//script')]
        text = "\n".join([t.strip() for t in texts if t.strip()]).replace(
            u'\xa0', " ").replace(u'\u3000', " ")
        text_more = "\n".join([t.strip() for t in texts_more if t.strip()]).replace(
            u'\xa0', " ").replace(u'\u3000', " ")
        
        text=text+text_more

        if text and title:
            yield {"date": date,
                   "source": self.name,
                   "title": title.strip(),
                   "text": text.strip()}

    def warn_on_generator_with_return_value_stub(spider, callable):
        pass

    scrapy.utils.misc.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
    scrapy.core.scraper.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub

