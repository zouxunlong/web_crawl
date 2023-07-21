import json
import scrapy
from datetime import time, datetime


class zh_USChinaPress_Spider(scrapy.Spider):
    name = 'zh_uschinapress'
    allowed_domains = ['uschinapress.com', "offshoremedia.net"]
    start_urls = [
        'https://cms.offshoremedia.net/front/list/getHotRecommended?pageNum=1&pageSize=500&blockId=681488843383377920']


    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.start_time = datetime.combine(start_date, time())
        self.end_time = datetime.combine(end_date, time())
        self.base_url_1 = 'https://cms.offshoremedia.net/front/list/getHotRecommended?pageNum='
        self.base_url_2 = '&pageSize=500&blockId=681488843383377920'
        self.page = 1

    def parse(self, response):
        json_resp = json.loads(response.body)['info']['contentList']
        pages = json.loads(response.body)['info']['pages']
        for item in json_resp:
            date_time_str = item["contentCreateTime"]
            date_time = datetime.fromtimestamp(int(str(date_time_str)[:10]))
            if date_time >= self.start_time and date_time < self.end_time:
                date = str(date_time.date())
                title = item["contentTitle"]
                url = item["contentStaticPage"]
                yield scrapy.Request(url=url,
                                     callback=self.parse_article,
                                     cb_kwargs={"date": date, "title": title})

        self.page += 1
        next_page_link = self.base_url_1 + str(self.page) + self.base_url_2
        if self.page<=pages:
            yield scrapy.Request(url=next_page_link, callback=self.parse)
    

    def parse_article(self, response, *args, **kwargs):
        date = kwargs["date"]
        title = kwargs["title"]
        text_nodes = response.xpath('//div[@class="graphicArticle"]/p[not(contains(@style, "text-align"))]')
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
