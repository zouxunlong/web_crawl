import scrapy
from datetime import time, datetime
import json


class zh_BBC_Spider(scrapy.Spider):
    name = 'zh_BBC'
    allowed_domains = ['bbc.com', 'bbci.co.uk']
    url_1 = 'https://push.api.bbci.co.uk/batch?t=%2Fdata%2Fbbc-morph-lx-commentary-data-paged%2Fabout%2F1aeae0bf-b885-4e30-94b7-d98b6362b934%2FisUk%2Ffalse%2Flimit%2F10%2FnitroKey%2Flx-nitro%2FpageNumber%2F'
    url_2 = '%2FserviceName%2Fzhongwen-simp%2Fversion%2F1.5.6'
    page = 1
    start_urls = [url_1 + str(page) + url_2]
    been_relevant = False

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.start_time = datetime.combine(start_date, time())
        self.end_time = datetime.combine(end_date, time())

    def parse(self, response):
        json_resp = json.loads(response.body)
        articles = json_resp["payload"][0]["body"]["results"]
        # If it remains False the whole page, stop process.
        now_relevant = False

        for article in articles:
            date_time_str = article["dateAdded"]
            date_time = datetime.strptime(date_time_str[:19], "%Y-%m-%dT%X")

            if date_time < self.start_time:
                continue
            elif date_time < self.end_time:
                self.been_relevant = True
                now_relevant = True

                title = article["title"]
                date = str(date_time.date())

                if "url" in article:
                    yield scrapy.Request(url='https://bbc.com' + article["url"],
                                         callback=self.parse_article,
                                         cb_kwargs={"date": date,
                                                    "title": title})
                else:
                    text_children = article["body"]
                    text = ""
                    for text_obj in text_children:
                        if text_obj["name"] != "paragraph":
                            continue
                        if "text" in text_obj["children"][0]:
                            text += text_obj["children"][0]["text"] + '\n'
                    if text:
                        yield {"date": date,
                               "title": title,
                               "text": text}

        if not self.been_relevant or now_relevant:
            self.page += 1
            yield scrapy.Request(url=self.url_1 + str(self.page) + self.url_2,
                                 callback=self.parse)

    def parse_article(self, response, *args, **kwargs):
        date = kwargs["date"]
        title = kwargs["title"]
        texts = response.xpath(
            '//main[@role="main"]/div/p/text()').getall()
        text = "\n".join(texts[:-1])

        if text:
            yield {"date": date,
                   "title": title,
                   "text": text}
            
    def warn_on_generator_with_return_value_stub(spider, callable):
        pass

    scrapy.utils.misc.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
    scrapy.core.scraper.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub

