from datetime import time, datetime
import scrapy
import json


class en_BBC_Spider(scrapy.Spider):
    name = 'en_BBC'
    allowed_domains = ['bbc.com', 'bbci.co.uk']
    url_1 = 'https://push.api.bbci.co.uk/batch?t=%2Fdata%2Fbbc-morph-lx-commentary-data-paged%2Fabout%2F8467c0e0-584b-41de-9682-756b311216b5%2FisUk%2Ffalse%2Flimit%2F20%2FnitroKey%2Flx-nitro%2FpageNumber%2F'
    url_2 = '%2Fversion%2F1.5.6'
    page = 1
    start_urls = [url_1 + str(page) + url_2]
    been_relevant = False

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
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
            elif date_time >= self.end_time:
                continue

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
                            "source": self.name,
                            "title": title,
                            "text": text}

        if not self.been_relevant or now_relevant:
            self.page += 1
            yield scrapy.Request(url=self.url_1 + str(self.page) + self.url_2,
                                 callback=self.parse)

    def parse_article(self, response, *args, **kwargs):
        
        date = kwargs["date"]
        title = kwargs["title"]
        
        text_nodes = response.xpath('//div[@data-component="text-block"]')
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

