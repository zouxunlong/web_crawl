import scrapy
from datetime import time, datetime


class en_Mothership_Spider(scrapy.Spider):

    name = 'en_Mothership'

    start_urls = ['https://mothership.sg/category/news/',
                  'https://mothership.sg/category/abroad/',
                  'https://mothership.sg/tag/ms-weekend/',
                  'https://mothership.sg/category/environment/',
                  'https://mothership.sg/category/parliament/',
                  'https://mothership.sg/category/perspectives/',
                  'https://mothership.sg/category/community/history/',
                  'https://mothership.sg/tag/covid-19/',
                  'https://mothership.sg/category/lifestyle/lifestyle-news/',
                  'https://mothership.sg/category/lifestyle/hot-deals/',
                  'https://mothership.sg/category/lifestyle/trending/',
                  'https://mothership.sg/category/lifestyle/drama/',
                  'https://mothership.sg/category/lifestyle/things-to-eat/',
                  'https://mothership.sg/category/lifestyle/things-to-do/',
                  'https://mothership.sg/category/lifestyle/stories-of-us/',
                  'https://mothership.sg/category/lifestyle/celebrity/',
                  'https://mothership.sg/category/lifestyle/heartwarming/',
                  'https://mothership.sg/category/lifestyle/travel/',
                  ]

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.start_time = datetime.combine(start_date, time())
        self.end_time = datetime.combine(end_date, time())

    def parse(self, response):

        articles = response.xpath('//*[@class="ind-article "]/a')

        for article in articles:
            date_time_str = article.xpath('.//span[@class="publish-date"]/text()').get()
            date_time = datetime.strptime(date_time_str, "%B %d, %Y, %H:%M %p")
            if date_time < self.start_time:
                return
            elif date_time < self.end_time:
                date = str(date_time.date())
                title = article.xpath('.//h1/text()').get()
                url = article.xpath('./@href').get()
                yield response.follow(url=url,
                                      callback=self.parse_article,
                                      cb_kwargs={"date": date, "title": title})

    def parse_article(self, response, *args, **kwargs):

        date = kwargs["date"]
        title = kwargs["title"]
        text_nodes = response.xpath('//div[@class="content-article-wrap"]/p[position()<last()-1]')
        texts=[''.join(text_node.xpath(".//text()").getall()).replace('\n', " ") for text_node in text_nodes if not text_node.xpath('.//script')]
        text = "\n".join([t.strip() for t in texts if t.strip()]).replace(u'\xa0', " ").replace(u'\u3000', " ")
        if text:
            yield {"date": date,
                   "source": self.name,
                   "title": title,
                   "text": text}

