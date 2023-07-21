import scrapy
from datetime import  time, datetime
import json

class en_ABC_Spider(scrapy.Spider):
    name = 'en_ABC'
    allowed_domains = ['abc.net.au']
    start_urls = ['https://www.abc.net.au/news-web/api/loader/channelrefetch?name=PaginationArticles&documentId=8498186&size=250&total=250',
                  'https://www.abc.net.au/news-web/api/loader/channelrefetch?name=PaginationArticles&documentId=10719976&size=250&total=250',
                  'https://www.abc.net.au/news-web/api/loader/channelrefetch?name=PaginationArticles&documentId=4771062&size=250&total=250',
                  'https://www.abc.net.au/news-web/api/loader/channelrefetch?name=PaginationArticles&documentId=12785632&size=250&total=250',
                  'https://www.abc.net.au/news-web/api/loader/channelrefetch?name=PaginationArticles&documentId=7940720&size=250&total=250',
                  'https://www.abc.net.au/news-web/api/loader/channelrefetch?name=PaginationArticles&documentId=7571224&size=250&total=250',
                  'https://www.abc.net.au/news-web/api/loader/channelrefetch?name=PaginationArticles&documentId=12785638&size=250&total=250',
                  'https://www.abc.net.au/news-web/api/loader/channelrefetch?name=PaginationArticles&documentId=12785658&size=250&total=250',
                  ]

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.start_time = datetime.combine(start_date, time())
        self.end_time = datetime.combine(end_date, time())

    def parse(self, response):
        
        json_resp = json.loads(response.body)
        
        for item in json_resp['collection']:
            
            date_time_str = item["timestamp"]["dates"]["firstPublished"]
            date_time = datetime.strptime(date_time_str[:10], "%Y-%m-%d")
            
            if date_time < self.start_time or date_time >= self.end_time:
                continue

            url=response.urljoin(item["link"]["to"])
            date = str(date_time.date())
            title = item["title"]["children"]

            yield scrapy.Request(
                url=url,
                callback=self.parse_article,
                cb_kwargs={"date": date, "title": title})


    def parse_article(self, response, *args, **kwargs):

        date = kwargs["date"]
        title = kwargs["title"]
        
        text_nodes = response.xpath('//div[@id="body"]/div/div/div/*[self::p or self::h2]')
        texts=[''.join(text_node.xpath(".//text()").getall()).replace('\n', " ") for text_node in text_nodes if not text_node.xpath('.//script')]
        text = "\n".join([t.strip() for t in texts if t.strip()]).replace(u'\xa0', " ").replace(u'\u3000', " ")

        if text:
            yield {"date": date,
                   "source": self.name,
                   "title": title,
                   "text": text}
