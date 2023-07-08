from datetime import time, datetime, timedelta
import scrapy


class id_Bisnis_Spider(scrapy.Spider):
    name = 'id_Bisnis'
    allowed_domains = ['bisnis.com']
    start_urls = []

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.start_time = datetime.combine(start_date, time())
        self.end_time = datetime.combine(end_date, time())

        while self.start_time < self.end_time:
            self.start_urls.append(
                r'https://www.bisnis.com/index?c=0&d=' + str(self.start_time.date()))
            self.start_time += timedelta(days=1)

    def parse(self, response):

        articles = response.xpath('//ul[@class="list-news indeks-new"]/li')
        for article in articles:
            date_time_str = article.xpath(
                './div[2]/div/div/div[@class="date"]/text()').get().split('|')[0].strip()
            date_time = datetime.strptime(date_time_str, "%d %b %Y")
            date = str(date_time.date())
            title = article.xpath(".//h2//text()").get()

            yield scrapy.Request(url=article.xpath(".//@href").get(),
                                 callback=self.parse_article,
                                 cb_kwargs={"date": date, "title": title})

        next_page_link = response.xpath('//a[@rel="next"]/@href').get()
        if next_page_link and len(next_page_link) > 1:
            yield scrapy.Request(next_page_link, callback=self.parse)

    def parse_article(self, response, *args, **kwargs):

        date = kwargs["date"]
        title = kwargs["title"]
        text_nodes = response.xpath('//div[@itemprop="articleBody"]/p[position() < last()]')
        texts = [''.join(text_node.xpath(".//text()").getall()).replace(u'\xa0', " ") for text_node in text_nodes]
        text = "\n".join(texts[:])
        if text:
            yield {"date": date,
                   "title": title,
                   "text": text}

    def warn_on_generator_with_return_value_stub(spider, callable):
        pass

    scrapy.utils.misc.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
    scrapy.core.scraper.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub

