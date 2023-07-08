import scrapy
from datetime import time, datetime


class en_ChinaDaily_Spider(scrapy.Spider):
    name = 'en_Chinadaily'
    allowed_domains = ['chinadaily.com.cn']
    start_urls = [
        'https://www.chinadaily.com.cn/world/asia_pacific',
        'https://www.chinadaily.com.cn/world/america',
        'https://www.chinadaily.com.cn/world/europe',
        'https://www.chinadaily.com.cn/world/middle_east',
        'https://www.chinadaily.com.cn/world/africa',
        'https://www.chinadaily.com.cn/world/china-us',
        'https://www.chinadaily.com.cn/world/cn_eu',
        'https://www.chinadaily.com.cn/world/China-Japan-Relations',
        'https://www.chinadaily.com.cn/world/china-africa',
        'https://www.chinadaily.com.cn/china/governmentandpolicy',
        'https://www.chinadaily.com.cn/china/society',
        'https://www.chinadaily.com.cn/china/scitech',
        'https://www.chinadaily.com.cn/china/59b8d010a3108c54ed7dfc30',
        'https://www.chinadaily.com.cn/china/coverstory',
        'https://www.chinadaily.com.cn/china/environment',
        'https://www.chinadaily.com.cn/china/59b8d010a3108c54ed7dfc27',
        'https://www.chinadaily.com.cn/china/59b8d010a3108c54ed7dfc25',
        'https://www.chinadaily.com.cn/business/economy',
        'https://www.chinadaily.com.cn/business/companies',
        'https://www.chinadaily.com.cn/business/biz_industries',
        'https://www.chinadaily.com.cn/business/tech',
        'https://www.chinadaily.com.cn/business/motoring',
        'https://www.chinadaily.com.cn/business/money',
        'https://www.chinadaily.com.cn/life/fashion',
        'https://www.chinadaily.com.cn/life/celebrity',
        'https://www.chinadaily.com.cn/life/people',
        'https://www.chinadaily.com.cn/food',
        'https://www.chinadaily.com.cn/life/health',
        'https://www.chinadaily.com.cn/culture/art',
        'https://www.chinadaily.com.cn/culture/musicandtheater',
        'https://www.chinadaily.com.cn/culture/filmandtv',
        'https://www.chinadaily.com.cn/culture/books',
        'https://www.chinadaily.com.cn/culture/heritage',
        'https://www.chinadaily.com.cn/culture/eventandfestival',
        'https://www.chinadaily.com.cn/culture/culturalexchange',
        'https://www.chinadaily.com.cn/travel/news',
        'https://www.chinadaily.com.cn/travel/citytours',
        'https://www.chinadaily.com.cn/travel/guidesandtips',
        'https://www.chinadaily.com.cn/travel/footprint',
        'https://www.chinadaily.com.cn/travel/aroundworld',
        'https://www.chinadaily.com.cn/travel/59b8d013a3108c54ed7dfca3',
        'https://www.chinadaily.com.cn/sports/soccer',
        'https://www.chinadaily.com.cn/sports/basketball',
        'https://www.chinadaily.com.cn/sports/volleyball',
        'https://www.chinadaily.com.cn/sports/tennis',
        'https://www.chinadaily.com.cn/sports/golf',
        'https://www.chinadaily.com.cn/sports/59b8d012a3108c54ed7dfc72',
        'https://www.chinadaily.com.cn/sports/swimming',
        'https://www.chinadaily.com.cn/opinion/editionals',
        'https://www.chinadaily.com.cn/opinion/op-ed',
        'https://www.chinadaily.com.cn/opinion/globalviews',
        'https://www.chinadaily.com.cn/opinion/commentator',
        'https://www.chinadaily.com.cn/opinion/opinionline',
    ]

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.start_time = datetime.combine(start_date, time())
        self.end_time = datetime.combine(end_date, time())

    def parse(self, response):
        articles = response.xpath(
            '//div[@class="mb10 tw3_01_2" or @class="mb10 tw3_01_2 "]')
        for article in articles:
            date_time_str = article.xpath(
                './/span[@class="tw3_01_2_t"]/b/text()').get()
            date_time = datetime.strptime(date_time_str, "%Y-%m-%d %H:%M")
            if date_time < self.start_time:
                return
            elif date_time < self.end_time:
                date = str(date_time.date())
                title = article.xpath(
                    './span[@class="tw3_01_2_t"]/h4//text()').get()
                yield response.follow(url='https:' + article.xpath("(.//@href)").get(),
                                      callback=self.parse_article,
                                      cb_kwargs={"date": date, "title": title})

        next_page_link = response.xpath('//a[text()="Next"]/@href').get()
        if next_page_link:
            yield scrapy.Request(url='https:' + next_page_link, callback=self.parse)

    def parse_article(self, response, *args, **kwargs):
        date = kwargs["date"]
        title = kwargs["title"]
        texts = response.xpath('//div[@id="Content"]/p//text()').getall()
        text = "\n".join(texts[:])
        if text:
            yield {"date": date,
                   "title": title,
                   "text": text}

    def warn_on_generator_with_return_value_stub(spider, callable):
        pass
    scrapy.utils.misc.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
    scrapy.core.scraper.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
