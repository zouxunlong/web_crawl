from datetime import time, datetime
import scrapy


class ms_Bernama_Spider(scrapy.Spider):
    name = 'ms_Bernama'
    allowed_domains = ['bernama.com']
    start_urls = [
        'https://bernama.com/bm/arkib.php?am',
        'https://bernama.com/bm/arkib.php?ek',
        'https://bernama.com/bm/arkib.php?pb',
        'https://bernama.com/bm/arkib.php?su',
        'https://bernama.com/bm/arkib.php?re',
        'https://bernama.com/bm/arkib.php?du',
        'https://bernama.com/bm/arkib.php?mtdc',
        'https://bernama.com/bm/arkib.php?neurogine',
        'https://bernama.com/bm/arkib.php?mida',
        'https://bernama.com/bm/arkib.php?eaic',
        'https://bernama.com/bm/arkib.php?matrade',
        'https://bernama.com/bm/arkib.php?corporate'
    ]

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.start_time = datetime.combine(start_date, time())
        self.end_time = datetime.combine(end_date, time())


    def parse(self, response):

        articles = response.xpath(
            '//div[@class="col-12 col-sm-12 col-md-8 col-lg-8 mt-4"]/div/div[@class="col-7 col-md-7 col-lg-7 mt-1"]')
        for article in articles:

            date_time_str = article.xpath('.//div/text()').get()
            date_time = datetime.strptime(date_time_str, "%d/%m/%Y %H:%M %p")

            if date_time < self.start_time:
                return
            elif date_time < self.end_time:
                date = str(date_time.date())
                title = article.xpath("./h6//text()").get()
                yield scrapy.Request(url=article.xpath(".//h6/a/@href").get(),
                                     callback=self.parse_article,
                                     cb_kwargs={"date": date,
                                                "title": title})

        next_page_link = response.xpath(
            '//i[@class="fa fa-chevron-right"]/parent::a/@href').get()
        if next_page_link and len(next_page_link) > 1:
            yield scrapy.Request(url='https://bernama.com/bm/'+next_page_link, callback=self.parse)

    def parse_article(self, response, *args, **kwargs):
        
        date = kwargs["date"]
        title = kwargs["title"]
        texts = response.xpath(
            '//div[@class="col-12 mt-3 text-dark text-justify"]/p/text()').getall()
        texts=[text.replace(u'\xa0', " ") for text in texts]
        text = "\n".join(texts)
        if text:
            yield {"date": date,
                   "title": title,
                   "text": text}


    def warn_on_generator_with_return_value_stub(spider, callable):
        pass
    scrapy.utils.misc.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
    scrapy.core.scraper.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub

