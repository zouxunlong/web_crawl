from datetime import time, datetime
import scrapy


class en_Bernama_Spider(scrapy.Spider):
    name = 'en_Bernama'
    allowed_domains = ['bernama.com']
    custom_settings = {"DOWNLOAD_TIMEOUT": 60}

    start_urls = [
        'https://bernama.com/en/archive.php?ge',
        'https://bernama.com/en/archive.php?bu',
        'https://bernama.com/en/archive.php?po',
        'https://bernama.com/en/archive.php?sp',
        'https://bernama.com/en/archive.php?fe',
        'https://bernama.com/en/archive.php?wn',
        'https://bernama.com/en/archive.php?mtdc',
        'https://bernama.com/en/archive.php?neurogine',
        'https://bernama.com/en/archive.php?mida',
        'https://bernama.com/en/archive.php?eaic',
        'https://bernama.com/en/archive.php?matrade',
        'https://bernama.com/en/archive.php?corporate'
    ]

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
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
            elif date_time >= self.end_time:
                continue

            date = str(date_time.date())
            title = article.xpath("./h6//text()").get()
            url = article.xpath(".//h6/a/@href").get()

            yield scrapy.Request(url=url,
                                 callback=self.parse_article,
                                 cb_kwargs={"date": date, "title": title})

        next_page_link = response.xpath(
            '//i[@class="fa fa-chevron-right"]/parent::a/@href').get()
        if next_page_link and len(next_page_link) > 1:
            yield scrapy.Request(url='https://bernama.com/en/'+next_page_link, callback=self.parse)

    def parse_article(self, response, *args, **kwargs):

        date = kwargs["date"]
        title = kwargs["title"]

        text_nodes = response.xpath(
            '//div[@class="col-12 mt-3 text-dark text-justify"]/p')
        texts = [''.join(text_node.xpath(".//text()").getall()).replace('\n', " ")
                 for text_node in text_nodes if not text_node.xpath('.//script')]
        text = "\n".join([t.strip() for t in texts if t.strip()]).replace(
            u'\xa0', " ").replace(u'\u3000', " ")

        if text:
            yield {"date": date,
                   "source": self.name,
                   "title": title,
                   "text": text}

    def warn_on_generator_with_return_value_stub(spider, callable):
        pass
    scrapy.utils.misc.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
    scrapy.core.scraper.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
