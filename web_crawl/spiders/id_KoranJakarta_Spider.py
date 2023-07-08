from datetime import time, datetime, timedelta
import scrapy


class id_KoranJakarta_Spider(scrapy.Spider):
    name = 'id_koranjakarta'
    allowed_domains = ['koran-jakarta.com']
    start_urls = []

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.start_time = datetime.combine(start_date, time())
        self.end_time = datetime.combine(end_date, time())
        this_date = start_date
        while this_date < end_date:
            this_date_string = datetime.strftime(this_date, "%d+%B+%Y")
            self.start_urls.append(
                f'https://koran-jakarta.com/terbaru?date={this_date_string}&page=1')
            this_date += timedelta(days=1)

    def parse(self, response):
        articles = response.xpath('//article')
        if not articles:
            return
        response_url = response.request.url
        date_chunk = response_url.split('=')[-2]
        date_time = datetime.strptime(date_chunk, "%d+%B+%Y&page")
        for article in articles:
            date = str(date_time.date())
            url = article.xpath(".//@href").get()
            title = article.xpath(".//a/text()").get()

            yield scrapy.Request(url=url,
                                 callback=self.parse_article,
                                 cb_kwargs={"date": date, "title": title})
        next_page_link = response_url[:-1] + str(1 + int(response_url[-1]))
        if next_page_link and len(next_page_link) > 1:
            yield scrapy.Request(next_page_link, callback=self.parse)

    def parse_article(self, response, *args, **kwargs):

        date = kwargs["date"]
        title = kwargs["title"]
        texts = response.xpath('//div[contains(@class, "article-description")]/p//text()').getall()
        text = "\n".join(texts[:])
        if text:
            yield {"date": date,
                   "title": title,
                   "text": text}
        
    def warn_on_generator_with_return_value_stub(spider, callable):
        pass

    scrapy.utils.misc.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
    scrapy.core.scraper.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
