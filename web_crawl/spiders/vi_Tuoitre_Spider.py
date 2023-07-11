import scrapy
from datetime import time, datetime, timedelta


class vi_Tuoitre_Spider(scrapy.Spider):
    name = 'vi_tuoitre'
    allowed_domains = ['tuoitre.vn']
    start_urls = []

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.start_time = datetime.combine(start_date, time())
        self.end_time = datetime.combine(end_date, time())
        this_date = self.start_time
        while this_date < self.end_time:
            this_date_string = datetime.strftime(this_date, "%d-%m-%Y")
            self.start_urls += [
                f'https://tuoitre.vn/timeline-xem-theo-ngay/3/{this_date_string}/trang-1.htm',
                f'https://tuoitre.vn/timeline-xem-theo-ngay/3/{this_date_string}/trang-2.htm',
                f'https://tuoitre.vn/timeline-xem-theo-ngay/3/{this_date_string}/trang-3.htm',
                f'https://tuoitre.vn/timeline-xem-theo-ngay/3/{this_date_string}/trang-4.htm',
                f'https://tuoitre.vn/timeline-xem-theo-ngay/3/{this_date_string}/trang-5.htm'
            ]  # Usually it only goes to page 2 or 3. 5 is a reasonable buffer.
            this_date += timedelta(days=1)

    def parse(self, response):
        articles = response.xpath('//li')
        for article in articles:

            response_url = response.request.url
            date_time_str = response_url.split('/')[-2]
            date_time = datetime.strptime(date_time_str, "%d-%m-%Y")

            if date_time < self.start_time:
                return
            elif date_time < self.end_time:
                url = article.xpath("./div/h3/a/@href").get()
                date = str(date_time.date())
                title = article.xpath('./div/h3/a/@title').get()
                yield response.follow(url=url,
                                      callback=self.parse_article,
                                      cb_kwargs={"date": date, "title": title})


    def parse_article(self, response, *args, **kwargs):
        date = kwargs["date"]
        title = kwargs["title"]
        text_nodes = response.xpath('//div[@class="detail-content afcbc-body"]/*[self::p or self::h2]')
        texts=[''.join(text_node.xpath(".//text()").getall()).replace(u'\xa0', " ").replace(u'\u3000', " ") for text_node in text_nodes if not text_node.xpath('.//script')]
        text = "\n".join(texts[:])
        if text:
            yield {"date": date,
                   "source": self.name,
                   "title": title,
                   "text": text}


    def warn_on_generator_with_return_value_stub(spider, callable):
        pass

    scrapy.utils.misc.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
    scrapy.core.scraper.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
