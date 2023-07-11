from datetime import time, datetime, timedelta
import scrapy


class id_MediaIndonedia_Spider(scrapy.Spider):
    name = 'id_mediaindonesia'
    allowed_domains = ['mediaindonesia.com']
    start_urls = ['https://mediaindonesia.com/read/terkini']
    translate_month = {
        'Januari': 'Jan',
        'Februari': 'Feb',
        'Maret': 'Mar',
        'April': 'Apr',
        'Mei': 'May',
        'Juni': 'Jun',
        'Juli': 'Jul',
        'Agustus': 'Aug',
        'September': 'Sep',
        'Oktober': 'Oct',
        'November': 'Nov',
        'Desember': 'Dec'
    }

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.start_time = datetime.combine(start_date, time())
        self.end_time = datetime.combine(end_date, time())

    def parse(self, response):
        articles = response.xpath('//div[@class="article-big"]')

        for article in articles:
            date_time_list = article.xpath(
                './/span[@class="meta"]/a[2]/text()').get().split()
            date_time_list[2] = self.translate_month[date_time_list[2]]
            date_time_str = ' '.join(date_time_list[1:4])
            date_time = datetime.strptime(date_time_str, "%d %b %Y,")

            if date_time < self.start_time:
                return
            elif date_time < self.end_time:
                date = str(date_time.date())
                url = article.xpath('.//h2/a/@href').get()
                title = article.xpath('.//h2/a/@title').get()
                yield scrapy.Request(url=url,
                                     callback=self.parse_article,
                                     cb_kwargs={"date": date, "title": title})

        next_page_link = response.xpath('//a[@rel="next"]/@href').get()
        if next_page_link:
            yield scrapy.Request(next_page_link, callback=self.parse)

    def parse_article(self, response, *args, **kwargs):
        date = kwargs["date"]
        title = kwargs["title"]
        text_nodes = response.xpath('//div[@itemprop="articleBody"]/p')
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
