from datetime import time, datetime, timedelta
import scrapy
from urllib.parse import urlparse
from urllib.parse import parse_qs


class id_Jawapos_Spider(scrapy.Spider):

    name = 'id_jawapos'
    allowed_domains = ['jawapos.com']
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
        self.start_urls = []
        self.base_url = 'https://www.jawapos.com/indeks-berita?daterange='
        incremented_date = self.end_date-timedelta(days=1)
        while incremented_date >= self.start_date:
            day = incremented_date.strftime("%d %B %Y - %d %B %Y")
            self.start_urls.append(self.base_url + day)
            incremented_date -= timedelta(days=1)


    def parse(self, response):
        articles = response.xpath('//div[@class="latest__item"]')

        for article in articles:

            url = response.url
            parsed_url = urlparse(url)
            date_time_str = parse_qs(parsed_url.query)['daterange'][0]
            date_time = datetime.strptime(date_time_str.split("-")[0], "%d %B %Y ")

            if date_time < self.start_time:
                return
            elif date_time >= self.end_time:
                continue

            date = str(date_time.date())
            url = article.xpath('.//h2/a/@href').get()
            title = article.xpath('.//h2/a/text()').get()

            yield scrapy.Request(url=url,
                                 callback=self.parse_article,
                                 cb_kwargs={"date": date, "title": title})

        next_page_link = response.xpath('//a[@class="paging__link paging__link--next"]/@href').get()
        if next_page_link:
            yield response.follow(next_page_link, callback=self.parse)


    def parse_article(self, response, *args, **kwargs):
        date = kwargs["date"]
        title = kwargs["title"]
        text_nodes = response.xpath('//article/p')
        texts = [''.join(text_node.xpath(".//text()").getall()).replace('\n', " ")
                 for text_node in text_nodes if not text_node.xpath('.//script')]
        text = "\n".join([t.strip() for t in texts if t.strip()]).replace(
            u'\xa0', " ").replace(u'\u3000', " ")
        if text and title:
            yield {"date": date,
                   "source": self.name,
                   "title": title.strip(),
                   "text": text.strip()}

    def warn_on_generator_with_return_value_stub(spider, callable):
        pass

    scrapy.utils.misc.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
    scrapy.core.scraper.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
