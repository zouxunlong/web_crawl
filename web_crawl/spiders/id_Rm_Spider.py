from datetime import time, datetime, timedelta
import scrapy
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode


class id_Rm_Spider(scrapy.Spider):

    name = 'id_rm'
    allowed_domains = ['rm.id']

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.start_time = datetime.combine(start_date, time())
        self.end_time = datetime.combine(end_date, time())
        self.start_urls = []
        self.base_url = 'https://rm.id/menu_rmid/data_ajaxin_aja.php?jenis=data_load_index&row=0&data_per_hal=12&tanggal='
        incremented_date = self.end_date-timedelta(days=1)
        while incremented_date >= self.start_date:
            day = incremented_date.strftime("%d-%m-%Y")
            self.start_urls.append(self.base_url + day)
            incremented_date -= timedelta(days=1)

    def parse(self, response):
        articles = response.xpath('//div[@class="card-body"]')

        for article in articles:

            url = response.url
            parsed_url = urlparse(url)
            date_time_str = dict(parse_qsl(parsed_url.query))['tanggal']
            date_time = datetime.strptime(date_time_str, "%d-%m-%Y")

            if date_time < self.start_time:
                return
            elif date_time >= self.end_time:
                continue

            date = str(date_time.date())
            url = article.xpath('./a/@href').get()
            title = article.xpath('./a/h5/text()').get()

            yield scrapy.Request(url="https://rm.id/"+url,
                                 callback=self.parse_article,
                                 cb_kwargs={"date": date, "title": title})

        if len(articles) == 12:
            url = response.url
            url_parts = list(urlparse(url))
            query = dict(parse_qsl(url_parts[4]))
            query["row"] = str(int(query["row"])+12)
            url_parts[4] = urlencode(query)
            next_page_link = urlunparse(url_parts)
            yield scrapy.Request(next_page_link, callback=self.parse)

    def parse_article(self, response, *args, **kwargs):
        date = kwargs["date"]
        title = kwargs["title"]
        text_nodes = response.xpath('//div[@class="isi-berita"]/p')
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
