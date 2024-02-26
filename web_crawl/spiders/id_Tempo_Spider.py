from datetime import time, datetime, timedelta
import scrapy


class id_Tempo_Spider(scrapy.Spider):

    name = 'id_tempo'
    allowed_domains = ['tempo.co']
    custom_settings = {"DOWNLOAD_DELAY": 1.5}

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.start_time = datetime.combine(start_date, time())
        self.end_time = datetime.combine(end_date, time())
        self.start_urls = []
        self.base_url = 'https://www.tempo.co/indeks/'
        incremented_date = self.end_date-timedelta(days=1)
        while incremented_date >= self.start_date:
            day = incremented_date.strftime("%Y-%m-%d")
            self.start_urls.append(self.base_url + day)
            incremented_date -= timedelta(days=1)


    def parse(self, response):
        articles = response.xpath('//div[@class="card-box ft240 margin-bottom-sm"]/article[@class="text-card"]')

        for article in articles:
            date_time_str = response.url[-10:]
            date_time = datetime.strptime(date_time_str, "%Y-%m-%d")

            if date_time < self.start_time:
                return
            elif date_time >= self.end_time:
                continue

            date = str(date_time.date())
            url = article.xpath('./h2/a/@href').get()
            title = article.xpath('./h2/a/text()').get()

            yield scrapy.Request(url=url,
                                 callback=self.parse_article,
                                 cb_kwargs={"date": date, "title": title})


    def parse_article(self, response, *args, **kwargs):
        date = kwargs["date"]
        title = kwargs["title"]
        text_nodes = response.xpath('//div[@class="detail-konten"]/p')
        if text_nodes:
            texts=[''.join(text_node.xpath(".//text()").getall()).replace('\n', " ") for text_node in text_nodes if not text_node.xpath('.//script')]
        else:
            text_nodes = response.xpath('//div[@class="detail-konten"]')
            texts=[''.join(text_node.xpath("./text()").getall()).replace('\n', " ") for text_node in text_nodes]
        text = "\n".join([t.strip() for t in texts if t.strip()]).replace(u'\xa0', " ").replace(u'\u3000', " ")
        if text and title:
            yield {"date": date,
                   "source": self.name,
                   "title": title.strip(),
                   "text": text.strip()}

    def warn_on_generator_with_return_value_stub(spider, callable):
        pass

    scrapy.utils.misc.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
    scrapy.core.scraper.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
