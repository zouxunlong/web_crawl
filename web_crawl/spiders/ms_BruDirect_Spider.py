from datetime import time, datetime
import scrapy


class ms_BruDirect_Spider(scrapy.Spider):
    name = 'ms_Brudirect'
    allowed_domains = ['brudirect.com']
    start_urls = ['https://brudirect.com/viewall_national-bahasamelayu.php']

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.start_time = datetime.combine(start_date, time())
        self.end_time = datetime.combine(end_date, time())

    def parse(self, response):
        articles = response.xpath('//div[@class="toogle"]//td')
        for article in articles:
            date_time_str = article.xpath(
                './p[3]/text()').extract_first().strip().replace(u'\xa0', u' ')
            date_time_str = date_time_str[:-19]+date_time_str[-16:]
            date_time = datetime.strptime(date_time_str, "%B %d %Y | %H:%M %p")
            if date_time < self.start_time:
                return
            elif date_time >= self.end_time:
                continue

            date = str(date_time.date())
            title = article.xpath("./p[2]//text()").get()
            url = article.xpath('.//@href').get()

            yield response.follow(url=url,
                                  callback=self.parse_article,
                                  cb_kwargs={"date": date, "title": title})

        next_page_link = response.xpath('//a[text()="next"]/@href').get()
        if next_page_link:
            yield scrapy.Request(response.urljoin(next_page_link), callback=self.parse)

    def parse_article(self, response, *args, **kwargs):

        date = kwargs["date"]
        title = kwargs["title"]
        text_nodes = response.xpath(
            '//span[@style="text-align: justify;"]/p[not (@class="m_2")]')
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
