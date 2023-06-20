import scrapy
from datetime import time, datetime


class ta_Theekkathir_Spider(scrapy.Spider):
    name = 'ta_theekkathir'
    allowed_domains = ['theekkathir.in']
    start_urls = ['https://theekkathir.in/News/GetNewsListByCategory?CategoryName=world&PageNo=1']
    base_url = 'https://theekkathir.in/News/GetNewsListByCategory?CategoryName=world&PageNo='
    page = 1
    tamil_months = {
        "ஜனவரி": '1',
        "பிப்ரவரி": '2',
        "மார்ச்": '3',
        "ஏப்ரல்": '4',
        "மே": '5',
        "ஜூன்": '6',
        "ஜூலை": '7',
        "ஆகஸ்ட்": '8',
        "செப்டம்பர்": '9',
        "அக்டோபர்": '10',
        "நவம்பர்": '11',
        "டிசம்பர்": '12',
    }

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.start_time = datetime.combine(start_date, time())
        self.end_time = datetime.combine(end_date, time())


    def parse(self, response):
        articles = response.xpath('//article')
        for article in articles:
            date_time_ta = article.xpath('.//a[@class="zm-date"]//text()').get()
            date_list = date_time_ta.split()
            date_list[0] = self.tamil_months[date_list[0]]
            date_time_str = ' '.join(date_list)
            date_time = datetime.strptime(date_time_str, "%m %d, %Y")

            if date_time < self.start_time:
                return
            elif date_time < self.end_time:
                date = str(date_time.date())
                url = article.xpath('.//h2[@class="zm-post-title"]/a/@href').get()
                title = article.xpath('.//h2[@class="zm-post-title"]/a//text()').get()
                yield response.follow(url=url,
                                      callback=self.parse_article,
                                      cb_kwargs={"date": date, "title": title})
                

        self.page += 1
        next_page_link = self.base_url + str(self.page)
        yield scrapy.Request(url=next_page_link, callback=self.parse)


    def parse_article(self, response, *args, **kwargs):
        date = kwargs["date"]
        title = kwargs["title"]
        text_nodes = response.xpath('//div[@class="zm-post-content"]/*')
        texts=[''.join(text_node.xpath(".//text()").getall()).replace(u'\xa0', " ").replace(u'\u3000', " ") for text_node in text_nodes if not text_node.xpath('.//script')]
        text = "\n".join(texts[:])
        if text:
            yield {"date": date,
                   "title": title,
                   "text": text}





    def warn_on_generator_with_return_value_stub(spider, callable):
        pass

    scrapy.utils.misc.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
    scrapy.core.scraper.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub

