import scrapy
from datetime import time, datetime


class zh_ChinaDaily_Spider(scrapy.Spider):
    name = 'zh_Chinadaily'
    allowed_domains = ['chinadaily.com.cn']
    start_urls = [
        'https://cn.chinadaily.com.cn/gtx/5d63917ba31099ab995dbb29/worldnews',
        'https://cn.chinadaily.com.cn/gtx/5d63917ba31099ab995dbb29/happening',
        'https://china.chinadaily.com.cn/5bd5639ca3101a87ca8ff636',
        'https://world.chinadaily.com.cn/5bd55927a3101a87ca8ff614',
        'https://caijing.chinadaily.com.cn/stock/5f646b7fa3101e7ce97253d9',
        'https://caijing.chinadaily.com.cn/stock/5f646b7fa3101e7ce97253dc',
        'https://caijing.chinadaily.com.cn/stock/5f646b7fa3101e7ce97253d6',
        'https://caijing.chinadaily.com.cn/stock/5f646b7fa3101e7ce97253d3',
        'https://caijing.chinadaily.com.cn/5b7620c4a310030f813cf452',
        'https://tech.chinadaily.com.cn/5b8f760ea310030f813ed4c4',
        'https://tech.chinadaily.com.cn/5b7621aaa310030f813cf459',
        'https://tech.chinadaily.com.cn/5b76219ca310030f813cf458',
        'https://tech.chinadaily.com.cn/5b762186a310030f813cf457',
        'https://tech.chinadaily.com.cn/5b762218a310030f813cf45f',
        'https://cn.chinadaily.com.cn/wenlv/5b7628c6a310030f813cf493',
        'https://cn.chinadaily.com.cn/wenlv/5b7628c6a310030f813cf492',
        'https://cn.chinadaily.com.cn/wenlv/5b7628c6a310030f813cf48f',
        'https://china.chinadaily.com.cn/5bd5639ca3101a87ca8ff634',
        'https://tw.chinadaily.com.cn/5e1ea9f6a3107bb6b579a144',
        'https://tw.chinadaily.com.cn/5e1ea9f6a3107bb6b579a147',
        'https://tw.chinadaily.com.cn/5e23b3dea3107bb6b579ab65',
        'https://tw.chinadaily.com.cn/5e23b3dea3107bb6b579ab68',
        'https://tw.chinadaily.com.cn/5e1ea9f6a3107bb6b579a14a',
    ]

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.start_time = datetime.combine(start_date, time())
        self.end_time = datetime.combine(end_date, time())

    def parse(self, response):

        articles = response.xpath(
            '//div[@class="busBox1-two" or @class="busBox1-two first_box" or @class="busBox1"or @class="busBox3"]')
        for article in articles:

            url = article.xpath("(.//h3/a/@href)").get()
            date_time_str = ''.join(url.split("/")[4:6])
            date_time = datetime.strptime(date_time_str, "%Y%m%d")
            if date_time < self.start_time:
                return
            elif date_time >= self.end_time:
                continue

            date = str(date_time.date())
            title = article.xpath(".//h3/a/text()").get()
            yield scrapy.Request(url='https:' + url,
                                    callback=self.parse_article,
                                    cb_kwargs={"date": date, "title": title})

        next_page_link = response.xpath('//a[text()="Next"]/@href').get()
        if next_page_link:
            yield scrapy.Request(url='https:' + next_page_link, callback=self.parse)

    def parse_article(self, response, *args, **kwargs):
        date = kwargs["date"]
        title = kwargs["title"]

        text_nodes = response.xpath('//div[@id="Content"]/p')
        texts=[''.join(text_node.xpath(".//text()").getall()).replace('\n', " ") for text_node in text_nodes if not text_node.xpath('.//script')]
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
