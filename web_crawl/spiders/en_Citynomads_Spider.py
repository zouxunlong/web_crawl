from datetime import time, datetime
import scrapy


class en_Citynomads_Spider(scrapy.Spider):
    name = 'en_citynomads'
    allowed_domains = ['citynomads.com']

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.start_time = datetime.combine(start_date, time())
        self.end_time = datetime.combine(end_date, time())
        self.start_urls = [
            'https://citynomads.com/food-section/fine-dining/',
            'https://citynomads.com/drinks-section/bar-hopping-in-asia/',
            'https://citynomads.com/food-section/cafes-in-singapore/',
            'https://citynomads.com/food-section/casual-dining/',
            'https://citynomads.com/food-section/just-opened-in-singapore/',
            'https://citynomads.com/culture-section/art/',
            'https://citynomads.com/culture-section/city-nomads-radio/',
            'https://citynomads.com/culture-section/culture-vault/',
            'https://citynomads.com/culture-section/design-and-decor/',
            'https://citynomads.com/culture-section/entertainment/film/',
            'https://citynomads.com/culture-section/music/',
            'https://citynomads.com/fashion-beauty/',
            'https://citynomads.com/wellness-section/holistic-healing/',
            'https://citynomads.com/wellness-section/fitness/',
            'https://citynomads.com/wellness-section/sustainable-living/',
            'https://citynomads.com/singapore/',
            'https://citynomads.com/travel-guides/travel-oceania/australia/',
            'https://citynomads.com/travel-guides/europe/',
            'https://citynomads.com/travel-guides/east-asia/hong-kong/',
            'https://citynomads.com/travel-guides/travel-southeast-asia/indonesia/',
            'https://citynomads.com/travel-guides/east-asia/japan/',
            'https://citynomads.com/travel-guides/travel-oceania/new-caledonia/',
            'https://citynomads.com/travel-guides/east-asia/south-korea/',
            'https://citynomads.com/travel-guides/travel-southeast-asia/thailand/',
            'https://citynomads.com/travel-guides/travel-southeast-asia/vietnam/',
            'https://citynomads.com/fun-things-to-do-in-singapore-this-week/',
        ]


    def parse(self, response):
        articles = response.xpath('//main//article')

        for article in articles:
            date_time_str = article.xpath('.//time[1]/text()').get()
            date_time = datetime.strptime(date_time_str, "%B %d, %Y")

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

        next_page_link = response.xpath('//a[@class="next page-numbers"]/@href').get()
        if next_page_link:
            yield scrapy.Request(next_page_link, callback=self.parse)


    def parse_article(self, response, *args, **kwargs):
        date = kwargs["date"]
        title = kwargs["title"]
        text_nodes = response.xpath('//div[@class="entry__content"]/h3 | //div[@class="entry__content"]/p')
        if text_nodes:
            texts = [''.join(text_node.xpath(".//text()").getall()).replace('\n', " ") for text_node in text_nodes if not text_node.xpath('.//script')]
        else:
            text_nodes = response.xpath('//div[@class="entry__content"]')
            texts = [''.join(text_node.xpath("./text()").getall()).replace('\n', " ") for text_node in text_nodes]
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

