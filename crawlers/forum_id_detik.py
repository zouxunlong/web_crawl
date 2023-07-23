import re
import scrapy
from datetime import date, time, datetime, timezone, timedelta
from scrapy.crawler import CrawlerProcess
from pathlib import Path
import plac
import os
import json


class DetikSpider(scrapy.Spider):

    name = 'detik'

    start_urls = ['https://forum.detik.com/forum.php?dftforum', ]

    def parse(self, response):

        channels = response.xpath('//*[@class="alt1Active"]/div[1]/a')

        for channel in channels:
            channel_title = channel.xpath('.//text()').extract_first()
            channel_url = response.urljoin(channel.xpath('@href').extract_first())
            yield scrapy.Request(url=channel_url, callback=self.parse_channel_page, meta={'channel_title': channel_title})

    def parse_channel_page(self, response):

        next_tag = response.xpath('//a[@rel="next"]')
        if len(next_tag) == 2:
            next_page_url = response.urljoin(next_tag[1].xpath('@href').extract_first())
            yield scrapy.Request(url=next_page_url, callback=self.parse_channel_page, meta=response.meta)

        threads = response.xpath('.//body/div/div[6]/div[3]/div[1]/div/form[1]/table[3]/tbody[2]/tr/td[3]/div[1]/a[last()]')

        for thread in threads:

            thread_url = response.urljoin(thread.xpath('@href').extract_first())
            thread_title = thread.xpath('text()').extract_first()
            thread_id = thread.xpath('./@id').extract_first()

            response.meta['thread_url'] = thread_url
            response.meta['thread_title'] = thread_title
            response.meta['thread_id'] = thread_id
            response.meta['post_text'] = ''

            yield scrapy.Request(url=thread_url, callback=self.parse_thread, dont_filter=True, meta=response.meta)

    def parse_thread(self, response):

        posts = response.xpath("//*[contains(concat(' ',@id,' '), 'post_message_')]")

        post_text = response.meta['post_text']

        for post in posts:
            text = ''.join(post.xpath(".//text()[not(ancestor-or-self::table[@cellpadding='6' and not(@class)])]").extract())
            post_text += re.sub('\n+','\n', text)
            post_text += '\n\n---POST---\n\n'

        response.meta['post_text']=post_text
        
        next_tag = response.xpath('//a[@rel="next"]')
        if len(next_tag) == 2:
            next_page_url = response.urljoin(next_tag[1].xpath('@href').extract_first())
            yield scrapy.Request(url=next_page_url, callback=self.parse_thread, meta=response.meta)
        else:

            thread_id = response.meta['thread_id']
            thread_title = response.meta['thread_title']
            thread_url = response.meta['thread_url']

            with open('/home/xuancong/airflow/data/id_detik/'+str(thread_id)+'.txt','w', encoding='utf8') as f_out:
                f_out.write(thread_title+'\n\n')
                f_out.write(thread_url+'\n\n')
                f_out.write(post_text)


            result = {
                'thread_id': response.meta['thread_id'],
                'thread_title': response.meta['thread_title'],
                'thread_url': response.meta['thread_url'],
                'channel_title': response.meta['channel_title'],
                'post_text': post_text,
            }

            yield result


    def warn_on_generator_with_return_value_stub(spider, callable):
        pass

    scrapy.utils.misc.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
    scrapy.core.scraper.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub


def main(output_path='/home/xuancong/web_crawl/data/social_media/id_detik/id_detik.jsonl'):

    os.chdir(os.path.dirname(__file__))

    process = CrawlerProcess(
        settings={
            "FEEDS": {
                output_path: {
                    "format": "jsonlines",
                    "overwrite": True,
                    "encoding": "utf8",
                },
            },
            "LOG_LEVEL": "INFO",
            "USER_AGENT": "PostmanRuntime/7.28.4",
        }
    )

    process.crawl(DetikSpider)
    crawler = list(process.crawlers)[0]
    process.start()

    stats = crawler.stats.get_stats()

    print(stats['item_scraped_count']
          if 'item_scraped_count' in stats.keys() else 0)


if __name__ == "__main__":
    plac.call(main)
