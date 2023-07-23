import re
from scrapy.crawler import CrawlerProcess
import scrapy
import plac


class HardwareZoneSpider(scrapy.Spider):

    name = 'hardwarezone'

    start_urls = ['https://forums.hardwarezone.com.sg/', ]

    def parse(self, response):

        channels = response.xpath('//*[@class="node-title"]/a')

        for channel in channels:
            channel_title = channel.xpath('text()').extract_first()
            channel_url = response.urljoin(
                channel.xpath('@href').extract_first())
            yield scrapy.Request(url=channel_url, callback=self.parse_channel, dont_filter=True, cb_kwargs={'channel_title': channel_title})

    def parse_channel(self, response, *args, **kwargs):

        channel_title = kwargs["channel_title"]
        next_tag = response.xpath(
            '//*[@class="pageNav-jump pageNav-jump--next"]')
        if (len(next_tag) == 2):
            next_page_url = response.urljoin(
                next_tag[1].xpath('./@href').get())
            yield scrapy.Request(url=next_page_url, callback=self.parse_channel, cb_kwargs={'channel_title': channel_title})

        threads = response.xpath('//a[@data-xf-init="preview-tooltip"]')

        for thread in threads:

            url = thread.xpath("./@href").get()
            thread_url = response.urljoin(url)
            thread_title = thread.xpath('text()').get()
            post_text = ''

            yield scrapy.Request(url=thread_url,
                                 callback=self.parse_thread,
                                 dont_filter=True,
                                 cb_kwargs={'channel_title': channel_title,
                                            'thread_title': thread_title,
                                            'thread_url': thread_url,
                                            'post_text': post_text,
                                            })

    def parse_thread(self, response, *args, **kwargs):

        channel_title = kwargs["channel_title"]
        thread_title = kwargs["thread_title"]
        thread_url = kwargs["thread_url"]
        post_text = kwargs["post_text"]

        posts = response.xpath(
            '//article[@class="message message--post js-post js-inlineModContainer  "]')

        for post in posts:
            text = ''.join([t.strip() for t in post.xpath('.//div[@class="bbWrapper"]/text()').getall()])
            post_text += re.sub('\n+', '\n', text)
            post_text += '\n\n---POST---\n\n'

        next_tag = response.xpath(
            '//a[@class="pageNav-jump pageNav-jump--next"]')
        if (len(next_tag) == 2):
            next_page_url = response.urljoin(next_tag[1].xpath('@href').get())
            yield scrapy.Request(url=next_page_url,
                                 callback=self.parse_thread,
                                 dont_filter=True,
                                 cb_kwargs={'channel_title': channel_title,
                                            'thread_title': thread_title,
                                            'thread_url': thread_url,
                                            'post_text': post_text,
                                            })

        else:
            if thread_title and thread_url and channel_title and post_text:
                result = {
                    'thread_title': thread_title,
                    'thread_url': thread_url,
                    'channel_title': channel_title,
                    'post_text': post_text,
                }
                yield result


def main(output_path='/home/xuancong/web_crawl/data/social_media/en_hardwarezone/en_hardwarezone.jsonl'):

    process = CrawlerProcess(
        settings={
            "FEEDS": {
                output_path: {
                    "format": "jsonlines",
                    "overwrite": True,
                    "encoding": "utf8",
                },
            },
            "DOWNLOADER_MIDDLEWARES": {
                "middlewares.WebCrawlDownloaderMiddleware": 543,
            },
            "LOG_LEVEL": "INFO",
        }
    )
    process.crawl(HardwareZoneSpider)
    process.start()


if __name__ == "__main__":
    plac.call(main)
