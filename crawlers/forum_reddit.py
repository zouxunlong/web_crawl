from datetime import date, time, datetime, timezone, timedelta
from scrapy.crawler import CrawlerProcess
from pymongo import MongoClient
from pprint import pprint
import requests
import json
import scrapy
import plac
import os

os.chdir(os.path.dirname(__file__))
MONGO_CONNECTION_STRING = "mongodb://localhost:27017/"
mongo_client = MongoClient(MONGO_CONNECTION_STRING)
auth = requests.auth.HTTPBasicAuth(
    'd2p95UEf_of2CQdBHPLKNw', 'PeoQgdvoL4VYBqyFrbYSt-1tp9Te9w')

# apply for a reddit developer account and fill in corresponding items
data = {'grant_type': 'password',
        'username': 'Livid_Building_9534',
        'password': '317899zxl'}

sub_reddits = ['/r/SingaporeRaw',
               '/r/singapore',
               '/r/askSingapore',
               '/r/NTU',
               '/r/nus',
               '/r/SGExams',
               '/r/singaporefi']

headers = {'User-Agent': 'a-score/0.0.1'}

# get api token
res = requests.post('https://www.reddit.com/api/v1/access_token',
                    auth=auth, data=data, headers=headers)
TOKEN = res.json()['access_token']


class RedditSpider(scrapy.Spider):

    name = 'reddit'

    # config api request headers with token got
    headers = {'Authorization': f"bearer {TOKEN}"}
    base_url = 'https://oauth.reddit.com'

    post_url = '/new?limit=100'
    comment_url = '/comments/article?limit=1'
    more_comment_url = '/api/morechildren?'

    def start_requests(self):

        for sub_reddit_url in sub_reddits:
            yield scrapy.Request(url=self.base_url+sub_reddit_url+self.post_url, headers=self.headers, callback=self.parse, meta={'sub_reddit_url': sub_reddit_url})

    def parse(self, response):

        responceData = json.loads(response.body)
        if responceData['data']['after'] is not None:
            url = response.url + f"&after={responceData['data']['after']}"
            yield scrapy.Request(url=url, headers=self.headers, callback=self.parse)

        for post in responceData['data']['children']:
            try:
                reddit = {
                    '_id': post['data']['name'],
                    'link_id': post['data']['name'],
                    'id': post['data']['id'],
                    'subreddit': post['data']['subreddit'],
                    'user_id': post['data']['author'],
                    'text': post['data']['title']+post['data']['selftext'],
                    'ups': post['data']['ups'],
                    'score': post['data']['score'],
                    'created_at': datetime.utcfromtimestamp(post['data']['created_utc']),
                    'url': post['data']['url']
                }

                result = mongo_client['pipeline_reddit']['posts'].update_one(
                    {"_id": reddit['_id']}, {'$setOnInsert': reddit}, upsert=True)
                if result.upserted_id != None:
                    self.crawler.stats.inc_value('item_scraped_count')
                url = self.base_url + '/r/' + \
                    reddit['subreddit']+self.comment_url + \
                    f"&article={reddit['id']}"
                yield scrapy.Request(url=url, headers=self.headers, callback=self.parse_comments, dont_filter=True, meta=response.meta)
            except Exception as e:
                print(e)
                pass

    def parse_comments(self, response):
        responceData = json.loads(response.body)
        for post in responceData[1]['data']['children']:
            if post['kind'] == 't1':
                try:
                    reddit = {
                        '_id': post['data']['name'],
                        'link_id': post['data']['link_id'],
                        'id': post['data']['id'],
                        'subreddit': post['data']['subreddit'],
                        'user_id': post['data']['author'],
                        'text': post['data']['body'],
                        'ups': post['data']['ups'],
                        'score': post['data']['score'],
                        'created_at': datetime.utcfromtimestamp(post['data']['created_utc']),
                        'url': 'https://www.reddit.com'+post['data']['permalink']
                    }

                    result = mongo_client['pipeline_reddit']['posts'].update_one(
                        {"_id": reddit['_id']}, {'$setOnInsert': reddit}, upsert=True)
                    if result.upserted_id != None:
                        self.crawler.stats.inc_value('item_scraped_count')
                    if post['data']['replies'] != '':
                        children = ",".join(
                            post['data']['replies']['data']['children'][0]['data']['children'])
                        url = self.base_url+self.more_comment_url + \
                            f"link_id={post['data']['parent_id']}&children={children}"
                        yield scrapy.Request(url=url, headers=self.headers, callback=self.parse_more_comments, dont_filter=True, meta=response.meta)
                except Exception as e:
                    print(e)
                    pass
            elif post['kind'] == 'more':
                try:
                    children = ",".join(post['data']['children'])
                    url = self.base_url+self.more_comment_url + \
                        f"link_id={post['data']['parent_id']}&children={children}"
                    yield scrapy.Request(url=url, headers=self.headers, callback=self.parse_more_comments, dont_filter=True, meta=response.meta)
                except Exception as e:
                    print(e)
                    pass

    def parse_more_comments(self, response):
        responceData = json.loads(response.body)
        for post in responceData['jquery'][10][3][0]:
            if post['kind'] == 't1':
                try:
                    reddit = {
                        '_id': post['data']['name'],
                        'link_id': post['data']['link_id'],
                        'id': post['data']['id'],
                        'subreddit': post['data']['subreddit'],
                        'user_id': post['data']['author'],
                        'text': post['data']['body'],
                        'ups': post['data']['ups'],
                        'score': post['data']['score'],
                        'created_at': datetime.utcfromtimestamp(post['data']['created_utc']),
                        'url': 'https://www.reddit.com'+post['data']['permalink']
                    }

                    result = mongo_client['pipeline_reddit']['posts'].update_one(
                        {"_id": reddit['_id']}, {'$setOnInsert': reddit}, upsert=True)
                    if result.upserted_id != None:
                        self.crawler.stats.inc_value('item_scraped_count')
                except Exception as e:
                    print(e)
                    pass
            elif post['kind'] == 'more':
                try:
                    children = ",".join(post['data']['children'])
                    url = self.base_url+self.more_comment_url + \
                        f"link_id={post['data']['parent_id']}&children={children}"
                    yield scrapy.Request(url=url, headers=self.headers, callback=self.parse_more_comments, dont_filter=True, meta=response.meta)
                except Exception as e:
                    print(e)
                    pass


def main():

    task_start_time = datetime.now(timezone(timedelta(hours=0)))

    process = CrawlerProcess(
        settings={
            "USER_AGENT": "PostmanRuntime/7.28.4",
            "LOG_LEVEL": "INFO",
        }
    )
    process.crawl(RedditSpider)
    crawler = list(process.crawlers)[0]
    process.start()

    task_end_time = datetime.now(timezone(timedelta(hours=0)))

    stats = crawler.stats.get_stats()

    meta_data = {
        "task": "t1_scraping_reddit",
        "start_time": task_start_time.strftime("%Y-%m-%d %H:%M:%S%z"),
        "end_time": task_end_time.strftime("%Y-%m-%d %H:%M:%S%z"),
        "time_duration": "{} seconds".format((task_end_time - task_start_time).seconds),
        "new_reddit_posts_get": stats['item_scraped_count'] if 'item_scraped_count' in stats.keys() else 0,
    }

    pprint(meta_data)

    with open("t1_scraping_reddit.meta", "a") as metafile:
        metafile.write(json.dumps(meta_data)+"\n")


if __name__ == "__main__":
    plac.call(main)
