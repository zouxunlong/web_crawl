# Incremental news crawling project 

**Incremental news crawling project** is to crawl articles by daily incremental from 39 sources (17 en, 2 id, 2 ms, 5 ta, 3 vi, 10 zh):
|Language |Sources                                                   
|-------|----------------------------------------------------------|
|English|`ABC` `BBC` `Bernama` `Chinadaily` `CNA` `CNN` `france24news` `koreaherald` `MoscowTimes` `Mothership` `oneindia` `straitstimes` `techcrunch` `theguardian` `Theindependent` `thenational` `Weekender` |
|Indonesian|`koranjakarta` `mediaindonesia` |
|Malay|`Bernama` `Brudirect` |
|Tamil|`BBC` `dinamani` `hindutamil` `oneindia` `Theekkathir` |
|Vietnamese|`nguoiviet` `nhandan` `tuoitre` |
|Chinese|`ABC` `ABC` `Chinadaily` `Chinanews` `Newsmarket` `Sina` `Twreporter` `uschinapress` `voachinese` `zaobao`|


#### Project Script Files
- `/home/xuanlong/web_crawl/web_crawl`

#### Project data and log files
- crawled articles will be stored into Elasticsearch [Data pool](http://10.2.56.247:3001/DataPool) collection `news_articles_en` / `news_articles_id` / `news_articles_ms` / `news_articles_vi` / `news_articles_ta` / `news_articles_zh`, at the same time, these articles will be split into sentences and save in .jsonl format at path `/home/xuanlong/web_crawl/data/news_article/` for subsequent processing & back translation
- log files for daily crawling will be placed at `/home/xuanlong/web_crawl/data/`

#### Quick Start

```bash
python ./web_crawl/runner.py
```




# Independent crawlers 

**Independent crawlers** are one-time run crawlers, each crawler for one source:
|Language |Sources                                                   
|-------|----------------------------------------------------------|
|English|`hardwarezone` `reddit` |
|Indonesian|`detik` |
|Thai|`ch3plus` `koratdaily` `prachachat` `thansettakij` |


#### Project Script Files
- `/home/xuanlong/web_crawl/crawlers`

#### Project data and log files
- crawled articles will be split into sentences and save in .jsonl format at path `/home/xuanlong/web_crawl/data/` for subsequent processing & back translation
- log files for daily crawling will be placed at `/home/xuanlong/web_crawl/data`

#### Quick Start
for source `hardwarezone`
```bash
python ./web_crawl/crawlers/forum_en_hardwarezone.py
```
for source `reddit`
```bash
python ./web_crawl/crawlers/forum_reddit.py
```
for source `detik`
```bash
python ./web_crawl/crawlers/forum_id_detik.py
```
for source `ch3plus`
```bash
python ./web_crawl/crawlers/th_ch3plus_Spider.py
```
for source `koratdaily`
```bash
python ./web_crawl/crawlers/th_koratdaily_Spider.py
```
for source `prachachat`
```bash
python ./web_crawl/crawlers/th_prachachat_Spider.py
```
for source `thansettakij`
```bash
python ./web_crawl/crawlers/th_thansettakij_Spider.py
```


