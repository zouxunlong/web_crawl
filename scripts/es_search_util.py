import json
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan

ES_CONNECTION_STRING = "http://10.2.56.247:9200"
es = Elasticsearch(hosts=ES_CONNECTION_STRING, http_auth=('viewer', '1234abcd')).options(ignore_status=400, request_timeout=30)


def get_all_data(context_variables):

    selected_data_pool = context_variables['selected_data_pool']
    selected_language_types = context_variables['selected_language_types']
    selected_sources = context_variables['selected_sources']
    selected_domains = context_variables['selected_domains']
    selected_projects = context_variables['selected_projects']
    selected_custom_tags = context_variables['selected_custom_tags']
    selected_last_commit_bys = context_variables['selected_last_commit_bys']
    query = context_variables['query']

    filter = []
    sort = {}

    if not selected_data_pool:
        return []

    if selected_data_pool in ['parallel_en_id', 'parallel_en_ms', 'parallel_en_vi', 'parallel_en_ta', 'parallel_en_th', 'parallel_en_zh', 'bt_data']:
        sort = {"LaBSE": {"order": "asc"}}
    
    if selected_data_pool in ['news_articles_en', 'news_articles_id', 'news_articles_ms', 'news_articles_ta', 'news_articles_th', 'news_articles_vi', 'news_articles_zh']:
        sort = {"date": {"order": "desc"}}
    

    if query.strip():
        filter.append({"multi_match": {"query": query.strip(),
                                       "fields": ["sentence_src", "sentence_tgt", "sentence_en", "sentence_zh", "sentence_ms", "sentence_id", "sentence_vi", "sentence_ta", "sentence_th", "title", "text"]}})

    if selected_language_types:
        filter.append({"terms": {"language_type": selected_language_types}})

    if selected_projects:
        filter.append({"terms": {"project": selected_projects}})

    if selected_domains:
        filter.append({"terms": {"domain": selected_domains}})

    if selected_sources:
        filter.append({"terms": {"source": selected_sources}})

    if selected_custom_tags:
        filter.append({"terms": {"custom_tag": selected_custom_tags}})

    if selected_last_commit_bys:
        filter.append({"terms": {"last_commit_by": selected_last_commit_bys}})


    response = scan(client=es,
                    query={"query": {"bool": {"filter": filter}}},
                    index=selected_data_pool,
                    )

    return response


if __name__=="__main__":
    context_variables = {"selected_data_pool": 'news_articles_zh',
                         "query": '',
                         "selected_language_types": [],
                         "selected_sources": [],
                         "selected_domains": [],
                         "selected_projects": [],
                         "selected_custom_tags": [],
                         "selected_last_commit_bys": [],
                         }
    response=get_all_data(context_variables)
    with open("zh.jsonl", mode="a", encoding="utf8") as file:
        for item in response:
            file.write(json.dumps(item['_source'], ensure_ascii=False)+"\n")
