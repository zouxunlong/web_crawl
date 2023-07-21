from newsplease import NewsPlease

article = NewsPlease.from_url('https://www.channelnewsasia.com/singapore/k-shanmugam-online-citizen-asiaone-leon-perera-driver-nicole-seah-workers-party-3640156')
print(article.get_serializable_dict())


# def crawl(url, init_id, final_id):
#     for i in range(init_id, final_id):
#         article = NewsPlease.from_url('{}{}'.format(url, str(i)))

#         if article.title is not None and article.language == 'th':
#             print(article.title)
#             print(article.maintext)


# if __name__=="__main__":
#     url = "http://www.koratdaily.com/blog.php?id="
#     init_id = 69
#     final_id = 15853
#     crawl(url, init_id, final_id)
