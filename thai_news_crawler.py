# crawl through fixed set of thai news sites with news-please
# To run program from start instead of continuing, clear crawl_state.txt and article_num.txt
from newsplease import NewsPlease
from pathlib import Path

"""

"""
loop = True
while loop:
    url_choice = input("Enter number corresponding to chosen site:\n"
                       "1. thairath\n"
                       "2. dailynews\n"
                       "3. matichon\n"
                       "4. khaosod\n"
                       "5. thaipost\n"
                       "6. komchadluek\n"
                       "7. posttoday\n"
                       "8. banmuang\n"
                       "9. naewna\n"
                       "10. siamrath\n"
                       "11. siamturakij\n"
                       "12. bbc\n"
                       "13. bangkokbiznews\n"
                       "14. sanook\n"
                       "15. kapook\n"
                       "16. tvpoolonline\n"
                       "17. nationtv\n"
                       "18. siamdara\n"
                       "19. prachachat\n"
                       "20. thansettakij\n"
                       "21. ch3plus\n"
                       "22. koratdaily\n"
                       "23. newtv\n")
    if url_choice == '1':
        url = "http://www.thairath.co.th/content/"
        crawl_folder = "thairath"
        initial_x_value = 100000
        final_x_value = 950001
        loop = False
    elif url_choice == '2':
        url = "https://www.dailynews.co.th/article/"
        crawl_folder = "dailynews"
        initial_x_value = 330000
        final_x_value = 575001
        loop = False
    elif url_choice == '3':
        url = "https://www.matichon.co.th/international/news_"
        crawl_folder = "matichon"
        initial_x_value = 2535000
        final_x_value = 3050000
        loop = False
    elif url_choice == '4':
        url = "https://www.khaosod.co.th/around-the-world-news/news_"
        crawl_folder = "khaosod"
        initial_x_value = 6300000
        final_x_value = 6700000
        loop = False
    elif url_choice == '5':
        url = "https://www.thaipost.net/abroad-news/"
        crawl_folder = "thaipost"
        initial_x_value = 42000
        final_x_value = 47000
        loop = False
    elif url_choice == '6':
        url = "https://www.komchadluek.net/news/"
        crawl_folder = "komchadluek"
        initial_x_value = 100000
        final_x_value = 497763
        loop = False
    elif url_choice == '7':
        url = "https://www.posttoday.com/world/"
        crawl_folder = "posttoday"
        initial_x_value = 500000
        final_x_value = 671136
        loop = False
    elif url_choice == '8':
        url = "https://www.banmuang.co.th/news/inter/"
        crawl_folder = "banmuang"
        initial_x_value = 74262
        final_x_value = 262982
        loop = False
    elif url_choice == '9':
        url = "https://www.naewna.com/inter/"
        crawl_folder = "naewna"
        initial_x_value = 550000
        final_x_value = 625525
        loop = False
    elif url_choice == '10':
        url = "https://siamrath.co.th/n/"
        crawl_folder = "siamrath"
        initial_x_value = 100000
        final_x_value = 309692
        loop = False
    elif url_choice == '11':
        url = "https://siamturakij.com/news/"
        crawl_folder = "siamturakij"
        initial_x_value = 10000
        final_x_value = 44049
        loop = False
    elif url_choice == '12':
        url = "https://www.bbc.com/thai/international-"
        crawl_folder = "bbc"
        initial_x_value = 55735058
        final_x_value = 59830561
        loop = False
    elif url_choice == '13':
        url = "https://www.bangkokbiznews.com/news/"
        crawl_folder = "bangkokbiznews"
        initial_x_value = 500500
        final_x_value = 980500
        loop = False
    elif url_choice == '14':
        url = "https://www.sanook.com/news/"
        crawl_folder = "sanook"
        initial_x_value = 1178145
        final_x_value = 8497294
        loop = False
    elif url_choice == '15':
        url = "https://hilight.kapook.com/view/"
        crawl_folder = "kapook"
        initial_x_value = 100000
        final_x_value = 219800
        loop = False
    elif url_choice == '16':
        url = "https://www.tvpoolonline.com/content/"
        crawl_folder = "tvpoolonline"
        initial_x_value = 1800000
        final_x_value = 1915336
        loop = False
    elif url_choice == '17':
        url = "https://www.nationtv.tv/news/"
        crawl_folder = "nationtv"
        initial_x_value = 378466023
        final_x_value = 378859077
        loop = False
    # elif url_choice == '18':
    #     url = "http://www.siamdara.com/hot-news/thai-news/"
    #     crawl_folder = "siamdara"
    #     initial_x_value = 1123762
    #     final_x_value = 1152194
    #     loop = False
    # elif url_choice == '19':
    #     url = "https://www.prachachat.net/world-news/news-"
    #     crawl_folder = "prachachat"
    #     initial_x_value = 787
    #     final_x_value = 834839
    #     loop = False
    # elif url_choice == '20':
    #     url = "https://www.thansettakij.com/world/"
    #     crawl_folder = "thansettakij"
    #     initial_x_value = 150000
    #     final_x_value = 508968
    #     loop = False
    # elif url_choice == '21':
    #     url = "https://ch3plus.com/news/category/"
    #     crawl_folder = "ch3plus"
    #     initial_x_value = 118558
    #     final_x_value = 273061
    #     loop = False
    # elif url_choice == '22':
    #     url = "http://www.koratdaily.com/blog.php?id="
    #     crawl_folder = "koratdaily"
    #     initial_x_value = 69
    #     final_x_value = 15853
    #     loop = False
    elif url_choice == '23':
        url = "https://www.newtv.co.th/news/"
        crawl_folder = "newtv"
        initial_x_value = 1
        final_x_value = 93521
        loop = False

    else:
        print("Please enter valid input")

# allows checking if crawl_state isempty
mypath = Path("C:\\Users\\Lenovo\\news-please-repo\\%s\\crawl_state.txt" % crawl_folder)
if mypath.stat().st_size == 0:
    x = initial_x_value
else:
    # if not empty, continue previous crawl
    with open('C:\\Users\\Lenovo\\news-please-repo\\%s\\crawl_state.txt' % crawl_folder, 'r') as f:
        x = int(f.read())

# prevents skips in article txt file num
mypath2 = Path("C:\\Users\\Lenovo\\news-please-repo\\%s\\article_num.txt" % crawl_folder)
if mypath.stat().st_size == 0:
    y = 1
else:
    with open('C:\\Users\\Lenovo\\news-please-repo\\%s\\article_num.txt' % crawl_folder, 'r') as f:
        y = int(f.read())


# In PyCharm go to "Run"/"Edit Configurations" and check "Emulate terminal in output console".
# PyCharm now accepts keyboard interrupts
print("Enter 'CTRL + C' to end the program")

for i in range(x, final_x_value):
    try:
        article = NewsPlease.from_url('%s%d' % (url,i))

        if article.title is not None and article.language == 'th':
            print(article.title)
            with open('C:\\Users\\Lenovo\\news-please-repo\\%s\\title %d.txt' % (crawl_folder,y), 'w',encoding='utf-8') as f: #
                f.write(article.title)
            y += 1

    except KeyboardInterrupt:
        with open('C:\\Users\\Lenovo\\news-please-repo\\%s\\crawl_state.txt' % crawl_folder, 'w') as f:
            f.write(str(i))
        with open('C:\\Users\\Lenovo\\news-please-repo\\%s\\article_num.txt' % crawl_folder, 'w') as f:
            f.write(str(y))
        print("Ending...")
        break
    except Exception as e:
        print(e)
        continue

with open('C:\\Users\\Lenovo\\news-please-repo\\%s\\crawl_state.txt' % crawl_folder, 'w') as f:
    f.write(str(i))
with open('C:\\Users\\Lenovo\\news-please-repo\\%s\\article_num.txt' % crawl_folder, 'w') as f:
    f.write(str(y))
print("Program Ended")

