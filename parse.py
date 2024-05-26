import requests
import urllib.parse
import ssl
from bs4 import BeautifulSoup
from dataclasses import dataclass 
import re
import json
import pymysql



@dataclass 
class kCardNews:
    title : str = None
    news_id : int = None
    release_date : str = None
    content : dict = None
    def initContent(self) -> bool:
        json_news = {}
        url = 'https://www.k-startup.go.kr/web/contents/webCARD_NEWS.do?page=1&viewCount=32&id=' + str(self.news_id) + '&schBdcode=&schGroupCode=&bdExt9=&bdExt10=&bdExt11=&bdUseyn=&schM=view'
        res = urllib.request.urlopen(url)
        html_data = BeautifulSoup(res.read(), 'html.parser')
        
        json_news['notice'] = str(html_data.find(class_="txt"))
        #print(str(json_news['notice']))
        slider_nav = html_data.find(class_='slider_nav')
        if slider_nav == None:
            return False
        
        url_list = []
        for element in slider_nav.select('div'):
            img = element.select_one('img')
            url_list.append(img.get('src'))
        json_news['images'] = url_list
        self.content = json_news
        return True

@dataclass 
class kPost: 
    flag_type: str = None
    d_day: str = None
    article_id : int = None
    title : str = None
    agency : str = None
    additional_info : list = None
    organization : str = None
    ##############################
    date_begin : str = None
    date_end : str = None
    content : dict = None
    
    def toJson(self) -> json:
        json_object = {}
        json_object['flag_type'] = self.flag_type
        json_object['d_day'] = self.d_day
        json_object['article_id'] = self.article_id
        json_object['title'] = self.title
        json_object['agency'] = self.agency
        json_object['additional_info'] = self.additional_info
        return json_object
    def initContent(self) -> bool:
        url = 'https://www.k-startup.go.kr/web/contents/bizpbanc-ongoing.do?schM=view&pbancSn=' + str(self.article_id)
        res = urllib.request.urlopen(url)
        json_post = {}
        html_data = BeautifulSoup(res.read(), 'html.parser')
        info_box = html_data.find(class_='information_box-wrap')
        assert type(info_box) is not None, 'info_box is None Type'

        info_bg_box = html_data.find(class_='bg_box')
        assert type(info_bg_box) is not None, 'info_bg_box is None Type'

        information_box = {}
        for element in info_bg_box.select('ul'):
            for inner in element.select('li'):
                table_inner = inner.find(class_='table_inner')
                assert type(table_inner) is not None, 'table_inner is None Type'
                tit = table_inner.find(class_='tit')
                txt = table_inner.find(class_='txt')
                assert type(tit) is not None, 'tit is None Type'
                assert type(txt) is not None, 'txt is None Type'
                information_box[tit.text.strip()] = txt.text.strip()
        json_post['info_box'] = information_box
        info_box = html_data.find(class_='information_list-wrap')
        assert type(info_box) is not None, 'info_box is None Type'
        information_list = str(info_box)
        attachment_list = []
        board_file = html_data.find(class_='board_file')
        if board_file != None:
            for attachment in board_file.select('li'):
                file_bg = attachment.find(class_='file_bg')
                if file_bg != None:
                    file_node = {}
                    file_name = file_bg.text.strip()
                    btn_down = attachment.find(class_='btn_down')
                    file_node['name'] = file_name
                    file_node['url'] = btn_down.get('href')
                    attachment_list.append(file_node)

                    
        json_post['attachment_list'] = attachment_list   
        json_post['desc_list'] = information_list
        self.content = json_post
        dates = json_post['info_box']['접수기간'].split(' ~ ')
        self.date_begin = dates[0]
        self.date_end = dates[1]

        return True
def getPosts(pageNumb):
    hdr = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36'}
    data = 'pbancClssCd=&pbancEndYn=N&schStr=regist&scrapYn=&suptBizClsfcCd=&suptReginCd=&aplyTrgtCd=&bizTrgtAgeCd=&bizEnyyCd=&siEng1=false&siEng2=false&siEng3=false&siEng4=false&siKor1=false&siKor2=false&siKor3=false&siAll=false&bizPbancNm='
    data = data.encode('utf-8')
    req = urllib.request.Request('https://www.k-startup.go.kr/web/module/bizpbanc-ongoing_bizpbanc-inquiry-ajax.do?page=' + str(pageNumb), headers=hdr)
    context = ssl._create_unverified_context()
    try:
        res = urllib.request.urlopen(req, context=context, data=data)
    except:
        return 0
    html_data = BeautifulSoup(res.read(), 'html.parser')
    posts = []
    for element in html_data.select('li'):
        post = kPost()
        tag = element.find(class_='top').find(class_=lambda value: (value and 'flag type' in value))
        if tag == None:
            continue
        post.flag_type = tag.text.strip()
        
        tag = element.find(class_='top').find(class_='flag day')
        if tag == None:
            continue
        post.d_day = tag.text.strip()

        tag = element.find(class_='left').find(class_='flag_agency')
        if tag == None:
            continue
        post.agency = tag.text.strip()
        
        tag = element.find(class_='middle').find('a', href=True)
        if tag == None:
            continue
        article_id = re.findall(r'\d+', tag['href'])
        if len(article_id) == 0:
            continue
        post.article_id = int(article_id[0])
        
        tag = element.find(class_='middle').find(class_='tit')
        if tag == None:
            continue
        post.title = tag.text.strip()
        
        tag = element.find(class_='bottom').find_all(class_='list')
        if tag == None:
            continue
        additional_info = []
        for info in tag:
            additional_info.append(info.text.strip())
        post.additional_info = additional_info
        post.organization = additional_info[1]

        posts.append(post)
    return posts

def sqlStr(str):
    return str.replace("'", "\\'").replace('"', '\\"')
def queryArticles(pageNumb):
    articles = getPosts(pageNumb)
    for i in range(0, len(articles)):
        articles[i].initContent()
    return articles

def commitArticle(conn, article):
    cur = conn.cursor()
    json_string = json.dumps(article.content, ensure_ascii=False)
    sql = f"INSERT INTO articles(`a_id`, `a_title`, `date_begin`, `date_end`, `agency`, `tag`, `organization`) VALUES ({article.article_id}, '{sqlStr(article.title)}', '{sqlStr(article.date_begin)}', '{sqlStr(article.date_end)}', '{article.agency}', '{sqlStr(article.flag_type)}', '{sqlStr(article.organization)}');" 
    cur.execute(sql)
    conn.commit()
    json_string = json.dumps(article.content, ensure_ascii = False)
    sql = f"INSERT INTO articleContents(`a_id`, `a_content`) VALUES ({article.article_id}, '{sqlStr(json_string)}');"
    cur.execute(sql)
    conn.commit()
    cur.close()
def updateArticle(conn, article):
    cur = conn.cursor()
    json_string = json.dumps(article.content, ensure_ascii = False)
    sql = f"UPDATE articleContents SET a_content = '{sqlStr(json_string)}' WHERE a_id = {article.article_id};"
    cur.execute(sql)
    conn.commit()
    cur.close()


def getCardNews(pageNumb):
    url = 'https://www.k-startup.go.kr/web/contents/webCARD_NEWS.do?viewCount=15&page=' + str(pageNumb)
    res = urllib.request.urlopen(url)
    html_data = BeautifulSoup(res.read(), 'html.parser')
    gallery_list = html_data.find(class_='gallery_list card_news')
    if gallery_list == None:
        print('gallery_list == None')
        return []
    cardnews_list = []
    for element in gallery_list.select('li'):
        cardnews = kCardNews()
        a = element.find('a')
        title = a.get('title') #
        cardnews.title = title
        
        news_id = re.findall(r'\d+', a.get('onclick'))
        if len(news_id) == 0:
            continue
        news_id = news_id[0] #
        cardnews.news_id = news_id
        
        release_date = a.find(class_='date').text.strip()#
        cardnews.release_date = release_date
        cardnews_list.append(cardnews)
    return cardnews_list
def queryCardNews(pageNumb):
    cardnews = getCardNews(pageNumb)
    for i in range(0, len(cardnews)):
        cardnews[i].initContent()
    return cardnews

def commitCardNews(conn, cardnews):
    cur = conn.cursor()
    json_string = json.dumps(cardnews.content, ensure_ascii=False)
    sql = f"INSERT INTO cardnews(`idcardnews`, `title`, `release_date`, `content`) VALUES ({cardnews.news_id}, '{sqlStr(cardnews.title)}', '{sqlStr(cardnews.release_date)}', '{sqlStr(json_string)}');" 
    cur.execute(sql)
    conn.commit()
    cur.close()

def updateCardNews(conn, cardnews):
    cur = conn.cursor()
    json_string = json.dumps(cardnews.content, ensure_ascii=False)
    sql = f"UPDATE cardnews SET content = '{sqlStr(json_string)}' WHERE idcardnews = {cardnews.news_id};"
    cur.execute(sql)
    conn.commit()
    cur.close()
def fetchArticles(conn):
    ret = []
    sql = "SELECT a_id FROM articles"
    with conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            result = cur.fetchall()
            for data in result:
                ret.append(data[0])
    return ret
def fetchCardNews(conn):
    ret = []
    sql = "SELECT idcardnews FROM cardnews"
    with conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            result = cur.fetchall()
            for data in result:
                ret.append(data[0])
    return ret


conn = pymysql.connect(host='g-startup-db.cvmmi8yioimp.ap-northeast-2.rds.amazonaws.com', user='admin', password='G-start-up!', db='g_startup_db', charset='utf8mb4')
a_articles = fetchArticles(conn)
print(f"now have {len(a_articles)} for articles")
conn = pymysql.connect(host='g-startup-db.cvmmi8yioimp.ap-northeast-2.rds.amazonaws.com', user='admin', password='G-start-up!', db='g_startup_db', charset='utf8mb4')
a_cardnews = fetchCardNews(conn)
print(f"now have {len(a_cardnews)} for cardnews")
for i in range(1, 7):
    conn = pymysql.connect(host='g-startup-db.cvmmi8yioimp.ap-northeast-2.rds.amazonaws.com', user='admin', password='G-start-up!', db='g_startup_db', charset='utf8mb4')

    posts = queryArticles(i)
    for post in posts:
        if int(post.article_id) in a_articles:
            updateArticle(conn, post)
        else:
            commitArticle(conn, post)

    cardnews = queryCardNews(i)
    for post in cardnews:
        if int(post.news_id) in a_cardnews:
            updateCardNews(conn, post)
        else:
            commitCardNews(conn, post)

    conn.close()
    print('Process (' + str(i) + ' / 6)')