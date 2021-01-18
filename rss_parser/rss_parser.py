import requests 
from collections import defaultdict
import re
import os
import logging
import mysql.connector
from bs4 import BeautifulSoup
import time
import pickle

#with open('loc2server.config', 'rb') as f:
#    configs = pickle.load(f)

DIR_PATH = os.path.dirname(os.path.realpath(__file__))

with open(os.path.join(DIR_PATH,'server2server.config'), 'rb') as f:
    configs = pickle.load(f)

mydb = mysql.connector.connect(
  host = configs['host'],
  user =  configs['user'],
  passwd = configs['passwd'],
  database = configs['database']
)




# Information for logging
FORMAT = '%(asctime)s %(levelname)s: %(message)s'

logging.basicConfig(level=logging.INFO, filename=os.path.join(DIR_PATH, 'logs/rss_parser.log'), filemode='a', format=FORMAT)
requests.adapters.DEFAULT_RETRIES = 5 
class RssParser(object):
    def __init__(self):
        self.start_time = time.time()
        self.rss_source_category_dict = defaultdict(lambda: defaultdict(list))
        self.CDATA_BLOCK = '\<\!\[CDATA\[(.+?)\]\]\>'
        
        # ------- Multiple urls rss source ------- #
        self.headers = {'user-agent': 'Mozilla/5.0 (Macintosh Intel Mac OS X 10_13_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36',  'Connection': 'close'}
        self.yahoo_rss_urls_t1 = ['https://tw.news.yahoo.com/rss/{}'.format(category) for category in ('politics', 'society', 'technology', 'finance', 'sports', 'health')]
        self.yahoo_news_url_dict = {'最新股市': 'https://tw.stock.yahoo.com/rss?category=news', 
                       '台股動態': 'https://tw.stock.yahoo.com/rss?category=tw-market',
                       '國際財經': 'https://tw.stock.yahoo.com/rss?category=intl-markets',
                       '小資理財': 'https://tw.stock.yahoo.com/rss?category=personal-finance',
                       '基金動態': 'https://tw.stock.yahoo.com/rss?category=funds-news',
                       '專家專欄': 'https://tw.stock.yahoo.com/rss?category=column',
                       '研究報導':'https://tw.stock.yahoo.com/rss?category=research'}
        self.ltn_rss_url = ['https://news.ltn.com.tw/rss/{}.xml'.format(category) for category in ['politics', 'society', 'business', 'sports', 'life', 'entertainment', 'opinion', 'world', 'local', 'people', 'novelty', 'all']]
        self.tpn_rss_url = ['https://www.peoplenews.tw/rss/{}'.format(category) for category in ['政治', '財經', '社會', '生活', '文化', '地方', '全球', '台語世界', '特企', '民報觀點','專欄', '專文', '人物', '論壇', '公民', '書摘', '熱門新聞', '總覽','即時']]
        self.newstalk_rss_url =['https://newtalk.tw/rss/category/{}'.format(category) for category in [1, 2, 3, 7, 4, 9, 14, 17, 8, 5, 11, 102, 18, 103, 16, 15, 6]]
        self.ettoday_rss_url = ['http://feeds.feedburner.com/ettoday/{}?format=xml'.format(category) for category in ['news', 'law', 'finance', 'insurance', 'house', 'global', 'army', 'china', 'lifestyle', 'family', 'society', 'local', 'sport', 'fashion', 'teck3c', 'travel', 'gender', 'star', 'pet', 'charity', 'health']]
        self.upmedia_rss_url =['https://www.upmedia.mg/createRSS.php?Type={}'.format(category) for category in [1, 2, 3, 24, 5, 12, 71, 82, 9, 143, 154]]
        self.storm_url_dict = {'風傳媒 - 國際': 'https://feeds.storm.mg/general/storm/cid?id=117',
                               '風傳媒 - 即時': 'https://feeds.storm.mg/general/storm/cid?id=87726',
                               '風傳媒 - 國內': 'https://feeds.storm.mg/general/storm/cid?id=22172',
                               '風傳媒 - 中港澳':'https://feeds.storm.mg/general/storm/cid?id=121',
                               '風傳媒 - 財經': 'https://feeds.storm.mg/general/storm/cid?id=23083',
                               '風傳媒 - 調查': 'https://feeds.storm.mg/general/storm/cid?id=24667',
                               '風傳媒 - 軍事': 'https://feeds.storm.mg/general/storm/cid?id=26644',
                               '風傳媒 - 藝文': 'https://feeds.storm.mg/general/storm/cid?id=84984'}
        self.sina_rss_url = sina_urls = ['https://news.sina.com.tw/rss/{}/tw.xml'.format(category) for category in ['society', 'politics', 'ents', 'china', 'global', 'life', 'travel', 'sports', 'finance', 'edu']]
        self.pchome_rss_urls_dict = {'PCHOME - 最新':'https://news.pchome.com.tw/rss/new',
                                     'PCHOME - 人氣':'https://news.pchome.com.tw/rss/hot',
                                     'PCHOME - 娛樂':'https://news.pchome.com.tw/rss/006',
                                     'PCHOME - 體育':'https://news.pchome.com.tw/rss/007',
                                     'PCHOME - 政治':'https://news.pchome.com.tw/rss/001',
                                     'PCHOME - 生活':'https://news.pchome.com.tw/rss/009',
                                     'PCHOME - 社會':'https://news.pchome.com.tw/rss/002',
                                     'PCHOME - 健康':'https://news.pchome.com.tw/rss/012',
                                     'PCHOME - 財經':'https://news.pchome.com.tw/rss/003',
                                     'PCHOME - 國際':'https://news.pchome.com.tw/rss/011',
                                     'PCHOME - 科技':'https://news.pchome.com.tw/rss/005',
                                     'PCHOME - 旅遊':'https://news.pchome.com.tw/rss/015',
                                     'PCHOME - 公共':'https://news.pchome.com.tw/rss/016',
                                     'PCHOME - 汽車':'https://news.pchome.com.tw/rss/018'}
        self.cna_rss_urls = ['http://feeds.feedburner.com/rsscna/{}?format=xml'.format(category) for category in ['politics', 'finance', 'mainland', 'lifehealth', 'sport', 'health', 'intworld', 'culture', 'social', 'technology', 'stars', 'local']]
        #self.cts_rss_urls = ['https://news.cts.com.tw/rss/{}.xml'.format(category) for category in ['hots', 'sports', 'politics', 'society', 'money', 'entertain', 'goodlife', 'general', 'international', 'program', 'life', 'exclusive']]
        self.ttv_rss_urls = ['https://www.ttv.com.tw/rss/RSSHandler.ashx?d=news{}'.format(category) for category in ['', '&t=E', '&t=L', '&t=A','&t=F', '&t=P', '&t=B', '&t=G', '&t=C', '&t=H', '&t=D', '&t=J']]
        self.bw_urls = ['http://cmsapi.businessweekly.com.tw/?CategoryId=efd99109-9e15-422e-97f0-078b21322450&TemplateId=8E19CF43-50E5-4093-B72D-70A912962D55', 'http://cmsapi.businessweekly.com.tw/?CategoryId=6f061304-ba38-4de9-9960-4e74420e71a0&TemplateId=8E19CF43-50E5-4093-B72D-70A912962D55', 'http://cmsapi.businessweekly.com.tw/?CategoryId=fd8d6ee6-df7c-4d2b-a635-7a6e433c9bd1&TemplateId=8E19CF43-50E5-4093-B72D-70A912962D55', 'http://cmsapi.businessweekly.com.tw/?CategoryId=24612ec9-2ac5-4e1f-ab04-310879f89b33&TemplateId=8E19CF43-50E5-4093-B72D-70A912962D55', 'http://cmsapi.businessweekly.com.tw/?CategoryId=ad004924-65aa-42af-af2f-487bf4e3c185&TemplateId=8e19cf43-50e5-4093-b72d-70a913062d55', 'https://www.businessweekly.com.tw/Event/feedsec.aspx?feedid=10&channelid=15', 'https://www.businessweekly.com.tw/Event/feedsec.aspx?feedid=1&channelid=4', 'https://www.businessweekly.com.tw/Event/feedsec.aspx?feedid=2&channelid=4', 'https://www.businessweekly.com.tw/Event/feedsec.aspx?feedid=5&channelid=4']
        self.epoch_urls = {'北美新聞': 'https://www.epochtimes.com/b5/nsc412.xml',
                           '大陸新聞': 'https://www.epochtimes.com/b5/nsc413.xml',
                           '臺灣新聞': 'https://www.epochtimes.com/b5/nsc414.xml',
                           '港澳新聞': 'https://www.epochtimes.com/b5/nsc415.xml',
                           '國際新聞': 'https://www.epochtimes.com/b5/nsc418.xml',
                           '科技動向': 'https://www.epochtimes.com/b5/nsc419.xml',
                           '財經消息': 'https://www.epochtimes.com/b5/nsc420.xml',
                           '臺灣地方新聞': 'https://www.epochtimes.com/b5/nsc1004.xml',
                           '社會新聞': 'https://www.epochtimes.com/b5/nsc994.xml',
                           '紀元社論': 'https://www.epochtimes.com/b5/nsc422.xml',
                           '紀元特稿': 'https://www.epochtimes.com/b5/nsc424.xml',
                           '專欄文集': 'https://www.epochtimes.com/b5/nsc423.xml',
                           '自由廣場': 'https://www.epochtimes.com/b5/nsc993.xml',
                           '讀編往來': 'https://www.epochtimes.com/b5/nsc1005.xml',
                           '論壇新聞': 'https://www.epochtimes.com/b5/nsc427.xml',
                           '讀者投書': 'https://www.epochtimes.com/b5/nsc426.xml',
                           '線民妙文': 'https://www.epochtimes.com/b5/nsc1015.xml',
                           '新聞時事': 'https://www.epochtimes.com/b5/nsc983.xml',
                           '文教休閒': 'https://www.epochtimes.com/b5/nsc984.xml',
                           '影視音樂': 'https://www.epochtimes.com/b5/nsc982.xml',
                           '文化歷史': 'https://www.epochtimes.com/b5/nsc975.xml',
                           '世界風情': 'https://www.epochtimes.com/b5/nsc976.xml',
                           '生活時尚': 'https://www.epochtimes.com/b5/nsc435.xml',
                           '美食天地': 'https://www.epochtimes.com/b5/nsc445.xml',
                           '醫療保健': 'https://www.epochtimes.com/b5/nsc1002.xml',
                           '留學移民': 'https://www.epochtimes.com/b5/nsc995.xml',
                           '教育園地': 'https://www.epochtimes.com/b5/nsc977.xml',
                           '文學世界': 'https://www.epochtimes.com/b5/nsc924.xml',
                           '藝術長河': 'https://www.epochtimes.com/b5/nsc985.xml',
                           '長篇連載': 'https://www.epochtimes.com/b5/nsc996.xml',
                           '人物專訪': 'https://www.epochtimes.com/b5/nsc1003.xml',
                           '國際足壇': 'https://www.epochtimes.com/b5/nsc706.xml',
                           '中國足球': 'https://www.epochtimes.com/b5/nsc917.xml',
                           '籃球': 'https://www.epochtimes.com/b5/nsc708.xml',
                           '網球': 'https://www.epochtimes.com/b5/nsc709.xml',
                           '棋牌': 'https://www.epochtimes.com/b5/nsc711.xml',
                           '棒球': 'https://www.epochtimes.com/b5/nsc1014.xml',
                           '其他運動': 'https://www.epochtimes.com/b5/nsc712.xml',
                           '港臺速遞':'https://www.epochtimes.com/b5/nsc787.xml',
                           '大陸聚焦':'https://www.epochtimes.com/b5/nsc794.xml',
                           '國際風雲':'https://www.epochtimes.com/b5/nsc1018.xml',
                           '華盛頓DC':'https://www.epochtimes.com/b5/nsc925.xml',
                           '美西北':'https://www.epochtimes.com/b5/nsc1000.xml',
                           '紐約':'https://www.epochtimes.com/b5/nsc529.xml',
                           '新澤西':'https://www.epochtimes.com/b5/nsc530.xml',
                           '波士頓':'https://www.epochtimes.com/b5/nsc531.xml', 
                           '費城':'https://www.epochtimes.com/b5/nsc990.xml',
                           '北加州':'https://www.epochtimes.com/b5/nsc533.xml',
                           '南加州':'https://www.epochtimes.com/b5/nsc970.xml',
                           '美中':'https://www.epochtimes.com/b5/nsc918.xml',
                           '美南':'https://www.epochtimes.com/b5/nsc919.xml',
                           '美東南':'https://www.epochtimes.com/b5/nsc920.xml',
                           '加拿大':'https://www.epochtimes.com/b5/nsc978.xml',
                           '歐洲':'https://www.epochtimes.com/b5/nsc974.xml',
                           '澳洲':'https://www.epochtimes.com/b5/nsc980.xml',
                           '亞太地區':'https://www.epochtimes.com/b5/nsc989.xml',
                           '回顧與展望':'https://www.epochtimes.com/b5/nsc1010.xml',
                           '電子期刊':'https://www.epochtimes.com/b5/nsc1011.xml'}
        
        self.cw_rss_url_dict = {'天下雜誌 - 評論': 'https://www.cw.com.tw/RSS/opinion.xml', 
                                '天下雜誌 - 文章': 'https://www.cw.com.tw/RSS/cw_content.xml'}
        self.eco_report_landing_url = 'https://money.udn.com/rssfeed/lists/1001'
        
        # ------- Single url rss source ------- #
        self.rss_source_dict = defaultdict(list)
        self.msn_rss_url = 'https://rss.msn.com/zh-tw/'
        self.pts_rss_url = 'https://about.pts.org.tw/rss/XML/newsfeed.xml'
        self.newsmarket_rss_url = 'https://www.newsmarket.com.tw/feed/'
        self.twreporter_rss_url = 'https://www.twreporter.org/a/rss2.xml'
        self.coolloud_rss_url = 'https://www.coolloud.org.tw/rss.xml'
        self.peopo_rss_url = 'https://www.peopo.org/rss-news'
        self.cmmedia_rss_url = 'https://www.cmmedia.com.tw/rss/yahoo/article'
        self.rti_rss_url = 'http://www.rti.org.tw/rss/'
        self.bo_rss_url = 'https://buzzorange.com/feed/'
        self.tnl_rss_url = 'https://feeds.feedburner.com/TheNewsLens'
    def yahoo_rss_parser(self):
        for url in self.yahoo_rss_urls_t1:
            try:
                r = requests.get(url, headers = self.headers)
                web_content = r.text
                rss_source_category = re.search(r'(\<title\>)(.+?)(\<\/title\>)', web_content).group(2)
                rss_source_category = re.search(self.CDATA_BLOCK, rss_source_category).group(1)
                rss_category, rss_source = rss_source_category.split('-')
                rss_source = rss_source.strip()
                rss_category = rss_category.strip()
                for index, match in enumerate(re.finditer(r'(\<link\>)(.+?)(\<\/link\>)', web_content)):
                    if index < 2:
                        continue
                    self.rss_source_category_dict[rss_source][rss_category].append((match.group(2), 'Yahoo Source 1'))
            except Exception as e:
                logging.error("Yahoo rss parser (1): {}".format(e))
        for rss_category, url in self.yahoo_news_url_dict.items():
            try:
                r = requests.get(url, headers = self.headers)
                web_content = r.text
                rss_source = re.search(r'(\<title\>)(.+?)(\<\/title\>)', web_content).group(2)
                for index, match in enumerate(re.finditer(r'(\<link\>)(.+?)(\<\/link\>)', web_content)):
                    if index < 2:
                        continue
                    self.rss_source_category_dict[rss_source][rss_category].append((match.group(2), 'Yahoo Source 2'))
            except Exception as e:
                logging.error("Yahoo rss parser (2): {}".format(e))

    def ltn_rss_parser(self):     
        for url in self.ltn_rss_url:
            try:
                r = requests.get(url, headers = self.headers)
                web_content = r.text
                rss_source_category = re.search(r'(\<title\>)(.+?)(\<\/title\>)', web_content).group(2)
                rss_category, rss_source = rss_source_category.split('-')
                rss_category = rss_category.strip()
                rss_source = rss_source.strip()
                for index, match in enumerate(re.finditer(r'(\<link\>)(.+?)(\<\/link\>)', web_content)):
                    if index < 1:
                        continue
                    self.rss_source_category_dict[rss_source][rss_category].append((match.group(2), 'LTN'))
            except Exception as e:
                logging.error("LTN rss parser: {}".format(e))
                logging.error('Error Url: {}'.format(url))

    def tpn_rss_parser(self):
        for url in self.tpn_rss_url:
            try:
                r = requests.get(url, headers = self.headers)
                web_content = r.text
                rss_source_category = re.search(r'(\<title\>)(.+?)(\<\/title\>)', web_content).group(2)
                rss_source, rss_category = rss_source_category.split('-')
                rss_source = rss_source.strip()
                rss_category = rss_category.strip()
                for index, match in enumerate(re.finditer(r'(\<link\>)(.+?)(\<\/link\>)', web_content)):
                    if index < 1:
                        continue
                    self.rss_source_category_dict[rss_source][rss_category].append((match.group(2), 'TPN'))
            except Exception as e:
                logging.error("TPN rss parser: {}".format(e))
                logging.error('Error Url: {}'.format(url))
    
    def newstalk_rss_parser(self):
        for url in self.newstalk_rss_url:
            try:
                r = requests.get(url, headers = self.headers)
                web_content = r.text
                rss_source_category = re.search(r'(\<title\>)(.+?)(\<\/title\>)', web_content).group(2)
                rss_source, rss_category = rss_source_category.split('-')
                rss_source = rss_source.strip()
                rss_category = rss_category.strip()
                for index, match in enumerate(re.finditer(r'(\<link\>)(.+?)(\<\/link\>)', web_content)):
                    if index < 1:
                        continue
                    self.rss_source_category_dict[rss_source][rss_category].append((match.group(2), 'NewsTalk'))
            except Exception as e:
                logging.error("NewsTalk rss parser: {}".format(e))
                logging.error('Error Url: {}'.format(url))
    
    def ettoday_rss_parser(self):
        for url in self.ettoday_rss_url:
            try:
                r = requests.get(url, headers = self.headers)
                web_content = r.text
                rss_source, rss_category = re.search(r'\<title\>.+\n\t\t\t{}'.format(self.CDATA_BLOCK), web_content).group(1).split()
                rss_source = rss_source.strip()
                rss_category = rss_category.strip()
                for index, match in enumerate(re.finditer(r'(\<link\>)(.+?)(<\/link\>)', web_content)):
                    if index < 1:
                        continue
                    self.rss_source_category_dict[rss_source][rss_category].append((match.group(2), 'ETToday'))
            except Exception as e:
                logging.error("Ettoday rss parser: {}".format(e))
                logging.error('Error Url: {}'.format(url))
            

    def udn_rss_parser(self):
        udn_rss_url_dict = self.udn_rss_urls_extractor()
        for category, url in udn_rss_url_dict.items():
            try:
                r = requests.get(url, headers = self.headers)
                r.encoding = 'utf-8'
                web_content = r.text
                rss_source = '經濟日報'
                rss_category = category.strip()
                for index, match in enumerate(re.finditer(r'(\<link\>)(.+?)(<\/link\>)', web_content)):
                    if index < 1:
                        continue
                    self.rss_source_category_dict[rss_source][rss_category].append((match.group(2), 'UDN'))
            except Exception as e:
                logging.error("udn rss parser: {}".format(e))
                logging.error('Error Url: {}'.format(url))
    
    def upmedia_rss_parser(self):
        for url in self.upmedia_rss_url:
            try:
                r = requests.get(url, headers = self.headers)
                web_content = r.text
                title_list = re.search(r'\<title\>(.+?)\<\/title\>', web_content).group(1).split()
                rss_source = ' '.join(title_list[:2]).strip()
                rss_category = title_list[-1].strip()

                for index, match in enumerate(re.finditer(r'(\<link\>)(.+?)(<\/link\>)', web_content)):
                    if index < 1:
                        continue
                    self.rss_source_category_dict[rss_source][rss_category].append((match.group(2), 'UpMedia'))
            except Exception as e:
                logging.error("upmedia rss parser: {}".format(e))
                logging.error('Error Url: {}'.format(url))
            
    def storm_rss_parser(self):
        for key, url in self.storm_url_dict.items():
            try:
                r = requests.get(url, headers = self.headers)
                web_content = r.text
                title_list = re.search(r'\<title\>(.+?)\<\/title\>', web_content).group(1).split()
                rss_source, rss_category = key.split('-')
                rss_source = rss_source.strip()
                rss_category = rss_category.strip()
                
                for index, match in enumerate(re.finditer(r'(\<link\>)(.+?)(<\/link\>)', web_content)):
                    if index < 1:
                        continue
                    self.rss_source_category_dict[rss_source][rss_category].append((match.group(2), 'Storm'))
            except Exception as e:
                logging.error("storm rss parser: {}".format(e))
                logging.error('Error Url: {}'.format(url))
    
    def sina_rss_parser(self):
        for url in self.sina_rss_url:
            try:
                r = requests.get(url, headers = self.headers)
                web_content = r.text
                rss_source_category = re.search(r'(\<title\>)(.+?)(\<\/title\>)', web_content).group(2)
                rss_category, rss_source = rss_source_category.split('-')
                rss_category = rss_category.strip()
                rss_source = rss_source.strip()
                for index, match in enumerate(re.finditer(r'(\<link\>)(.+?)(\<\/link\>)', web_content)):
                    if index < 1:
                        continue
                    self.rss_source_category_dict[rss_source][rss_category].append((match.group(2), 'SINA'))
            except Exception as e:
                logging.error("sina rss parser: {}".format(e))
                logging.error('Error Url: {}'.format(url))

    def pchome_rss_parser(self):
        for key, url in self.pchome_rss_urls_dict.items():
            try:
                r = requests.get(url, headers = self.headers)
                web_content = r.text
                rss_source, rss_category = key.split('-')
                rss_source = rss_source.strip()
                rss_category = rss_category.strip()
                for index, match in enumerate(re.finditer(r'(\<link\>){}(<\/link\>)'.format(self.CDATA_BLOCK), web_content)):
                    if index < 1:
                        continue
                    self.rss_source_category_dict[rss_source][rss_category].append((match.group(2), 'PCHOME'))
            except Exception as e:
                logging.error("pchome rss parser: {}".format(e))
                logging.error('Error Url: {}'.format(url))
    
    def cna_rss_parser(self):
        for url in self.cna_rss_urls:
            try:
                r = requests.get(url, headers = self.headers)
                web_content = r.text
                rss_source, rss_category = re.search(r'\<title\>(.+?)\<\/title\>', web_content).group(1).split()
                rss_source = rss_source.strip()
                rss_category = rss_category.strip()
                for index, match in enumerate(re.finditer(r'(\<link\>)(.+?)(<\/link\>)', web_content)):
                    if index < 2:
                        continue
                    self.rss_source_category_dict[rss_source][rss_category].append((match.group(2), 'CNA'))
            except Exception as e:
                logging.error("cna rss parser: {}".format(e))
                logging.error('Error Url: {}'.format(url))
            
    def cts_rss_parser(self):
        for url in self.cts_rss_urls:
            try:
                r = requests.get(url, headers = self.headers)
                web_content = r.text
                rss_source, rss_category = re.search(r'\<title\>(.+)\<\/title\>', web_content).group(1).split()
                rss_source = rss_source.strip()
                rss_category = rss_category.strip()
                for index, match in enumerate(re.finditer(r'(\<link\>)(.+?)(<\/link\>)', web_content)):
                    if index < 1:
                        continue
                    self.rss_source_category_dict[rss_source][rss_category].append((match.group(2), 'CTS'))
            except Exception as e:
                logging.error("cts rss parser: {}".format(e))
                logging.error('Error Url: {}'.format(url))
    
    def ttv_rss_parser(self):
        for url in self.ttv_rss_urls:
            try:
                r = requests.get(url, headers = self.headers)
                r.encoding='utf-8'
                web_content = r.text
                rss_source_category = re.search(r'(\<title\>)(.+?)(\<\/title\>)', web_content).group(2)
                try:
                    rss_source, rss_category = rss_source_category.split('-')
                    rss_source = rss_source.strip()
                    rss_category = rss_category.strip()
                except:
                    rss_source = '台視新聞'
                    rss_category = '總覽'
                for index, match in enumerate(re.finditer(r'(\<link\>)(.+?)(\<\/link\>)', web_content)):
                    if index < 2:
                        continue
                    self.rss_source_category_dict[rss_source][rss_category].append((match.group(2), 'TTV'))
            except Exception as e:
                logging.error("ttv rss parser: {}".format(e))
                logging.error('Error Url: {}'.format(url))
    
    def bw_rss_parser(self):
        for url in self.bw_urls:
            try:
                r = requests.get(url, headers = self.headers)
                r.encoding='utf-8'
                web_content = r.text
                rss_source_category = re.search(r'(\<title\>)(.+?)(\<\/title\>)', web_content).group(2)
                try:
                    rss_source, rss_category = rss_source_category.split('-')
                    rss_source = rss_source.strip()
                    rss_category = rss_category.strip()
                except:
                    rss_source, rss_category = rss_source_category.split()
                    rss_source = rss_source.strip()
                    rss_category = rss_category.strip()

                for index, match in enumerate(re.finditer(r'(\<link\>)(.+?)(\<\/link\>)', web_content)):
                    if index < 2:
                        continue
                    self.rss_source_category_dict[rss_source][rss_category].append((match.group(2), 'BW'))
            except Exception as e:
                logging.error("bw rss parser: {}".format(e))
                logging.error('Error Url: {}'.format(url))
            
    def epoch_rss_parser(self):
        for key, url in self.epoch_urls.items():
            try:
                r = requests.get(url, headers = self.headers)
                web_content = r.text
                rss_category = key.strip()
                rss_source = '大紀元'.strip()
                for index, match in enumerate(re.finditer(r'(\<link\>)(.+?)(\<\/link\>)', web_content)):
                    if index < 2:
                        continue
                    self.rss_source_category_dict[rss_source][rss_category].append((match.group(2), 'EPOCH'))
            except Exception as e:
                logging.error("epoch rss parser: {}".format(e))
                logging.error('Error Url: {}'.format(url))

    def cw_rss_parser(self):
        for key, url in self.cw_rss_url_dict.items():
            try:
                r = requests.get(url, headers = self.headers)
                r.encoding='utf-8'
                web_content = r.text
                rss_source, rss_category = key.split('-')
                rss_source = rss_source.strip()
                rss_category = rss_category.strip()
                for index, match in enumerate(re.finditer(r'(\<link\>)(.+?)(\<\/link\>)', web_content)):
                    if index < 2:
                        continue
                    self.rss_source_category_dict[rss_source][rss_category].append((match.group(2), 'CW'))

            except Exception as e:
                logging.error("epoch rss parser: {}".format(e))
                logging.error('Error Url: {}'.format(url))

    # ------- Single rss url parsers ------- #
    def msn_rss_parser(self):
        try:
            r = requests.get(self.msn_rss_url, headers = self.headers)
            web_content = r.text
            rss_source = 'MSN'
            for index, match in enumerate(re.finditer(r'(\<link\>)(.+?)(\<\/link\>)', web_content)):
                if index < 2:
                    continue
                self.rss_source_dict[rss_source].append((match.group(2), 'MSN'))
                
        except Exception as e:
            logging.error("MSN rss parser: {}".format(e))
            
    def pts_rss_parser(self):
        try:
            r = requests.get(self.pts_rss_url, headers = self.headers)
            web_content = r.text
            rss_source = '公視新聞網'
            for index, match in enumerate(re.finditer(r'(\<link\>)(.+?)(\<\/link\>)', web_content)):
                if index < 1:
                    continue
                self.rss_source_dict[rss_source].append((match.group(2), 'PTS'))
                
        except Exception as e:
            logging.error("pts rss parser: {}".format(e))
    
    def newsmarket_rss_parser(self):
        try:
            r = requests.get(self.newsmarket_rss_url, headers = self.headers)
            web_content = r.text
            rss_source = re.search(r'(\<title\>)(.+?)(\<\/title\>)', web_content).group(2).strip()
            for index, match in enumerate(re.finditer(r'(\<link\>)(.+?)(\<\/link\>)', web_content)):
                if index < 1:
                    continue
                self.rss_source_dict[rss_source].append((match.group(2), 'NewsMarket'))
        except Exception as e:
            logging.error("nesmarket rss parser: {}".format(e))
            
    def twreporter_rss_parser(self):
        try:
            r = requests.get(self.twreporter_rss_url, headers = self.headers)
            web_content = r.text
            rss_source = re.search(r'(\<title\>){}(\<\/title\>)'.format(self.CDATA_BLOCK), web_content).group(2).strip()
            for index, match in enumerate(re.finditer(r'(\<link\>)(.+?)(\<\/link\>)', web_content)):
                if index < 2:
                    continue
                self.rss_source_dict[rss_source].append((match.group(2), 'TWRepoeter'))
        except Exception as e:
            logging.error("twreporter rss parser: {}".format(e))
    
    def coolloud_rss_parser(self):
        try:
            r = requests.get(self.coolloud_rss_url, headers = self.headers)
            web_content = r.text
            rss_source = re.search(r'(\<title\>)(.+?)(\<\/title\>)', web_content).group(2).strip()
            for index, match in enumerate(re.finditer(r'(\<link\>)(.+?)(\<\/link\>)', web_content)):
                if index < 1:
                    continue
                self.rss_source_dict[rss_source].append((match.group(2), 'CoolLoud'))
        except Exception as e:
            logging.error("coolloud rss parser: {}".format(e))
    
    def peopo_rss_parser(self):
        try:
            r = requests.get(self.peopo_rss_url, headers = self.headers)
            web_content = r.text
            rss_source = '公民新聞'
            for index, match in enumerate(re.finditer(r'(\<link\>)(.+?)(\<\/link\>)', web_content)):
                if index < 1:
                    continue
                self.rss_source_dict[rss_source].append((match.group(2), 'Peopo'))
        except Exception as e:
            logging.error("peopo rss parser: {}".format(e))
            
    def cmmedia_rss_parser(self):
        try:
            r = requests.get(self.cmmedia_rss_url, headers = self.headers)
            web_content = r.text
            rss_source = re.search(r'(\<title\>)(.+?)(\<\/title\>)', web_content).group(2).strip()
            for index, match in enumerate(re.finditer(r'(\<link\>)(.+?)(\<\/link\>)', web_content)):
                if index < 1:
                    continue
                self.rss_source_dict[rss_source].append((match.group(2), 'CMMedia'))
        except Exception as e:
            logging.error("cmmedia rss parser: {}".format(e))
    
    def rti_rss_parser(self):
        try:
            r = requests.get(self.rti_rss_url, headers = self.headers)
            web_content = r.text
            rss_source = re.search(r'(\<title\>)(.+?)(\<\/title\>)', web_content).group(2).strip()
            for index, match in enumerate(re.finditer(r'(\<link\>)(.+?)(\<\/link\>)', web_content)):
                if index < 2:
                    continue
                self.rss_source_dict[rss_source].append((match.group(2), 'RTI'))
        except Exception as e:
            logging.error("rti rss parser: {}".format(e))
    
    def bo_rss_parser(self):
        try:
            r = requests.get(self.bo_rss_url, headers = self.headers)
            web_content = r.text
            rss_source = re.search(r'(\<title\>)(.+?)(\<\/title\>)', web_content).group(2).strip()
            for index, match in enumerate(re.finditer(r'(\<link\>)(.+?)(\<\/link\>)', web_content)):
                if index < 1:
                    continue
                self.rss_source_dict[rss_source].append((match.group(2), 'BO'))
        except Exception as e:
            logging.error("bo rss parser: {}".format(e))
            
    def tnl_rss_parser(self):
        try:
            r = requests.get(self.tnl_rss_url, headers = self.headers)
            web_content = r.text
            rss_source = re.search(r'(\<title\>)(.+?)(\<\/title\>)', web_content).group(2).strip()
            for index, match in enumerate(re.finditer(r'(\<link\>)(.+?)(\<\/link\>)', web_content)):
                if index < 2:
                    continue
                self.rss_source_dict[rss_source].append((match.group(2), 'TNL'))
        except Exception as e:
            logging.error("rti rss parser: {}".format(e))
    
    def udn_rss_urls_extractor(self):
        udn_rss_url_dict = {}
        udn_rss_landing_url = 'https://money.udn.com/rssfeed/lists/1001'
        r = requests.get(udn_rss_landing_url, headers = self.headers)
        web_content = r.text
        soup = BeautifulSoup(web_content, 'lxml')
        for group_index, group in enumerate(soup.find_all('div', class_ = 'group')):
            if group_index < 1:
                continue
            for dt in group.find_all('dt'):
                category = dt.get_text()
                category_url = dt.find('a').get('href')
                if category and category_url:
                    udn_rss_url_dict[category] = category_url
        return udn_rss_url_dict
    
    def rss_insert(self):
        for news_source in self.rss_source_category_dict.keys():
            for news_category in self.rss_source_category_dict[news_source].keys():
                for url, rss_source in self.rss_source_category_dict[news_source][news_category]:
                    mycursor = mydb.cursor()
                    try:
                        sql = "INSERT INTO `news_db`.news_rss_feeds (news_url, news_source, news_category, rss_source) VALUES (%s, %s, %s, %s)"
                        val = (str(url), str(news_source), str(news_category), str(rss_source))
                        mycursor.execute(sql, val)
                    except Exception as e:
                        # duplicated news_url, news_source, news_category
                        if e.errno == 1062:
                            mycursor.close()
                            continue
                        logging.error(e)
                        print(e)
                    else:
                        mydb.commit()
                    mycursor.close()
        
        for news_source, source_url_lst in self.rss_source_dict.items():
            for url, rss_source in source_url_lst:
                mycursor = mydb.cursor()
                try:
                    sql = "INSERT INTO `news_db`.news_rss_feeds (news_url, news_source, rss_source) VALUES (%s, %s, %s)"
                    val = (str(url), str(news_source), str(rss_source))
                    mycursor.execute(sql, val)
                except Exception as e:
                  # duplicated news_url, news_source
                    if e.errno == 1062:
                        mycursor.close()
                        continue
                    logging.error(e)
                    print(e)
                else:
                    mydb.commit()
                mycursor.close()
    def log_printer(self):
      logging.info('Finish crawl content in {} second'.format(time.time() - self.start_time))




rss_parser = RssParser()

# ------- Update the multiple urls source ------- #
rss_parser.yahoo_rss_parser()
rss_parser.ltn_rss_parser()
rss_parser.tpn_rss_parser()
rss_parser.newstalk_rss_parser()
rss_parser.ettoday_rss_parser()
rss_parser.udn_rss_parser()
rss_parser.upmedia_rss_parser()
rss_parser.storm_rss_parser()
rss_parser.sina_rss_parser()
rss_parser.pchome_rss_parser()
rss_parser.cna_rss_parser()
#rss_parser.cts_rss_parser()
rss_parser.ttv_rss_parser()
rss_parser.bw_rss_parser()
rss_parser.epoch_rss_parser()
rss_parser.cw_rss_parser()


# ------- Update the signle url source ------- #

rss_parser.msn_rss_parser()
rss_parser.pts_rss_parser()
rss_parser.newsmarket_rss_parser()
rss_parser.twreporter_rss_parser()
rss_parser.coolloud_rss_parser()
rss_parser.peopo_rss_parser()
rss_parser.cmmedia_rss_parser()
rss_parser.rti_rss_parser()
rss_parser.bo_rss_parser()
rss_parser.tnl_rss_parser()



rss_parser.rss_insert()
rss_parser.log_printer()
mydb.close()