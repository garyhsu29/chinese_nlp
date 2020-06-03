# Rss Parsing:
### Files:
* [rss\_parsing\_notebook.ipynb:](https://github.com/garyhsu29/chinese_nlp/blob/master/crawler/rss_parsing_notebook.ipynb) 使用 Requests，Regular Expression 以及少量的 BeautifulSoup function來取得所有在Rss Feeds上的新聞網址。
*  [rss_parser.py](https://github.com/garyhsu29/chinese_nlp/blob/master/rss_parser/rss_parser.py) 把 notebook 格式的程式碼轉換成可使用 crontab 定時執行的 .py 格式
*  logs/

## Rss Feed could be:  
- Multiple Rss Urls
- Single Rss Url

### Multiple Rss urls landing page:  
 1. [Yahoo 奇摩股市](https://tw.stock.yahoo.com/rss_index.html)
 2. [自由時報 (LTN)](https://service.ltn.com.tw/RSS)
 3. [民報](https://www.peoplenews.tw/subscription)
 4. [新頭殼](https://newtalk.tw/rss)
 5. [東森新聞 (Ettoday)](https://www.ettoday.net/events/news-express/epaper.php)
 6. [上報](https://www.upmedia.mg/rss.php)
 7. [風傳媒](https://www.storm.mg/feeds)
 8. [新浪台灣(Sina)](https://news.sina.com.tw/rss/index.html)
 9. [Pchome](https://news.pchome.com.tw/member_rss/)
 10. [中央通訊社(CNA)](http://rss.cna.com.tw/rsscna/)
 11. [華視 (CTS)](https://news.cts.com.tw/plugin/)
 12. [台視 (TTV)](https://www.ttv.com.tw/rss/)
 13. [大紀元 (Epoch)](https://www.epochtimes.com/b5/djy-rss.htm)
 14. [聯合新聞網-經濟日報](https://money.udn.com/rssfeed/lists/1001) 
 15. [商業週刊 （內容與新聞差異較大，未放入資料庫）](https://www.businessweekly.com.tw/RSS) 
 16. [天下雜誌（內容與新聞差異較大，未放入資料庫）](https://www.cw.com.tw/article/article.action?id=5070394)  


### Single Rss url:
1. [MSN](https://rss.msn.com/zh-tw/)  
2. [公視 (PTS)](https://about.pts.org.tw/rss/XML/newsfeed.xml)
3. [上下游新聞(Newsmarket)](https://www.newsmarket.com.tw/feed/) 
4. [報導者新聞 (內容為專文，和新聞文章差異較大，未放入資料庫)](https://www.twreporter.org/a/rss2.xml)
5. [苦勞網 (內容為專文，和新聞文章差異較大，未放入資料庫)](https://www.coolloud.org.tw/rss.xml)
6. [公民新聞網](https://www.peopo.org/rss-news)
7. [MATA TAIWAN：為原住民內容，內文不多，未放入資料庫](https://www.matataiwan.com/feed/)
8. [信傳媒](https://www.cmmedia.com.tw/rss/yahoo/article)
9. [RTI 中央廣播電台](http://www.rti.org.tw/rss/)
10. [報橘](https://buzzorange.com/feed/)
11. [關鍵新聞網](https://feeds.feedburner.com/TheNewsLens)

