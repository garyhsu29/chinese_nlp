## 1. Chinese NLP project:
![NLP Image](./CHINESENLP.png)

### 1. Rss parser
* 利用 requests 從 rss url 來獲取該 url 的內容
* 利用 Regular Expression / BeautifulSoup：從 url 的內容來獲取「標題 (title)」和「連結 (link)」
* 更多內容請參考資料夾 [rss\_parser](https://github.com/garyhsu29/chinese_nlp/tree/master/rss_parser)

### 2. News parser
* 利用 rss\_parser 取得的 news\_url 來獲取連結內容的新聞資訊
*  利用 BeautifulSoup/Regular Expression 從原始的 html 之中提取出 新聞內容、新聞標題、新聞描述、新聞關鍵字、新聞相關連結等等
*  更多內容請參考資料夾 [news\_parser](https://github.com/garyhsu29/chinese_nlp/tree/master/news_parser)

