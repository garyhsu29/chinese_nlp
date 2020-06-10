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


### 3. Content analyzer
* 利用 Regular Expression 來切將文章切成段落（有考慮用每一個句號來切，但最後考慮到中文表達的特性，還是按照段落來切比較能完整表達）
* 利用 [CkipTagger](https://github.com/ckiplab/ckiptagger):
	* 將句子切成單詞：今天天氣真好。 -> [今天, 天氣, 真, 好, 。]
	* 將單詞標記上他的詞性：今天天氣真好。 -> [今天(Nd), 天氣(Na), 真(D), 好(VH), 。(PERIODCATEGORY)] [更多詞性標記解釋。](https://github.com/ckiplab/ckiptagger/wiki/POS-Tags)
	* 將單詞裡面的專有名詞抓出來： 今天天氣真好。 -> ('DATE', '今天') [更多專有名詞標記](https://github.com/ckiplab/ckiptagger/wiki/Entity-Types)。
