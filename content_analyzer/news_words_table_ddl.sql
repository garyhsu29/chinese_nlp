DROP TABLE IF EXISTS news_db.`news_words`;

CREATE TABLE news_db.`news_words` (
  `news_word_id` int(11) NOT NULL AUTO_INCREMENT,
  `news_sent_id` int(11)  NOT NULL,
  `word_index` int(11) NOT NULL,
  `word` varchar(1000) CHARACTER SET utf8mb4 NOT NULL,
  `word_pos` varchar(200) NOT NULL,
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`news_word_id`),
  UNIQUE KEY `news_word_id_UNIQUE` (`news_word_id`),
  FOREIGN KEY (news_sent_id) REFERENCES news_db.news_sents(news_sent_id)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8mb4;