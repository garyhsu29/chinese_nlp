DROP TABLE IF EXISTS news_db.`news_title_words`;

CREATE TABLE news_db.`news_title_words` (
  `news_title_word_id` int(11) NOT NULL AUTO_INCREMENT,
  `news_id` int(11)  NOT NULL,
  `word_index` int(11) NOT NULL,
  `word` varchar(1000) CHARACTER SET utf8mb4 NOT NULL,
  `word_pos` varchar(200) NOT NULL,
  `word_approachs` varchar(200) NOT NULL,
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`news_title_word_id`),
  UNIQUE KEY `news_title_word_id_UNIQUE` (`news_title_word_id`),
  FOREIGN KEY (news_id) REFERENCES news_db.news_contents(news_id)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8mb4;