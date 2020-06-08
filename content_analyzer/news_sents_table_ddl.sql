DROP TABLE IF EXISTS news_db.`news_sents`;

CREATE TABLE news_db.`news_sents` (
  `news_sent_id` int(11) NOT NULL AUTO_INCREMENT,
  `news_id` int(11) NOT NULL,
  `sent_index` int(11) NOT NULL,
  `sent` varchar(10500) CHARACTER SET utf8mb4 NOT NULL,
  `created_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`news_sent_id`),
  UNIQUE KEY `news_sent_id_UNIQUE` (`news_sent_id`),
  FOREIGN KEY (news_id) REFERENCES news_db.news_contents(news_id)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8mb4;