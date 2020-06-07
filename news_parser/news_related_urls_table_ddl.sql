DROP TABLE IF EXISTS news_db.`news_related_urls`;

CREATE TABLE news_db.`news_related_urls` (
  `news_urls_id` int(11) NOT NULL AUTO_INCREMENT,
  `news_related_url` varchar(5000) CHARACTER SET utf8mb4 NOT NULL,
  `news_related_url_desc` varchar(5000) CHARACTER SET utf8mb4 NOT NULL,
  `news_id` int(11) NOT NULL,
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`news_urls_id`),
  UNIQUE KEY `news_urls_id_UNIQUE` (`news_urls_id`),
  FOREIGN KEY (news_id) REFERENCES news_db.news_contents(news_id)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8mb4;