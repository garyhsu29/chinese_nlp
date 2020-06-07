CREATE TABLE news_db.`news_rss_feeds` (
  `news_rss_feeds_id` int(11) NOT NULL AUTO_INCREMENT,
  `news_url` varchar(500) CHARACTER SET utf8mb4 NOT NULL,
  `news_source` varchar(100) CHARACTER SET utf8mb4 NOT NULL,
  `news_category` varchar(100) CHARACTER SET utf8mb4 DEFAULT NULL,
  `processed_status` tinyint(4) NOT NULL DEFAULT '0',
  `processed_success` tinyint(4) NOT NULL DEFAULT '0',
  `created_at` datetime DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`news_rss_feeds_id`),
  UNIQUE KEY `rss_id_UNIQUE` (`news_rss_feeds_id`),
  UNIQUE KEY `unique_source_category_url` (`news_source`,`news_category`,`news_url`)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8mb4;