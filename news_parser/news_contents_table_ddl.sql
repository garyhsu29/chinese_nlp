DROP TABLE IF EXISTS news_db.`news_contents`;

CREATE TABLE news_db.`news_contents` (
  `news_id` int(11) NOT NULL AUTO_INCREMENT,
  `news_rss_feeds_id` int(11) NOT NULL,
  `news` varchar(10500) CHARACTER SET utf8mb4 NOT NULL,
  `news_title` varchar(400) CHARACTER SET utf8mb4 DEFAULT NULL,
  `news_description` varchar(4000) CHARACTER SET utf8mb4 DEFAULT NULL,
  `news_keywords` varchar(400) CHARACTER SET utf8mb4 DEFAULT NULL,
  `news_category` varchar(100) CHARACTER SET utf8mb4 DEFAULT NULL,
  `news_published_date` datetime DEFAULT NULL,
  `news_fb_app_id` varchar(100) DEFAULT NULL,
  `news_fb_page` varchar(100) DEFAULT NULL,
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `processed_status` varchar(45) NOT NULL DEFAULT '0',
  `processed_success` tinyint(4) NOT NULL DEFAULT '0',
  PRIMARY KEY (`news_id`),
  UNIQUE KEY `news_id_UNIQUE` (`news_id`),
  FOREIGN KEY (news_rss_feeds_id) REFERENCES news_db.news_rss_feeds(news_rss_feeds_id)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8mb4;