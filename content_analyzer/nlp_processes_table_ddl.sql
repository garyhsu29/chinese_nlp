DROP TABLE IF EXISTS news_db.`sent_processes`;

CREATE TABLE news_db.`sent_processes` (
  `news_sent_id` int(11)  NOT NULL,
  `process_name` varchar(200) CHARACTER SET utf8mb4 NOT NULL,
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  FOREIGN KEY (news_sent_id) REFERENCES news_db.news_sents(news_sent_id)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8mb4;