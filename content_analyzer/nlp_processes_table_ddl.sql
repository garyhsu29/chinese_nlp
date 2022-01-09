DROP TABLE IF EXISTS news_db.`news_processes`;

CREATE TABLE news_db.`news_processes` (
  `news_id` int(11)  NOT NULL,
  `process_name` varchar(200) CHARACTER SET utf8mb4 NOT NULL,
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  FOREIGN KEY (news_id) REFERENCES news_db.news_contents(news_id)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8mb4;