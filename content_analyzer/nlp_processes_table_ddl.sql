DROP TABLE IF EXISTS news_db.`nlp_processes`;

CREATE TABLE news_db.`nlp_processes` (
  `nlp_processes_id` int(11) NOT NULL AUTO_INCREMENT,
  `news_sent_id` int(11)  NOT NULL,
  `process_name` varchar(200) CHARACTER SET utf8mb4 NOT NULL,
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`nlp_processes_id`),
  UNIQUE KEY `nlp_processes_id_UNIQUE` (`nlp_processes_id`),
  FOREIGN KEY (news_sent_id) REFERENCES news_db.news_sents(news_sent_id)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8mb4;