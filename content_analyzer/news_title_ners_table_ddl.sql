DROP TABLE IF EXISTS news_db.`news_title_ners`;

CREATE TABLE news_db.`news_title_ners` (
  `news_title_ner_id` int(11) NOT NULL AUTO_INCREMENT,
  `news_id` int(11)  NOT NULL,
  `start_index` int(11) NOT NULL,
  `end_index` int(11) NOT NULL,
  `ent_text` varchar(1000) CHARACTER SET utf8mb4 NOT NULL,
  `ent_type` varchar(500) CHARACTER SET utf8mb4 NOT NULL,
  `ner_approach` varchar(100) CHARACTER SET utf8mb4 NOT NULL,
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`news_title_ner_id`),
  UNIQUE KEY `news_title_ner_id_UNIQUE` (`news_title_ner_id`),
  FOREIGN KEY (news_id) REFERENCES news_db.news_contents(news_id)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8mb4;