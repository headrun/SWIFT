CRAWL_TABLE_CREATE_QUERY="""
CREATE TABLE `#CRAWL-TABLE#` (
  `sk` varchar(100) COLLATE utf8_unicode_ci NOT NULL,
  `url` text COLLATE utf8_unicode_ci,
  `crawl_type` varchar(20) COLLATE utf8_unicode_ci NOT NULL,
  `content_type` varchar(25) COLLATE utf8_unicode_ci NOT NULL,
  `crawl_status` int(11) NOT NULL DEFAULT '0',
  `meta_data` text COLLATE utf8_unicode_ci,
  `created_at` datetime NOT NULL,
  `modified_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`sk`,`content_type`,`crawl_status`),
  KEY `sk` (`sk`),
  KEY `type` (`crawl_type`),
  KEY `type_time` (`crawl_type`,`modified_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
"""

CRAWL_TABLE_SELECT_QUERY = 'SELECT sk, url, meta_data FROM %s WHERE content_type="%s" AND crawl_status=0 ORDER BY crawl_type DESC LIMIT %s;'

