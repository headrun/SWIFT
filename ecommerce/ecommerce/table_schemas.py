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

PRODUCTS_INFO_TABLE_CREATE_QUERY="""
CREATE TABLE `products_info` (
  `hd_id` varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  `source` varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  `sku` varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  `size` varchar(30) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  `title` varchar(512) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  `descripion` text CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  `specs` text CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  `image_url` text CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  `reference_url` text CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  `aux_info` text CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  `created_at` datetime NOT NULL,
  PRIMARY KEY (`hd_id`,`created_at`),
  UNIQUE KEY `source` (`source`,`sku`,`size`,`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
"""

PRODUCSTS_INSIGHTS_TABLE_CREATE_QUERY="""
CREATE TABLE `products_insights` (
  `hd_id` varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  `source` varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  `sku` varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  `size` varchar(30) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  `category` text CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  `sub_category` text CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  `brand` text CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  `ratings_count` int DEFAULT '0',
  `reviews_count` int DEFAULT '0',
  `mrp` float DEFAULT '0',
  `selling_price` float DEFAULT '0',
  `discount_percentage` float DEFAULT '0',
  `is_available` int DEFAULT '0',
  `created_at` datetime NOT NULL,
  PRIMARY KEY (`hd_id`,`created_at`),
  UNIQUE KEY `source` (`source`,`sku`,`size`,`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
"""


CRAWL_TABLE_SELECT_QUERY = 'SELECT sk, url, meta_data FROM %s WHERE content_type="%s" AND crawl_status=0 ORDER BY crawl_type DESC LIMIT %s;'

