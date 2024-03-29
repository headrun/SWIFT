CRAWL_TABLE="""
CREATE TABLE `ebay_crawl` (
  `sk` varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  `search_key` text CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  `end_time` text CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  `crawl_status` int NOT NULL DEFAULT '0',
  `created_at` datetime NOT NULL,
  `modified_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`sk`,`crawl_status`),
  KEY `sk` (`sk`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
"""

META_TABLE = """
CREATE TABLE `ebay_sold_items` (
  `sk` varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  `search_key` varchar(300) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  `item_id` varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  `top_rated` varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  `title` text CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  `location` varchar(512) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  `postal_code` varchar(30) CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  `returns_accepted` varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  `is_multi` varchar(200) CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  `category_id` varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  `category` text CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  `expedited_shipping` text CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  `ship_to_locations` text CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  `shipping_type` text CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  `shipping_service_cost` varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  `shipping_service_currency` varchar(30) CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  `current_price` varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  `current_price_currency` varchar(30) CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  `converted_current_price` varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  `converted_current_price_currency` varchar(30) CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  `selling_state` varchar(200) CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  `condition` varchar(200) CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  `listing_type` varchar(512) CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  `best_offer_enabled` varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  `buy_it_now_available` varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  `start_time` varchar(200) CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  `end_time` varchar(200) CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  `image_url` text CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  `reference_url` text CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  `timestamp` varchar(200) CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  `created_at` datetime NOT NULL,
  `modified_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`sk`, `item_id`),
  UNIQUE KEY `source` (`sk`,`item_id`,`title`(500)),
  KEY `created_at` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
"""
