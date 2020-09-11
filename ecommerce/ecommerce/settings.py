import os

PROJECT_DIR = os.path.abspath(os.path.join(os.getcwd(), os.pardir))

BOT_NAME = 'ecommerce'

SPIDER_MODULES = ['ecommerce.spiders']
NEWSPIDER_MODULE = 'ecommerce.spiders'

#COOKIES_ENABLED=True

# Obey robots.txt rules
ROBOTSTXT_OBEY = True

# Configure maximum concurrent requests performed by Scrapy (default: 16)
CONCURRENT_REQUESTS = 16
LOG_LEVEL = 'INFO'

DOWNLOAD_DELAY = 1
DOWNLOAD_TIMEOUT = 600
CONCURRENT_REQUESTS_PER_DOMAIN = 16
CONCURRENT_REQUESTS_PER_IP = 16

SPIDER_MIDDLEWARES = {
    'ecommerce.middlewares.SpiderMiddleware': 543,
}

DOWNLOADER_MIDDLEWARES = {
    'ecommerce.middlewares.EcommerceDownloaderMiddleware': 543,
    'ecommerce.middlewares.ProxyMiddleware': 100,
    'scrapy_crawlera.CrawleraMiddleware': 610
}

#PROXY_ENABLED = ['myntra']
DB_HOST = 'localhost'
DB_USERNAME = 'root'
DB_PASSWORD = 'Ecomm@34^$'
URLQ_DATABASE_NAME = 'urlqueue'
LOGS_DIR = '%s/logs/' % PROJECT_DIR

#EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
#}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html

ITEM_PIPELINES = {
   'ecommerce.pipelines.EcommercePipeline': 300,
}



CRAWLERA_PASS = 'Hdrn^2019'
CRAWLERA_USER = 'ankit@headrun.com'
CRAWLERA_PRESERVE_DELAY = True

#AUTOTHROTTLE_ENABLED = True
#AUTOTHROTTLE_START_DELAY = 5
#AUTOTHROTTLE_MAX_DELAY = 60
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
#AUTOTHROTTLE_DEBUG = False

#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = 'httpcache'
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'
