import os

PROJECT_DIR = os.path.abspath(os.path.join(os.getcwd(), os.pardir))

BOT_NAME = 'Yocket'

SPIDER_MODULES = ['Yocket.spiders']
NEWSPIDER_MODULE = 'Yocket.spiders'

# Obey robots.txt rules
ROBOTSTXT_OBEY = True

# Configure maximum concurrent requests performed by Scrapy (default: 16)
CONCURRENT_REQUESTS = 8
LOG_LEVEL = 'INFO'

# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
DOWNLOAD_DELAY = 1
DOWNLOAD_TIMEOUT = 600
CONCURRENT_REQUESTS_PER_DOMAIN = 8
CONCURRENT_REQUESTS_PER_IP = 8

# Disable cookies (enabled by default)
COOKIES_ENABLED = True

SPIDER_MIDDLEWARES = {
    'Yocket.middlewares.SpiderMiddleware': 543,
}

DOWNLOADER_MIDDLEWARES = {
    'Yocket.middlewares.YocketDownloaderMiddleware': 543,
    'Yocket.middlewares.ProxyMiddleware': 100
}

ITEM_PIPELINES = {
    'Yocket.pipelines.YocketPipeline': 300,
}

PROXY_ENABLED_SOURCES = ['yocket']
DB_HOST = 'localhost'
DB_USERNAME = 'root'
DB_PASSWORD = 'root'
URLQ_DATABASE_NAME = 'urlqueue'
LOGS_DIR = '%s/logs/' % PROJECT_DIR
