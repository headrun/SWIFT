# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from os import path, getcwd
from random import choice
from inspect import isgenerator
from w3lib.http import basic_auth_header
from scrapy import signals
from scrapy.utils.project import get_project_settings

settings = get_project_settings()

class EcommerceSpiderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, dict or Item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request, dict
        # or Item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class EcommerceDownloaderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class SpiderMiddleware(object):

    def process_spider_output(self, response, result, spider):
        if isgenerator(result):
            result = list(result)
            _result = []
            for r in result:
                if isinstance(r, (list, tuple)):
                    _result.extend(r)
                else:
                    _result.append(r)
            result = _result

        return result

class ProxyMiddleware(object):

    PROXY_ENABLED_SOURCES = settings.get('PROXY_ENABLED_SOURCES', [])

    def process_request(self, request, spider):
        source = spider.name.split('_')[0]
        if source in self.PROXY_ENABLED_SOURCES:
            proxy_path = path.abspath(path.join(getcwd(), '../proxies.txt'))
            with open(proxy_path, 'r') as proxy_file:
                proxy_list = proxy_file.readlines()

            user_agent_path = path.abspath(path.join(getcwd(), '../user_agents.txt'))
            with open(user_agent_path, 'r') as user_agent_file:
                user_agent_list = user_agent_file.readlines()

            proxy, port = choice(proxy_list).strip().split(':')
            request.meta['proxy'] = 'https://%s:%s' % (proxy, port)
            print(request.meta['proxy'])
            #request.headers['Proxy-Authorization'] = basic_auth_header('hr@headrun.com','hdrn^123!')
            request.headers['Proxy-Authorization'] = basic_auth_header('lum-customer-headrunmain-zone-static-country-in', 'v4iey84gn7b7')
            request.headers['User-Agent'] = choice(user_agent_list)
