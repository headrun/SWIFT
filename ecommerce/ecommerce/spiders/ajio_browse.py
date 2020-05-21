from urllib.parse import urljoin
from ecommerce.common_utils import *


class ajiospider(EcommSpider):
    name = 'ajio_browse'
    domain_url = "https://www.ajio.com"
    start_urls = []

    def __init__(self, *args, **kwargs):
        super(ajiospider, self).__init__(*args, **kwargs)
        self.headers = {
                'Connection': 'keep-alive',
                'Pragma': 'no-cache',
                'Cache-Control': 'no-cache',
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36',
                'Sec-Fetch-User': '?1',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-Mode': 'navigate',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
        }

    def start_requests(self):
        url = 'https://www.ajio.com/shop/men'
        yield Request(url, callback=self.parse, headers=self.headers)

    def parse(self,response):
        nodes = response.xpath('//ul[@class="level-first false"]/li')
        for node in nodes:
            url = ''.join(node.xpath('./a/@href').extract())
            link =  urljoin(self.domain_url, url) 
            yield Request(link,callback=self.parse_next,meta = {"node" : node,"handle_httpstatus_list":[400]},headers=self.headers)

    def parse_next(self,response): 
        node = response.meta['node']
        urls =  node.xpath('.//div[@class="items"]//span//a//@href').extract()
        for url in urls:
            try:
                category = url.split('-')[0].replace('/','')
            except:
                category = ''
            try:
                sub_category = url.split('-')[1]
            except:
                sub_category = ''
            product_url = urljoin(self.domain_url, url)
            meta_data = {'category':category,'sub_category':sub_category}
            sk = url.split('/')[-1]
            self.get_page('ajio_fashion_terminal',product_url,sk,meta_data=meta_data)
     
