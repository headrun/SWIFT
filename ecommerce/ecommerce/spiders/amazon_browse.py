from urllib.parse import urljoin
from scrapy.selector import Selector
from ecommerce.common_utils import *

class AmazonBrowseV2(EcommSpider):
    name = 'amazon_browse'
    domain_url = 'https://www.amazon.in'
    start_urls = []

    def __init__(self, *args, **kwargs):
        super(AmazonBrowseV2, self).__init__(*args, **kwargs)
        self.headers = {
            "authority": "www.amazon.in",
            "pragma": "no-cache",
            "cache-control": "no-cache",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.129 Safari/537.36",
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "sec-fetch-site": "none",
            "sec-fetch-mode": "navigate",
            "sec-fetch-user": "?1",
            "sec-fetch-dest": "document",
            "accept-language": "en-US,en;q=0.9,fil;q=0.8,te;q=0.7"
        }

    def start_requests(self):
        url = 'https://www.amazon.in/b?node=1571271031'
        yield Request(url, callback=self.parse, headers=self.headers)

    def parse(self, response):
        sel = Selector(response)
        fashion_nodes = get_nodes(sel, '//div[@aria-live="polite"]/li//span[contains(text(), "Women") or contains(text(), "Men")]')
        for fashion_node in fashion_nodes:
            category = extract_data(fashion_node, './text()')
            link = extract_data(fashion_node, './../@href')
            if 'http' not in link:
                link = urljoin(self.domain_url, link)
            yield Request(link, callback=self.parse_sub_categories, headers=self.headers, meta={'category': category})

    def parse_sub_categories(self, response):
        sel = Selector(response)
        sub_category_nodes = get_nodes(sel, '//ul[@class="a-unordered-list a-nostyle a-vertical s-ref-indent-two"]//a')
        for sub_category_node in sub_category_nodes:
            sub_category = extract_data(sub_category_node, './span/text()')
            link = extract_data(sub_category_node, './@href')
            if 'http' not in link:
                link = urljoin(self.domain_url, link)
            response.meta['sub_category'] = sub_category
            yield Request(link, callback=self.parse_listing, headers=self.headers, meta=response.meta)

    def parse_listing(self, response):
        sel = Selector(response)
        category = response.meta['category']
        sub_category = response.meta['sub_category']
        meta_data = {'category': category, 'sub_category': sub_category}
        product_nodes = get_nodes(sel, '//ul[contains(@class, "s-result-list")]/li') or\
                get_nodes(sel, '//div[contains(@class, "s-search-results")]//span[@data-component-type="s-product-image"]')
        for product_node in product_nodes:
            link = extract_data(product_node, './/a[contains(@class, "s-access-detail-page")]/@href') or\
                    extract_data(product_node, './/a/@href')
            sk = link.split('/')[-2]
            if link:
                link = 'https://www.amazon.in/dp/%s' % sk
                if 'http' not in link:
                    link = urljoin(self.domain_url, link)
                self.get_page('amazon_fashions_terminal', link, sk, meta_data=meta_data)

        next_page = extract_data(sel, '//a[@id="pagnNextLink"]/@href') or\
                extract_data(sel, '//a[contains(text(), "Next")]/@href')
        if next_page:
            if 'http' not in next_page:
                next_page = urljoin(self.domain_url, next_page)
            yield Request(next_page, callback=self.parse_listing, headers=self.headers, meta=response.meta)
