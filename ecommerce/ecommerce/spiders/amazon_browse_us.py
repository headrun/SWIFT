from urllib.parse import urljoin
from scrapy.selector import Selector
from ecommerce.common_utils import EcommSpider,\
    get_nodes, extract_data, Request


class AmazonUSBrowse(EcommSpider):
    name = 'amazon.com_browse'
    domain_url = 'https://www.amazon.com'
    start_urls = []

    def __init__(self, *args, **kwargs):
        super(AmazonUSBrowse, self).__init__(*args, **kwargs)
        self.headers = {
            'authority': 'www.amazon.in',
            'pragma': 'no-cache',
            'cache-control': 'no-cache',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.129 Safari/537.36',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'sec-fetch-site': 'none',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-user': '?1',
            'sec-fetch-dest': 'document',
            'accept-language': 'en-US,en;q=0.9,fil;q=0.8,te;q=0.7'
        }

    def start_requests(self):
        urls = ['https://www.amazon.com/s?k=t-shirt&i=fashion-mens-clothing']
        for url in urls:
            yield Request(url, callback=(self.parse), headers=(self.headers))

    def parse(self, response):
        sel = Selector(response)
        robot_check = extract_data(sel, '//title[contains(text(), "Robot")]/text()')
        if robot_check:
            print('Retrying')
            yield Request(response.url, callback=self.parse, headers=self.headers, meta=response.meta, dont_filter=True)

        else:
            category_nodes = get_nodes(sel, './/li[contains(@class, "a-spacing-micro s-navigation-indent-2")]')
            for category_node in category_nodes:
                category = extract_data(category_node, './span/a/span/text()')
                category_id = extract_data(category_node, './@id')
                category_link = extract_data(category_node, './span/a/@href')
                if category_link:
                    if 'http' not in category_link:
                        category_link = urljoin(self.domain_url, category_link)
                meta = {'category': category, 'category_id': category_id, 'category_link': category_link}
                yield Request(category_link, callback=self.parse_categories, headers=self.headers, meta=meta)
                 
    def parse_categories(self, response):
        sel = Selector(response)
        robot_check = extract_data(sel, '//title[contains(text(), "Robot")]/text()')
        if robot_check:
            print('Retrying')
            yield Request(response.url, callback=self.parse_sub_categories, headers=self.headers, meta=response.meta, dont_filter=True)

        else:
            meta_data = {'category': response.meta['category'], 'category_id': response.meta['category_id'], 'category_link': response.meta['category_link']}
            sub_category_nodes = get_nodes(sel, './/li[contains(@class, "a-spacing-micro s-navigation-indent-2")]')
            for sub_category_node in sub_category_nodes:
                sub_category = extract_data(sub_category_node, './span/a/span/text()')
                sub_category_id = extract_data(sub_category_node, './@id')
                sub_category_link = extract_data(sub_category_node, './span/a/@href')
                if sub_category_link:
                    if 'http' not in sub_category_link:
                        sub_category_link = urljoin(self.domain_url, sub_category_link)
                meta_data.update({'sub_category': sub_category, 'sub_category_id': sub_category_id, 'sub_category_link': sub_category_link})
                yield Request(sub_category_link, callback=self.parse_sub_categories, headers=self.headers, meta=meta_data)

    def parse_sub_categories(self, response):
        sel = Selector(response)
        robot_check = extract_data(sel, '//title[contains(text(), "Robot")]/text()')
        if robot_check:
            print('Retrying')
            yield Request(response.url, callback=self.parse_sub_categories, headers=self.headers, meta=response.meta, dont_filter=True)

        else:
            meta_data = {'category': response.meta['category'], 'category_id': response.meta['category_id'], 'category_link': response.meta['category_link'], 'sub_category': response.meta['sub_category'], 'sub_category_id': response.meta['sub_category_id'], 'sub_category_link': response.meta['sub_category_link']}
            product_nodes = get_nodes(sel, '//ul[contains(@class, "s-result-list")]/li') or \
            get_nodes(sel, '//div[contains(@class, "s-result-list")]//div[@data-component-type="s-search-result"]')
            for product_node in product_nodes:
                product_id = extract_data(product_node, './@data-asin')
                link = urljoin('https://www.amazon.com/dp/', '%s?th=1&psc=1' % product_id)
            next_page = extract_data(sel, '//a[@title="Next Page"]/@href') or extract_data(sel, '//li[@class="a-last"]/a[contains(text(), "Next")]/@href')
            if next_page:
                if 'http' not in next_page:
                    next_page = urljoin(self.domain_url, next_page)
                yield Request(next_page, callback=self.parse_sub_categories, headers=self.headers, meta=meta_data)
