from urllib.parse import urljoin
from scrapy.selector import Selector
from ecommerce.common_utils import EcommSpider,\
    get_nodes, extract_data, Request


class AmazonBrowseV2(EcommSpider):
    name = 'amazon_browse'
    domain_url = 'https://www.amazon.in'
    start_urls = []

    def __init__(self, *args, **kwargs):
        super(AmazonBrowseV2, self).__init__(*args, **kwargs)
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
        urls = ['https://www.amazon.in/gp/browse.html?node=1968024031&ref_=nav_em_0_2_10_2_sbc_mfashion_clothing',
                'https://www.amazon.in/gp/browse.html?node=1953602031&ref_=nav_em_0_2_11_2_sbc_wfashion_clothing']
        for url in urls:
            yield Request(url, callback=(self.parse), headers=(self.headers))

    def parse(self, response):
        sel = Selector(response)
        robot_check = extract_data(sel, '//title[contains(text(), "Robot")]/text()')
        if robot_check:
            print('Retrying')
            yield Request(response.url, callback=self.parse, headers=self.headers, meta=response.meta, dont_filter=True)

        else:
            category = extract_data(sel, '//h4[@class="a-size-small a-color-base a-text-bold"]/text()')
            sub_categories = ['T-Shirts & Polos', 'Shirts', 'Jeans', 'Western Wear', 'Ethnic Wear']
            fashion_nodes = get_nodes(sel, '//ul[contains(@class, "indent-two")]/div[@aria-live="polite"]/li')
            for fashion_node in fashion_nodes:
                sub_category = extract_data(fashion_node, './/a//text()')
                link = extract_data(fashion_node, './/a/@href')
                if '₹' in sub_category or '%' in sub_category or sub_category not in sub_categories:
                    continue

                if link:
                    if 'http' not in link:
                        link = urljoin(self.domain_url, link)
                    meta = {'category': category, 'sub_category': sub_category}
                    yield Request(link, callback=self.parse_sub_categories, headers=self.headers, meta=meta)

    def parse_sub_categories(self, response):
        sel = Selector(response)
        robot_check = extract_data(sel, '//title[contains(text(), "Robot")]/text()')
        if robot_check:
            print('Retrying')
            yield Request(response.url, callback=self.parse_sub_categories, headers=self.headers, meta=response.meta, dont_filter=True)

        else:
            meta_data = {'category': response.meta['category'], 'sub_category': response.meta['sub_category']}
            product_nodes = get_nodes(sel, '//ul[contains(@class, "s-result-list")]/li') or\
                get_nodes(sel, '//div[contains(@class, "s-result-list")]//div[@data-component-type="s-search-result"]')
            for product_node in product_nodes:
                sk = extract_data(product_node, './@data-asin')
                link = urljoin('https://www.amazon.in/dp/', '%s?th=1&psc=1' % sk)
                self.get_page('amazon_fashions_terminal', link, sk, meta_data=meta_data)

            sub_category_nodes = get_nodes(sel, '//ul[contains(@class, "indent-two")]/div[@aria-live="polite"]/li')
            for sub_category_node in sub_category_nodes:
                sub_category = extract_data(sub_category_node, './/a//text()')
                link = extract_data(sub_category_node, './/a/@href')
                if '₹' in sub_category or '%' in sub_category:
                    continue

                if link:
                    if 'http' not in link:
                        link = urljoin(self.domain_url, link)

                    meta_data.update({'sub_category': sub_category})
                    yield Request(link, callback=self.parse_sub_categories, headers=self.headers, meta=meta_data)

            next_page = extract_data(sel, '//a[@title="Next Page"]/@href') or\
                extract_data(sel, '//li[@class="a-last"]/a[contains(text(), "Next")]/@href')
            if next_page:
                if 'http' not in next_page:
                    next_page = urljoin(self.domain_url, next_page)
                yield Request(next_page, callback=self.parse_sub_categories, headers=self.headers, meta=meta_data)
