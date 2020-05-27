import re
import json
from ecommerce.common_utils import *
from ecommerce.items import InsightItem, MetaItem


class NowSpider(EcommSpider):
    name = "nnnow_browse"

    def __init__(self, *args, **kwargs):
        super(NowSpider, self).__init__(*args, **kwargs)
        self.category_array = ['men-t-shirts-polos', 'men-casual-shirts', 'men-formal-shirts', 'men-sweatshirts', 'men-jackets', 'men-suits-blazers', 'men-sweaters', 'women-tops-shirts-and-t-shirts','women-kurtas-kurtis','women-shirts', 'women-dresses-and-jumpsuits', 'women-jackets', 'women-sweatshirts', 'women-tunics', 'women-sweaters']
        self.cids = ['men_t_shirts_polos','men_casual_shirts','men_formal_shirts','men_sweatshirts','men_jackets','men_suits_blazers','men_sweaters','women_tops_shirts_and_t_shirts','women_kurtas_kurtis','women_shirts','women_dresses_and_jumpsuits','women_jackets','women_sweatshirts','women_tunics','women_sweaters']
        self.headers = headers = {
                'Connection': 'keep-alive',
                'Pragma': 'no-cache',
                'Cache-Control': 'no-cache',
                'Content-Type': 'application/json',
                'accept': 'application/json',
                'module': 'odin',
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36',
                'bbversion': 'v2',
                'Origin': 'https://www.nnnow.com',
                'Sec-Fetch-Site': 'same-site',
                'Sec-Fetch-Mode': 'cors',
                'Referer': 'https://www.nnnow.com/men-t-shirts-polos',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',

        }

    def start_requests(self):
        for category in self.category_array:
            for cid in self.cids:
                cid_nu = 'tn_'+cid
                data = {"deeplinkurl":"/%s?p=1&cid=%s"%(category,cid_nu)}
                url = 'https://api.nnnow.com/d/apiV2/listing/products'
                meta = {'range': 0, 'page': 1, 'category': category,'cid':cid_nu,"handle_httpstatus_list":[400]}
                yield Request(url, headers=self.headers, callback=self.parse, meta=meta,body=json.dumps(data),method = 'POST')

    def parse(self, response):
        headers = self.headers
        page = response.meta['page']
        request_category = response.meta['category']
        try:
            category = request_category.split('-')[0]
            sub_category = request_category.split('-')[1] + request_category.split('-')[2]
        except:
            category = ''
            sub_category = ''
        cid2 = response.meta['cid']
        source = self.name.split('_')[0]
        meta_data = {'category': category, 'sub_category': sub_category}
        _data = json.loads(response.body.decode('utf-8'))
        products = _data.get('data',{}).get('styles',{}).get('styleList',[])
        for product in products:
            product_url = 'https://www.nnnow.com' + product.get('url','')
            sk =  product_url.split('-')[-1]
            self.get_page('nnnow_fashion_terminal',product_url,sk,meta_data=meta_data)
        if products:
            page += 1
            data = {"deeplinkurl":"/%s?p=%s&cid=%s"%(request_category,page,cid2)}
            url = 'https://api.nnnow.com/d/apiV2/listing/products'
            meta = {'page': page,'category': request_category,'cid':cid2,"handle_httpstatus_list":[400]}
            yield Request(url, headers=self.headers, callback=self.parse, meta=meta,body=json.dumps(data),method = 'POST')

