import re
import json
from urllib.parse import urljoin
from ecommerce.common_utils import *
from ecommerce.items import InsightItem, MetaItem

class NextSpider(EcommSpider):
    name = 'next_browse'
    domain_url = "https://www.next.us/en/shop"

    def __init__(self, *args, **kwargs):
        super(NextSpider, self).__init__(*args, **kwargs)
        self.category_array = ['gender-men-productaffiliation-blazersandformaljackets']
        self.headers = headers = {
                    'authority': 'www.next.us',
                    'pragma': 'no-cache',
                    'cache-control': 'no-cache',
                    'accept': '*/*',
                    'x-requested-with': 'XMLHttpRequest',
                    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36',
                    'sec-fetch-site': 'same-origin',
                    'sec-fetch-mode': 'cors',
                    'referer': 'https://www.next.us/en/shop/gender-men-productaffiliation-formalshirts-0',
                    'accept-encoding': 'gzip, deflate, br',
                    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8'}

    def start_requests(self):
        for category in self.category_array:
            url = 'https://www.next.us/en/shop/%s/srt-0'%category
            yield Request(url, callback=self.parse, headers=self.headers,meta={'category':category,'page':0})

    def parse(self, response):
        page = response.meta['page']
        data = response.text
        price_ = ''.join(response.xpath('//div[@class="Options Price"]//script[contains(@type,"text/javascript")]/text()').extract()).replace('\r\n','').replace(';','').strip()
        min_price = ''.join(re.findall('PriceFilterMin = (.*)  PriceFilterMax',price_)).strip()
        max_price = ''.join(re.findall('PriceFilterMax = (.*)  PriceFilterFrom',price_)).strip() 
        category = response.meta['category']
        urls = response.xpath('//h2[@class="Title"]//a//@href').extract()
        for url in urls:
            url = 'https:'+url
            yield Request(url,callback=self.parse_next,headers=self.headers,meta={'category':category,'page':page})    
        if urls:
            page = page+24
            url = 'https://www.next.us/en/shop/%s/srt-%s'%(category,page)
            meta = {'page':page,'category':category}
            yield Request(url,callback = self.parse, meta=meta)
            
    def parse_next(self,response):
        page = response.meta['page']
        source = 'next'
        reviews = ''.join(response.xpath('//div[@class="reviewsContainer Content "]//@data-reviewscount').extract())
        json_data=json.loads(''.join(response.xpath('//script[@type="application/ld+json"]//text()').extract()).replace('\r\n','').strip())
        skuid = json_data.get('sku','')
        currency = json_data.get('offers',{}).get('priceCurrency','')
        review_count= json_data.get('aggregateRating',{}).get('reviewCount','')
        ratings = json_data.get('review',[])
        category = response.meta['category']
        product_category =  category.split('-')[1]
        sub_category = category.split('-')[-1]
        text = ''.join(response.xpath('//script[contains(text(), "var shotData")]/text()').extract()).replace('\r\n', '').replace(';', '').strip()
        data_ = ''.join(re.findall('var shotData = (.*)', text)).strip()
        data = json.loads(data_)
        products = data.get('Styles',[])[0]
        for rating in ratings:
            rating = rating.get('reviewRating',{}).get('ratingValue','')
            fits = products.get('Fits',[])
            for fit in fits:
                brandname = fit.get('Name','')
                items = fit.get('Items',[])
                for item in items:
                    name = item.get('SearchDescription','')
                    imageurl = item.get('Image','')
                    description = item.get('Composition','')
                    product_id = item.get('ItemNumber','')
                    aux_info = {'product_id': product_id, 'json_page': response.url}
                    sizes = item.get('Options','')
                    for size in sizes:
                        number = size.get('Number','')
                        size_ = size.get('Name','')
                        availability = size.get('StockStatus','')
                        price = ''.join(size.get('Price','')).replace('$','')
                        insight_item = InsightItem()
                        hd_id = encode_md5('%s%s%s' % (source, str(skuid), size))
                        insight_item.update({
                            'hd_id': hd_id, 'source': source, 'sku': skuid, 'size': size_,
                            'category':product_category, 'sub_category': sub_category, 'brand': brandname,
                            'ratings_count': '', 'reviews_count': '', 'mrp': price,
                            'selling_price': price, 'currency': currency,
                            'discount_percentage': '', 'is_available': availability
                        })
                        yield insight_item
                        meta_item = MetaItem()
                        meta_item.update({
                            'hd_id': hd_id, 'source': source, 'sku': skuid, 'web_id': product_id, 'size': size,
                            'title': name, 'category':product_category, 'sub_category': sub_category,
                            'brand': brandname, 'rating':'', 'ratings_count': '', 'reviews_count': '',
                            'mrp':price, 'selling_price': price, 'currency': currency,
                            'discount_percentage':'', 'is_available': availability,
                            'descripion': description, 'specs': '', 'image_url': imageurl,
                            'reference_url': response.url, 'aux_info': aux_info
                        })
                        yield meta_item

                        
                        
                        
