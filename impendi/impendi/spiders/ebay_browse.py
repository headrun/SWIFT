from ast import literal_eval
import datetime
from pytz import UTC
from dateutil.parser import parse
from pydispatch import dispatcher
from scrapy import signals
from scrapy.http import Request
from scrapy.selector import Selector
from scrapy.spiders import XMLFeedSpider
from impendi.items import EbayItem
from impendi.common_utils import get_nodes,\
    extract_data, get_queries_file, move_file,\
    create_default_dirs, QUERY_FILES_PROCESSING_DIR,\
    update_urlqueue_with_resp_status, close_mysql_connection
import urllib
import pandas as pd
import re

class EbayBrowse(XMLFeedSpider):
    handle_httpstatus_list = [500, 404]
    name = 'ebay_browse'
    start_urls = []

    def __init__(self, *args, **kwargs):
        super(EbayBrowse, self).__init__(*args, **kwargs)
        create_default_dirs()
        self.source = self.name.split('_')[0]
        '''
        self.source_key = kwargs.get('source_key', '')
        self.search_key = kwargs.get('search_key', '')
        self.search_token = kwargs.get('search_token', '')
        '''
        self.crawl_list = literal_eval(kwargs.get('jsons', '[]'))
        self.queries_file = get_queries_file(self.name)
        self.headers = {
        'authority': 'www.ebay.com',
        'pragma': 'no-cache',
        'cache-control': 'no-cache',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.89 Safari/537.36',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-user': '?1',
        'sec-fetch-dest': 'document',
        'referer': 'https://www.ebay.com/sch/ebayadvsearch',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'cookie': 'DG_ZID=60E7EB46-B86A-324E-9923-A23B8FDC5EFB; DG_ZUID=C46F744E-42CE-3A9A-9007-BF4C57BC1C57; DG_HID=23D4357D-D87A-311F-9F1D-97BBBEA13AF6; DG_SID=157.44.98.56:aMZEW6IBzXM+w0BCOka85EBLLRFMzVZFwfqmo0JC5iA; DG_IID=80EA0D19-A379-3776-8CCD-034DEFA8E74D; DG_UID=82370B72-2A0D-39E1-8454-8D258BEE353C; __gads=ID=60b006b0192475c1:T=1603786670:S=ALNI_Mb1Xwek5-yDNx8xpT0MgtT-octY8g; ak_bmsc=CE10FE38FD412A9A8E45AB7294563582172F9574E2340000369E9B5F9CCC5959~plWvDWye1QiR2aAGV8IlRFd9FyycpHtzkyMNIFhFozeB+wHN4UUCuST06KOyBUX1e5+vTy2Ob6aab4nKn8r8p75VbYGuePY5ieFb3lU8gSeHtZsCoEsWyTIQfkMiMViz1PYMDvPt4JWaQZlIXcdzaGuWvq1LMDPkiV3WTBp2/xQJVa/pj9kvRJBVQ9D9RgHFtqMWXKflMXB+VGYVckazuKGcl51wMYPQ2c+bPjUBP0EdE=; JSESSIONID=A5D67F063BB6CB423B4C7F0DAF0FCA8F; ns1=BAQAAAXQoEGjGAAaAANgASmF81eBjNjl8NjAxXjE2MDM3ODY2NTUwMjJeXjFeM3wyfDV8NHw3fDExXl5eNF4zXjEyXjEyXjJeMV4xXjBeMV4wXjFeNjQ0MjQ1OTA3NWFOy9FmJW+fIULabjUw4fy3KQRE; s=CgADuALFfnPPTMwZodHRwczovL3d3dy5lYmF5LmNvbS9zY2gvaS5odG1sP19zYWNhdD0wJl91ZGxvPSZfdWRoaT0mX3NhbWlsb3c9Jl9zYW1paGk9Jl9zYWRpcz0xNSZfc3Rwb3M9Jl9zb3A9MTImX2RtZD0xJl9pcGc9NTAmX2Zvc3JwPTEmX25rdz1GdW5rbyUyMFBvcCElMjBLaGFsJTIwS2hhbGVlc2klMjBhbmQlMjBSaGFlZ2FsAPgAIF+c89M2OTIwNjk4MjE3NTBhNjg4Yjg2ZDA0MTVmZmIzMTdkMDzY7GY*; nonsession=BAQAAAXQoEGjGAAaAAAgAHF/DL2AxNjAzNzg3ODc5eDMzMzc2MDc1MDg4MHgweDJZADMACmF81eA1MDAwMDksVVNBAMoAIGNeCWA2OTIwNjk4MjE3NTBhNjg4Yjg2ZDA0MTVmZmIzMTdkMADLAAJfm6loMTIpK/SzNhdY7Euxp7V2VC+MdtEfXA**; npii=btguid/692069821750a688b86d0415ffb317d0635e0960^cguid/77ee5fdd1750ac3d9e847dd4ecc1d4cd635e0960^; bm_sv=EF04A2C340F41EE07FED131EBC204807~1Oh3Ibe5NOVLNO+S80ClN9DBpMH7VI8UuwCNdb1TCM7+dv9K7cUzXBR3Cfn0WG7J+5Eg/zCTQRKl9ArfAi41rnFKXAP0G9A1QUH2MlRQS2r7xlwYTv5HxyqcwGllcEcr39zDsgVTn0qKGRTRMx/xLR6ASLdAhmmtenoN1/ECreQ=; ebay=%5Ejs%3D1%5Esbf%3D%2301000000000010000100110%5Epsi%3DA4hCOBO0*%5E; ds2=sotr/b7pQ8zQkzzzz^; dp1=bu1p/QEBfX0BAX19AQA**635e0960^pbf/#8000e00000010002000000635e096b^tzo/-14a635e0971^bl/IN635e0960^',
    }

        dispatcher.connect(self.spider_closed, signals.spider_closed)

    def spider_closed(self):
        self.queries_file.close()
        close_mysql_connection()
        move_file(self.queries_file.name, QUERY_FILES_PROCESSING_DIR)

    def start_requests(self):
        for crawl_list in self.crawl_list:
            source_key, search_key, end_time, search_token = crawl_list
            search_key = search_key.replace('&', ' and ')
            update_urlqueue_with_resp_status(self.source, 9, source_key)
            url = 'https://www.ebay.com/sch/i.html?'
            param = {'_nkw': search_key, '_in_kw': '1', '_ex_kw': '', '_sacat': '0', 'LH_Complete': '1', '_udlo': '',  '_udhi': '', '_samilow': '', '_samihi': '', '_sadis': '15', '_stpos': '', '_sargn': '-1&saslc=1', '_salic': '1', '_sop': '12', '_dmd': '1', '_ipg': '50'}

            url = url +  urllib.parse.urlencode(param)
            meta = {
                'crawl_check': 1, 'source_key':source_key, 'end_time': end_time,
                'search_key': search_key
            }
            yield Request(url, callback=self.parse, meta=meta, headers=self.headers)

    def parse(self, response):
        sel= Selector(response)
        item_urls = []
        item_urls1 = sel.xpath('//li[following-sibling::li[@class="lvresult clearfix li"]]//h3[@class="lvtitle"]/a/@href').extract()
        item_urls2 = sel.xpath('//ul//li//h3[@class="lvtitle"]//a//@href').extract()
        next_page = "".join(sel.xpath('//td[@class="pagn-next"]//a[@aria-label="Next page of results"]//@href').extract())
        if next_page:
            item_urls = item_urls2
        else:
            item_urls = item_urls1
        for item_url in item_urls:
            yield Request(item_url, callback=self.parsedata, headers = self.headers, meta = response.meta, dont_filter=True)
        next_page = "".join(sel.xpath('//td[@class="pagn-next"]//a[@aria-label="Next page of results"]//@href').extract())
        if next_page and '_pgn=' in next_page and 'www.ebay.com/sch/i.html?' in next_page:
            yield Request(next_page, callback=self.parse, headers = self.headers, meta = response.meta)

    def parsedata(self, response):
        source_key = response.meta['source_key']
        search_key = response.meta['search_key']
        end_time = response.meta['end_time']
        crawl_check = response.meta['crawl_check']
        item_res = Selector(response)
        try:
            input_end_date = pd.to_datetime(end_time.replace('.000Z', ''), format='%Y-%m-%dT%H:%M:%S')
        except:
            input_end_date = end_time.replace('.000Z', '')
        prod_url = response.url
        location = ''.join(item_res.xpath('//div[contains(text(), "Item location:")]/following-sibling::div[1]/text()').extract())
        date_time = ''.join(item_res.xpath('//*[@id="bb_tlft"]/span/span[@class="timeMs"]/@timems').extract())
        if date_time.strip():
            date_time = int(date_time)/1000
            end_date = datetime.datetime.fromtimestamp(date_time).strftime('%Y-%m-%d %H:%M:%S')
        else:
            end_date = ''
        end_date = pd.to_datetime(end_date)
        if '0000' not in str(end_time) and '0000' not in str(input_end_date):
            if (location.lower() == 'china') or (input_end_date > end_date):
                return []
        title = ''.join(item_res.xpath('//*[@id="itemTitle"]/text()').extract())
        condition = ''.join(item_res.xpath('//*[@id="vi-itm-cond"]/text()').extract())
        price = ''.join(item_res.xpath('//div[contains(text(), "Winning bid:")]/following-sibling::div[1]/span/text()').extract()).strip()
        if not price:
            price = ''.join(item_res.xpath('//div[contains(text(), "Price:")]/following-sibling::div[1]/span/text()').extract()).strip()
            if not price:
                price = "".join(item_res.xpath('//span[@class="notranslate vi-VR-cvipPrice"]//text()').extract())
        img_url = ''.join(item_res.xpath('//*[@id="mainContent"]//table//tbody//tr//td//div//img[@class="img img140"]/@src').extract())
        item_id = ''.join(item_res.xpath('//*[@id="descItemNumber"]/text()').extract())
        category = ''.join(item_res.xpath('//td[@class="attrLabels"][contains(text(), "Brand:")]/following-sibling::td//span[@itemprop="name"]//text()').extract())
        item_id = "".join(item_res.xpath('//div[@class="vi-cvip-dummyPad20"]//div//div[@id="vi-desc-maincntr"]//div[@id="descItemNumber"]//text()').extract())
        category_id = "".join(item_res.xpath('//li[@itemprop="itemListElement"]//a[@itemprop="item"][@class="scnd"]//@href').extract())
        shiping_type = "".join(item_res.xpath('//div[@class="u-flL sh-col"]//span[@id="shSummary"]//a[@class="si-pd sh-nwr"]//text()').extract())
        shipping_cost = "".join(item_res.xpath('//span[@id="shSummary"]//span[@id="fshippingCost"]//span//text()').extract())
        expedited_shipping = "".join(item_res.xpath('//span[@id="fShippingSvc"]/text()').extract())
        if expedited_shipping:
            expedited_shipping = 'true'
        else:
            expedited_shipping = 'false'
        if category_id:
            category_id = "".join(re.findall('/(\d+)', category_id))
        search_key = response.meta['search_key']
        if title:
            ebay_item = EbayItem()
            ebay_item.update({
                'source_key': source_key,
                'search_key': search_key,
                'item_id': item_id,
                'top_rated': '0', #extract_data(node, './/topRatedListing/text()'),
                'title': title,
                'location': location,
                'postal_code': '',
                'returns_accepted': '',#extract_data(node, './/returnsAccepted/text()'),
                'is_multi': '', #extract_data(node, './/isMultiVariationListing/text()'),
                'category_id': category_id,
                'category': category,
                'expedited_shipping': expedited_shipping,
                'ship_to_locations' : '', #extract_data(node, './/shipToLocations/text()'),
                'shipping_type': shiping_type,
                'shipping_service_cost': shipping_cost,
                'shipping_service_currency': '', #extract_data(node, './/shippingServiceCost/@currencyId'),
                'current_price': price,
                'current_price_currency': '', #extract_data(node, './/currentPrice/@currencyId'),
                'converted_current_price': '', #extract_data(node, './/convertedCurrentPrice/text()'),
                'converted_current_price_currency': '', #extract_data(node, './/convertedCurrentPrice/@currencyId'),
                'selling_state': '', #extract_data(node, './/sellingState/text()'),
                'listing_type': '', #extract_data(node, './/listingType/text()'),
                'best_offer_enabled': '', #extract_data(node, './/bestOfferEnabled/text()'),
                'buy_it_now_available': '', #extract_data(node, './/buyItNowAvailable/text()'),
                'start_time': '', #extract_data(node, './/startTime/text()'),
                'end_time': end_date,
                'image_url': img_url,
                'item_url': response.url,
                'condition': condition,
                'timestamp': datetime.datetime.now().strftime("%H:%M.%S"),
                'created_at' : datetime.datetime.now().replace(microsecond=0),
                'modified_at' : datetime.datetime.now().replace(microsecond=0)
                })
            yield ebay_item
            if title:
                update_urlqueue_with_resp_status(self.source, 1, source_key)
            else:
                update_urlqueue_with_resp_status(self.source, 2, source_key)
