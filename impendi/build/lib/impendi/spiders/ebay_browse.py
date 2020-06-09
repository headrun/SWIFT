from ast import literal_eval
from datetime import datetime
from scrapy.http import Request
from pydispatch import dispatcher
from scrapy import signals
from scrapy.selector import Selector
from scrapy.spiders import XMLFeedSpider
from impendi.items import EbayItem
from impendi.common_utils import get_nodes,\
    extract_data, get_queries_file, move_file,\
    create_default_dirs, QUERY_FILES_PROCESSING_DIR,\
    update_urlqueue_with_resp_status, close_mysql_connection

class EbayBrowse(XMLFeedSpider):
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
        self.url = 'https://svcs.ebay.com/services/search/FindingService/v1?OPERATION-NAME=findCompletedItems&SERVICE-VERSION=1.0.0&SECURITY-APPNAME=%s&RESPONSE-DATA-FORMAT=XML&REST-PAYLOAD&keywords=%s&paginationInput.pageNumber=%s'
        self.queries_file = get_queries_file(self.name)
        dispatcher.connect(self.spider_closed, signals.spider_closed)

    def spider_closed(self):
        self.queries_file.close()
        close_mysql_connection()
        move_file(self.queries_file.name, QUERY_FILES_PROCESSING_DIR)

    def start_requests(self):
        for crawl_list in self.crawl_list:
            source_key, search_key, search_token = crawl_list
            update_urlqueue_with_resp_status(self.source, 9, source_key)
            url = self.url % (search_token, search_key, 1)
            meta = {
                'crawl_check': 1, 'source_key':source_key,
                'search_key': search_key, 'search_token': search_token
            }
            yield Request(url, callback=self.parse, meta=meta)

    def parse(self, response):
        source_key = response.meta['source_key']
        search_key = response.meta['search_key']
        search_token = response.meta['search_token']
        crawl_check = response.meta['crawl_check']
        sel = Selector(response)
        sel.remove_namespaces()
        nodes = get_nodes(sel, '//searchResult/item')
        for node in nodes:
            ebay_item = EbayItem()
            ebay_item.update({
                'source_key': source_key,
                'search_key': search_key,
                'item_id': extract_data(node, './/itemId/text()'),
                'top_rated': extract_data(node, './/topRatedListing/text()'),
                'title': extract_data(node, './/title/text()'),
                'location': extract_data(node, './/location/text()'),
                'postal_code': extract_data(node, './/postalCode/text()'),
                'returns_accepted': extract_data(node, './/returnsAccepted/text()'),
                'is_multi': extract_data(node, './/isMultiVariationListing/text()'),
                'category_id': extract_data(node, './/categoryId/text()'),
                'category': extract_data(node, './/categoryName/text()'),
                'expedited_shipping': extract_data(node, './/expeditedShipping/text()'),
                'ship_to_locations' : extract_data(node, './/shipToLocations/text()'),
                'shipping_type': extract_data(node, './/shippingType/text()'),
                'shipping_service_cost': extract_data(node, './/shippingServiceCost/text()'),
                'shipping_service_currency': extract_data(node, './/shippingServiceCost/@currencyId'),
                'current_price': extract_data(node, './/currentPrice/text()'),
                'current_price_currency': extract_data(node, './/currentPrice/@currencyId'),
                'converted_current_price': extract_data(node, './/convertedCurrentPrice/text()'),
                'converted_current_price_currency': extract_data(node, './/convertedCurrentPrice/@currencyId'),
                'selling_state': extract_data(node, './/sellingState/text()'),
                'listing_type': extract_data(node, './/listingType/text()'),
                'best_offer_enabled': extract_data(node, './/bestOfferEnabled/text()'),
                'buy_it_now_available': extract_data(node, './/buyItNowAvailable/text()'),
                'start_time': extract_data(node, './/startTime/text()'),
                'end_time': extract_data(node, './/endTime/text()'),
                'image_url': extract_data(node, './/galleryURL/text()'),
                'item_url': extract_data(node, './/viewItemURL/text()'),
                'condition': extract_data(node, './/condition/conditionDisplayName/text()'),
                'timestamp': datetime.now().strftime("%H:%M.%S")
            })

            if ebay_item['location'] and 'china' not in ebay_item['location'].lower():
                yield ebay_item

        pagination = int(extract_data(sel, '//paginationOutput/totalPages/text()'))
        if pagination > 1:
            for page in range(1, pagination+1):
                url = self.url % (search_token, search_key, page)
                response.meta.update({'crawl_check': 0})
                yield Request(url, callback=self.parse, meta=response.meta)

        if crawl_check:
            total_entries = int(extract_data(sel, '//paginationOutput/totalEntries/text()'))
            if total_entries:
                update_urlqueue_with_resp_status(self.source, 1, source_key)
            else:
                update_urlqueue_with_resp_status(self.source, 2, source_key)
