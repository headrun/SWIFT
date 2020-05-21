from csv import writer
from datetime import datetime
from scrapy.spiders import XMLFeedSpider
from scrapy.selector import Selector

class EbayBrowse(XMLFeedSpider):
    name = 'ebay_browse'
    start_urls = ['https://svcs.ebay.com/services/search/FindingService/v1?OPERATION-NAME=findCompletedItems&SERVICE-VERSION=1.0.0&SECURITY-APPNAME=mohanave-websitem-PRD-699a952e7-14cc5498&RESPONSE-DATA-FORMAT=XML&REST-PAYLOAD&keywords=1861-O%20Seated%20Half%20Dollar%20Civil%20War']

    def __init__(self, *args, **kwargs):
        super(EbayBrowse, self).__init__(*args, **kwargs)
        self.search_key = kwargs.get('keyword', '')
        self.values = (('Search Keyword', 'Item Id', 'Top RatedListing', 'Title', 'Location', 'Postalcode', 'Return Accepted', 'is MultiVariation Listing', 'Category ID', 'Category', 'Expedited Shipping', 'Ship To Locations', 'Shipping Type', 'Shipping Service Cost_value', 'ShippingServiceCost_currency', 'CurrentPrice_value', 'CurrentPrice_currency', 'ConvertedCurrentPrice_value', 'ConvertedCurrentPrice_currency', 'Selling State', 'Condition', 'ListingType', 'BestOfferEnabled', 'BuyItNowAvailable', 'Start Time', 'End Time', 'Image', 'ItemListingURL', 'Timestamp'))

    def start_request(self):
        url = 'https://svcs.ebay.com/services/search/FindingService/v1?OPERATION-NAME=findCompletedItems&SERVICE-VERSION=1.0.0&SECURITY-APPNAME=mohanave-websitem-PRD-699a952e7-14cc5498&RESPONSE-DATA-FORMAT=XML&REST-PAYLOAD&keywords=%s' % self.search_key
        yield Request(url, callback=self.parse)

    def parse(self, response):
        sel = Selector(response)
        sel.remove_namespaces()
        condition = ''
        nodes = sel.xpath('//searchResult/item')
        for node in nodes:
            _id = ''.join(node.xpath('.//itemId/text()').extract())
            top_rated = ''.join(node.xpath('.//topRatedListing/text()').extract())
            title = ''.join(node.xpath('.//title/text()').extract())
            location = ''.join(node.xpath('.//location/text()').extract())
            postalCode = ''.join(node.xpath('.//postalCode/text()').extract())
            returns_accepted = ''.join(node.xpath('.//returnsAccepted/text()').extract())
            is_multi = ''.join(node.xpath('.//isMultiVariationListing/text()').extract())
            category_id = ''.join(node.xpath('.//categoryId/text()').extract())
            category = ''.join(node.xpath('.//categoryName/text()').extract())
            expedited_shipping = ''.join(node.xpath('.//expeditedShipping/text()').extract())
            ship_to_locations = ''.join(node.xpath('.//shipToLocations/text()').extract())
            shipping_type = ''.join(node.xpath('.//shippingType/text()').extract())
            shipping_service_cost = ''.join(node.xpath('.//shippingServiceCost/text()').extract())
            shipping_service_currency = ''.join(node.xpath('.//shippingServiceCost/@currencyId').extract())
            current_price = ''.join(node.xpath('.//currentPrice/text()').extract())
            current_price_currency = ''.join(node.xpath('.//currentPrice/@currencyId').extract())
            converted_current_price = ''.join(node.xpath('.//convertedCurrentPrice/text()').extract())
            converted_current_price_currency = ''.join(node.xpath('.//convertedCurrentPrice/@currencyId').extract())
            selling_state = ''.join(node.xpath('.//sellingState/text()').extract())
            listing_type = ''.join(node.xpath('.//listingType/text()').extract())
            best_offer_enabled = ''.join(node.xpath('.//bestOfferEnabled/text()').extract())
            buy_it_now_available = ''.join(node.xpath('.//buyItNowAvailable/text()').extract())
            start_time = ''.join(node.xpath('.//startTime/text()').extract())
            end_time = ''.join(node.xpath('.//endTime/text()').extract())
            gallery_url = ''.join(node.xpath('.//galleryURL/text()').extract())
            view_url = ''.join(node.xpath('.//viewItemURL/text()').extract())
            timestamp = datetime.now().strftime("%H:%M.%S")

            self.values.append((search_keyword, _id, top_rated, title, location, postalCode, returns_accepted, is_multi, category_id, category, expedited_shipping, ship_to_locations, shipping_type, shipping_service_cost, shipping_service_currency, current_price, current_price_currency, converted_current_price, converted_current_price_currency, selling_state, condition, listing_type, best_offer_enabled, buy_it_now_available, start_time, end_time, gallery_url, view_url, timestamp))

        with open('ebay_output.csv', 'w+') as _file:
            csv_writer = writer(_file)
            csv_writer.writerows(self.values)
