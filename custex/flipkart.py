import re
import json
import scrapy

class Flipkart(scrapy.Spider):
    name = 'flipkart_spider'
    start_urls = ['https://www.flipkart.com/b-natural-orange-juice/p/itmeuc5ghtyau3xh']

    def parse(self, response):
        data_text = response.xpath('//script[contains(text(), "window.__INITIAL_STATE__")]/text()').extract()
        data_ = ''.join(data_text).replace('\n', '').replace('\t', '').replace(';', '')
        text = ''.join(re.findall('window.__INITIAL_STATE__ = (.*)', data_))
        data = json.loads(text)
        product = data.get('pageDataV4', {}).get('page', {}).get('data', {}).get('10002', [])[1]
        product_data = product.get('widget', []).get('data', {})
        cat = product_data.get('productDetailsAnnouncement', {}).get('action', {}).get('params', {})
        bene = product_data.get('offerInfo', {}).get('value', {}).get('offerGroups', {})[0]
        product_name = product_data.get('titleComponent', {}).get('value', {}).get('title', '')
        quantity = product_data.get('titleComponent', {}).get('value', {}).get('subtitle', '')
        category = cat.get('analyticsData', {}).get('superCategory', '')
        benefits = bene.get('offers', [])[0].get('value', {}).get('description', '')
        number = data.get('pageDataV4', {}).get('page', {}).get('data', {}).get('10005', [])[2]
        pagedata = number.get('widget', {}).get('data', {})
        components = pagedata.get('renderableComponents', {})
        manufacture = pagedata.get('listingManufacturerInfo', {}).get('value', {})
        description = str(components[2].get('value', {}).get('attributes', [])[0].get('values', []))
        address = manufacture.get('detailedComponents', [])[1]
        manufacture_add = ''.join(address.get('value', {}).get('callouts', []))
        country = ''.join(manufacture.get('mappedCards', [])[2].get('values', []))
        packing_units = ''.join(components[0].get('value', {}).get('attributes', [])[0].get('values', ''))
        ingrediants = components[1].get('value', {}).get('attributes', [])
        pack_type = ''.join(ingrediants[5].get('values', ''))
        net_quantity = packing_units +' '+ pack_type
        self_life = ''.join(ingrediants[6].get('values', ''))
        items = ''.join(ingrediants[10].get('values', ''))
        organic = ''.join(ingrediants[7].get('name', ''))
        name = organic+' '+product_name
        data = {'productName':product_name,
                'link':response.url,
                'productName2':name,
                'quantity':quantity,
                'ingredients':items,
                'nutritialInformation':'',
                'typeOfFood':'',
                'address':manufacture_add,
                'shelfLife':self_life,
                'countryOfRegion':country,
                'howToUse':'',
                'netQuantity':net_quantity,
                'productCapcity':quantity,
                'packingType':pack_type,
                'packingUnits':packing_units,
                'description':description,
                'EAN':'',
                'FSSAI':'',
                'units':net_quantity,
                'food':category,
                'benefits':benefits}
