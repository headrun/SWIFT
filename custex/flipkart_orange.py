import re
import json
import scrapy

class Orange(scrapy.Spider):
    name = 'orange_data'
    start_urls = ['https://www.flipkart.com/appy-apple-juice-drink/p/itmevmnfwsmugdn8?pid=DAJEUARYBHCWFGHR&lid=LSTDAJEUARYBHCWFGHRR7CEIR&marketplace=GROCERY&iid=b87da40d-e52a-40b2-be82-fa76993b745b.DAJEUARYBHCWFGHR.SEARCH']
    handle_httpstatus_list = [500]

    def parse(self, response):
        data_text = response.xpath('//script[contains(text(), "window.__INITIAL_STATE__")]/text()').extract()
        data_ = ''.join(data_text).replace('\n', '').replace(';', '').replace('\t', '')
        text = ''.join(re.findall('window.__INITIAL_STATE__ = (.*)', data_))
        data = json.loads(text)
        pagedata = data.get('pageDataV4', {}).get('page', {}).get('data', {})
        productdata = pagedata.get('10002', [])[1].get('widget', []).get('data', {})
        number = pagedata.get('10005', [])[1].get('widget', {}).get('data', {})
        details = pagedata.get('10005', [])[2].get('widget', {}).get('data', {})
        components = number.get('renderableComponents', {})
        address = number.get('listingManufacturerInfo', {}).get('value', {}) 
        categorydetails = productdata.get('productDetailsAnnouncement', {}).get('action', {})
        product_name = productdata.get('titleComponent', {}).get('value', {}).get('title', '')
        quantity = productdata.get('titleComponent', {}).get('value', {}).get('subtitle', '')
        category = categorydetails.get('params', {}).get('analyticsData', {}).get('superCategory', '')
        benefits = details.get('announcement', {}).get('value', {}).get('title', '')
        description = str(components[2].get('value', {}).get('attributes', [])[0].get('values', []))
        manufacture_add = str(address.get('detailedComponents', [])[1].get('value', {}).get('callouts', []))
        country = str(address.get('mappedCards', [])[2].get('values', []))
        packing_units = ''.join(components[0].get('value', {}).get('attributes', [])[0].get('values', ''))
        ingrediants = components[1].get('value', {}).get('attributes', [])
        pack_type = ''.join(ingrediants[5].get('values', ''))
        net_quantity = packing_units + ' ' + pack_type
        self_life = ''.join(ingrediants[6].get('values', ''))
        items = ''.join(ingrediants[10].get('values', ''))
        organic = ''.join(ingrediants[7].get('name', ''))
        name = organic + ' ' + product_name
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
