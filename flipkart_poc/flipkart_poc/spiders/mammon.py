import scrapy
import json
import pandas as pd
import csv

class Amazon(scrapy.Spider):
    name = "mammon"
    
    def __init__(self, *args, **kwargs):
        super(Amazon, self).__init__(*args, **kwargs)

    def start_requests(self):
        headers = {
            'authority': 'www.amazon.in',
            'pragma': 'no-cache',
            'cache-control': 'no-cache',
            'rtt': '500',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36',
            'content-type': 'application/json; charset=UTF-8',
            'accept': 'application/json, text/plain, */*',
            'sec-fetch-dest': 'empty',
            'downlink': '0.55',
            'ect': '3g',
            'origin': 'https://www.amazon.in',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-mode': 'cors',
            'referer': 'https://www.amazon.in/stores/page/9AD494FE-BAFD-4A17-963D-6F128C4E5142?ingress=0&visitId=afb27578-5c1e-4385-9ce7-421749655f6a&ref_=ast_bln&productGridPageIndex=4',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'cookie': 'session-id=260-5810606-0339803; i18n-prefs=INR; ubid-acbin=260-2657214-8404948; x-wl-uid=175b4BFa16HhwV6lwUnD4ECqVzFIDOQK0Rp8Zpe54/u3N36F17w8ez11VyroT36OT4PdA53Ysy3k=; visitCount=7; session-token=47hAFeS5xOwFQRQaLRwHsuGCUxISdBRRep3Xt+9oj0IFTacse86sZA6hhaJbJA8n5S8XlOs0IyLEJFFQopDd2U8Q8YSL9IaDYxwjln5/H0e3Cj0E7+OvaJphuNqJvhY0g5ntqyJXRpx8+yMmrCDRAw4/uPByUe5YqXwhDW898P4KnmG/ELhxNozjtql2nO4i; csm-hit=tb:s-PEMX0T3DNDMY2S59V210|1590046298657&t:1590046300683&adb:adblk_no; session-id-time=2082758401l',
        }

        data = {
          "requestContext": {
            "obfuscatedMarketplaceId": "A21TJRUUN4KGV",
            "obfuscatedMerchantId": "A1VBAL9TL5WCBF",
            "language": "en-IN",
            "sessionId": "260-5810606-0339803",
            "customerIP": "157.46.240.191",
            "currency": "INR",
            "queryParameterMap": {
              "ingress": [
                "0"
              ],
              "visitId": [
                "afb27578-5c1e-4385-9ce7-421749655f6a"
              ],
              "slashargs": [
                ""
              ],
              "ref_": [
                "ast_bln"
              ],
              "productGridPageIndex": ["5"],
              "pageId": [
                "9AD494FE-BAFD-4A17-963D-6F128C4E5142"
              ]
            },
            "weblabMap": {
              "STORES_182381": "T1",
              "STORES_202826": "T1",
              "STORES_244674": "T1",
              "STORES_246554": "T1",
              "STORES_267445": "T1",
              "STORES_215529": "C",
              "STORES_PAGEID_VALIDATION_270842": "T1",
              "STORES_209062": "T1",
              "STORES_175696": "T1",
              "STORES_AAPI_254262": "T1",
              "STORES_248295": "T1",
              "STORES_183701": "T1",
              "STORES_187401": "T1",
              "STORES_180782": "T1",
              "STORES_239063": "T3"
            },
            "appendedParameters": {
              "ingress": "0",
              "visitId": "2c948b1a-6249-4b76-a257-10a0e075f801"
            },
            "deviceType": "Desktop",
            "ubId": "260-2657214-8404948",
            "slateToken": "djEsLTE5NjQ0NjU5MjIsMTU5MDA0NjI5ODAyMCw2QWFDaXdrdWNRUVg2eEFWcnlTRmVMUmQ2bW9tc2VuQzNFN1Y1Q3Y1UjlLdnZFc3RjSUdqZ1pua2NWMD0=",
            "debug": False,
            "internal": False,
            "profile": False,
            "inBlacklist": False,
            "customerId": ""
          },
          "pageContext": {
            "template": "Blank",
            "pageUrlStatus": "LbrNodeNameBrandName:Match",
            "brandName": "Mammon",
            "storeType": "STORE",
            "defaultPage": False,
            "brandColor": "",
            "entityType": "BRAND",
            "program": "default",
            "urlIdentifier": "",
            "rootPageId": "481F9F8D-6C35-41ED-B87C-22D3BB28E28D",
            "title": "ALL Products",
            "storeId": "9AD494FE-BAFD-4A17-963D-6F128C4E5142",
            "pagePath": "/stores/Mammon/ALLProducts/page/9AD494FE-BAFD-4A17-963D-6F128C4E5142",
            "version": "amzid:faceout:FO1VZZNI3QCV31D-FO3QRU2F71G0ODO",
            "entityName": "Mammon",
            "pageDescription": "ALL Products",
            "lastSubmittedTime": 1578552317780,
            "pageImage": "https://images-na.ssl-images-amazon.com/images/S/abs-image-upload-na/2/AmazonStores/A21TJRUUN4KGV/b4d4eff7102becb106240b270cfac903.w3000.h600.jpg",
            "theme": "diamond",
            "productGridType": "standard",
            "id": "9AD494FE-BAFD-4A17-963D-6F128C4E5142",
            "browseNode": 13317165031,
            "brandLogo": {
              "imageWidth": "603",
              "imageOffsetTop": 0,
              "shape": "square",
              "imageKey": "abs-image-upload-na/0/AmazonStores/A21TJRUUN4KGV/a714a4dca7df8e33744db3a2b1cadbc1.w603.h603.jpg",
              "imageUrl": "https://images-na.ssl-images-amazon.com/images/S/abs-image-upload-na/0/AmazonStores/A21TJRUUN4KGV/a714a4dca7df8e33744db3a2b1cadbc1.w603.h603.jpg",
              "hideBrandLogo": True,
              "assetTags": "",
              "imageOffsetLeft": 0,
              "imageHeight": "603",
              "image": "https://m.media-amazon.com/images/S/abs-image-upload-na/0/AmazonStores/A21TJRUUN4KGV/a714a4dca7df8e33744db3a2b1cadbc1.w603.h603.jpg"
            }
          },
          "widgetType": "ProductGrid",
          "sectionType": "ProductGrid",
          "productGridType": "ap",
          "endpoint": "ajax-data",
          "ASINList" : ['B085B3VPDH', 'B085962CRL', 'B085B4243P', 'B07ZBVP2KX', 'B085B49J53', 'B085985ZBV', 'B085B36GKZ', 'B085B34DZT', 'B0865TR54R', 'B0859FLT9L', 'B0859F931M', 'B0858ZB3NL', 'B084V7LY4Q', 'B07X45PVKH'],
          "displayProductGridHeader": False
        }
        yield scrapy.Request('https://www.amazon.in/juvec', callback=self.product_details,  headers=headers, body=json.dumps(data), method="POST")



    def product_details(self, response):
        data = json.loads(response.body)
        data1 = pd.DataFrame()
        for prod in data.get('products', []):
            sku = prod.get('asin')
            title = prod.get('title', {}).get('displayString')
            features = prod.get('featureBullets', {}).get('featureBullets')
            product_url = prod.get('links', {}).get('viewOnAmazon', {}).get('url')
            mrp = prod.get('buyingOptions', {})[0].get('price',{}).get('basisPrice', {}).get('moneyValueOrRange', {}).get('value', {}).get('amount', {})
            selling_price = prod.get('buyingOptions', {})[0].get('price',{}).get('priceToPay', {}).get('moneyValueOrRange', {}).get('value',{}).get('amount', {})
            img_url = prod.get('productImages', {}).get('images', {})[0].get('hiRes', {}).get('url', {})
            ratings = prod.get('customerReviewsSummary', {}).get('rating', {}).get('displayString', {})
            fields= {'SKU':sku, 'Title':title, 'Features':features, 'Product URL': product_url, 'MRP':mrp, 'Selling Price': selling_price,
            'Image URL': img_url, 'Ratings': ratings}
            temp = pd.DataFrame([fields])
            data1 = data1.append(temp)
        fin_data = data1.to_csv('mammon3.csv', index=False)


