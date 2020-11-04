import requests
from scrapy.selector import Selector
import datetime
import MySQLdb
import pandas as pd

class EbaySpider():

    headers = {
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

    def __init__(self):
        self.conn = MySQLdb.connect(host='localhost', user='root', passwd= 'P@tN3R#74#$', db='urlqueue', charset='utf8mb4', use_unicode=True)
        self.cursor = self.conn.cursor()
        self.getdata()

    def __del__(self):
        self.cursor.close()
        self.conn.close()

    def getdata(self):
        self.cursor.execute('select sk, search_key, end_time from ebay_crawl')
        key_words = self.cursor.fetchall()
        url = 'https://www.ebay.com/sch/i.html'
        final_result = []
        for key in key_words:
            sku, search_key, end_time = key

            params = (
                    ('_nkw', search_key),
                    ('_in_kw', '1'),
                    ('_ex_kw', ''),
                    ('_sacat', '0'),
                    ('LH_Complete', '1'),
                    ('_udlo', ''),
                    ('_udhi', ''),
                    ('_samilow', ''),
                    ('_samihi', ''),
                    ('_sadis', '15'),
                    ('_stpos', ''),
                    ('_sargn', '-1&saslc=1'),
                    ('_salic', '1'),
                    ('_sop', '12'),
                    ('_dmd', '1'),
                    ('_ipg', '50'),
                    )
            res = Selector(text=requests.get(url, headers=self.headers, params=params).text)
            item_urls = res.xpath('//li[following-sibling::li[@class="lvresult clearfix li"]]//h3[@class="lvtitle"]/a/@href').extract()
            for url in item_urls:
                parsed_data = self.parsedata(url, sku, search_key, end_time)
                if parsed_data:
                    final_result.append(parsed_data)
        df = pd.DataFrame(final_result, columns=['SKU ID', 'Search Key', 'ItemListingURL', 'Title', 'Condition', 'End Time', 'Price', 'Image URL', 'Location', 'Item ID', 'Category', 'Timestamp'])
        df.to_csv('ebay_sold_items.csv', index=False)
    
    def parsedata(self, url, sku, search_key, end_time):
        response_text = requests.get(url, headers=self.headers).text
        item_res = Selector(text=response_text)
        input_end_date = pd.to_datetime(end_time.replace('.000Z', ''), format='%Y-%m-%dT%H:%M:%S')
        prod_url = url
        location = ''.join(item_res.xpath('//div[contains(text(), "Item location:")]/following-sibling::div[1]/text()').extract())
        date_time = ''.join(item_res.xpath('//*[@id="bb_tlft"]/span/span[@class="timeMs"]/@timems').extract())
        if date_time.strip():
            date_time = int(date_time)/1000
            end_date = datetime.datetime.fromtimestamp(date_time).strftime('%Y-%m-%d %H:%M:%S')
        else:
            end_date = ''
        end_date = pd.to_datetime(end_date)
        if (location.lower() == 'china') or (input_end_date > end_date):
            return []
        title = ''.join(item_res.xpath('//*[@id="itemTitle"]/text()').extract())
        condition = ''.join(item_res.xpath('//*[@id="vi-itm-cond"]/text()').extract())
        price = ''.join(item_res.xpath('//div[contains(text(), "Winning bid:")]/following-sibling::div[1]/span/text()').extract()).strip()
        if not price:
            price = ''.join(item_res.xpath('//div[contains(text(), "Price:")]/following-sibling::div[1]/span/text()').extract()).strip()
        img_url = ''.join(item_res.xpath('//*[@id="mainContent"]//table//tbody//tr//td//div//img[@class="img img140"]/@src').extract())
        item_id = ''.join(item_res.xpath('//*[@id="descItemNumber"]/text()').extract())
        category = ''.join(item_res.xpath('//td[@class="attrLabels"][contains(text(), "Brand:")]/following-sibling::td//span[@itemprop="name"]//text()').extract())
        timestamp = datetime.datetime.now().strftime("%H:%M.%S")
        return [sku, search_key, prod_url, title, condition, end_date, price, img_url, location, item_id, category, timestamp]

if __name__ == '__main__':
    obj = EbaySpider()

