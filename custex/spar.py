import scrapy
import csv
from scrapy.selector import Selector

class Spar(scrapy.Spider):
        name = "spar_final"

        def __init__(self, *args, **kwargs):
                super(Spar, self).__init__(*args, **kwargs)
                oupf = open('spar_final.csv', 'w')
                self.csv_file = csv.writer(oupf)
                self.csv_file.writerow(['Product name','Links','product_name_line2','product_name_line3','List of ingredients','Nutritional Information','Veg /Non Veg/NA','Name and Complete address of the Manufacturer','Shelf Life','Country of Origin','How to Use?','Net Quantity','Product capacity','Packing Type','Packing Units','Description','EAN','FSSAI','Units','Food/ Non Food','Benefits','Image Front name','Image Back Name','Image Ingredients','Content Info','Image Nutriteints','Content Info','FSSAI Image'])

        def start_requests(self):
                url = 'https://www.sparindia.com/en/Beverages/Soft-Drinks/Lime-Drink/7-Up-Nimbooz-Lemon-Soft-Drink-350ml/p/100045215'
                yield scrapy.Request(url, callback=self.parse)

        def parse(self, response):
                sel = Selector(response)
                title = ''.join(sel.xpath('//*[@id="productdetaillist"]/div[2]/div[2]/div/div[1]/div[2]/h4//text()').extract())
                prod_url = response.url
                brand = ''.join(sel.xpath('//*[@id="productdetaillist"]/div[2]/div[2]/div/div[1]/div[2]/div/div[1]/div[2]//text()').extract())
                about = sel.xpath('//div[contains(text(),"About Product description")]')
                product_desc = ''.join(about.xpath('../table[1]//tbody//tr//td//text()').extract())
                ingredients = ''.join(about.xpath('../table[2]//tbody//tr//td//text()').extract())
                benefits_desc = sel.xpath('//div[contains(text(),"Benefits")]')
                benefits = ''.join(benefits_desc.xpath('../table//tbody//tr[1]//td//text()').extract()).replace('\n', '').replace('\t', '')
                features = ''.join(benefits_desc.xpath('../table//tbody//tr[2]//td//text()').extract()).replace('\n', '').replace('\t', '')
                usage_desc = sel.xpath('//div[contains(text(),"Usage")]')
                usage = ''.join(usage_desc.xpath('../table//tbody//tr//td//text()').extract()).replace('\n', '').replace('\t', '')
                storage_desc = sel.xpath('//div[contains(text(),"Storage")]')
                storage = ''.join(storage_desc.xpath('../table[2]//tbody//tr[2]//td//text()').extract()).replace('\n', '').replace('\t', '')
                nutri_desc = sel.xpath('//div[contains(text(),"Nutrition")]')
                nutrition = ''.join(nutri_desc.xpath('../table//tbody//tr[1]//td//text()').extract()).replace('\n', '').replace('\t', '')
                additives = ''.join(nutri_desc.xpath('../table//tbody//tr[2]//td//text()').extract()).replace('\n', '').replace('\t', '')
                prod_desc = sel.xpath('//div[contains(text(),"Produce - (EAN)")]')
                ean = ''.join(prod_desc.xpath('../table[1]//tbody/tr/td//text()').extract()).replace('\n', '').replace('\t', '')
                address = ''.join(prod_desc.xpath('../table[8]//tbody/tr/td//text()').extract()).replace('\n', '').replace('\t', '')
                if title:
                        self.csv_file.writerow([repr(title), repr(prod_url), repr(brand), repr(product_desc), repr(ingredients), repr(benefits), repr(features), repr(usage), repr(storage), repr(nutrition), repr(additives), repr(ean), repr(address)])
