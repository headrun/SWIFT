import scrapy
import csv
from scrapy.selector import Selector

class Amazon(scrapy.Spider):
	name = "pantry"

	def __init__(self, *args, **kwargs):
		super(Amazon, self).__init__(*args, **kwargs)
		oupf = open('amazon_pantry.csv', 'w')
		self.csv_file = csv.writer(oupf)
		self.csv_file.writerow(['Product name','Links','brand','product_name_line3','List of ingredients','Nutritional Information','Veg /Non Veg/NA','How to use','Name and Complete address of the Manufacturer','Shelf Life','Country of Origin','Product capacity','Packing Type','Packing Units','Description','EAN','FSSAI','Units','Food/ Non Food','Benefits','Image Front name','Image Back Name','Image Ingredients','Content Info','Image Nutriteints','Content Info','FSSAI Image'])

	def start_requests(self):
		urls = ['https://www.amazon.in/Brooke-Bond-Roses-Dust-100g/dp/B01I3K9DZQ',
				'https://www.amazon.in/24-Mantra-Organic-Mango-Juice/dp/B00DKDY29O',
				'https://www.amazon.in/24-Mantra-Organic-Mixed-Fruit/dp/B00L927R3G'
				'https://www.amazon.in/Brooke-Bond-Roses-Dust-100g/dp/B01I3K9DZQ',
				'https://www.amazon.in/Brooke-Bond-Roses-Dust-Star/dp/B01N1A09H9',
				'https://www.amazon.in/Brooke-Bond-Roses-Natural-Care/dp/B01GCE0J80',
				'https://www.amazon.in/Brooke-Bond-Roses-Natural-Care/dp/B01FW5PZ66',
				'https://www.amazon.in/UP-Nimbooz-Masala-Soda-500ml/dp/B01JAIEYPE',
				'https://www.amazon.in/UP-Lemon-Soft-Drink-Bottle/dp/B01GCDVL9M',
				'https://www.amazon.in/UP-Lemon-Soft-Drink-250/dp/B009VPA1BI',
				'https://www.amazon.in/Soft-Drink-2-25L-Bottle/dp/B0140PQS2O',
				'https://www.amazon.in/Cipla-Immuno-Boosters-Years-Count/dp/B01N5RRGKS',
				'https://www.amazon.in/AloFrut-Kiwi-Aloevera-Juice-1000ml/dp/B07CRH7QY4',
				'https://www.amazon.in/Alofruit-Alofrut-Kiwi-Juice-300ml/dp/B07Q5B4ZWL',
				'https://www.amazon.in/Alo-Frut-Fruit-Litchi-Juice/dp/B017OYWWN2',
				'https://www.amazon.in/Alo-Frut-Fruit-Juice-Litchi/dp/B079DG6BK6',
				'https://www.amazon.in/Alo-Frut-Mixed-Fruit-Juice/dp/B079VRQ4MN',
				'https://www.amazon.in/Alofruit-Alofrut-Orange-Juice-1L/dp/B07Q7KRYB8',
				'https://www.amazon.in/Axiom-Alo-Fruit-Orange-Juice/dp/B078T43GVK',
				'https://www.amazon.in/Axiom-Fruit-Berries-Aloevera-Bottle/dp/B078T3WXXP',
				'https://www.amazon.in/Alo-fruit-Fruit-Guava-Juice/dp/B017OYWUWA'
				'https://www.amazon.in/Amar-Tea-Dust-250g/dp/B017ICUEG2',
				'https://www.amazon.in/Amul-Pro-Chocolate-200ml-Bottle/dp/B018E0HIL6',
				'https://www.amazon.in/Amul-Pro-Refill-Pouch-500g/dp/B00Q8FWR98',
				'https://www.amazon.in/Appy-Apple-Juice-600ml-Bottle/dp/B079L1LF49',
				'https://www.amazon.in/Appy-Apple-Juice-Fizz-Bottle/dp/B01IA0W86A',
				'https://www.amazon.in/Appy-Apple-Juice-250ml-Bottle/dp/B073ZFN6QB',
				'https://www.amazon.in/Parle-Apple-Juice-500ml-Bottle/dp/B00TYGLXXY',
				'https://www.amazon.in/Natural-Juice-Apple-Tetra-Pack/dp/B01DIO6B2K',
				'https://www.amazon.in/Natural-100-Pomegranate-Juice-1L/dp/B06XH4RPPD',
				'https://www.amazon.in/Natural-100-Pomegranate-Juice-1L/dp/B06XH4RPPD',
				'https://www.amazon.in/Natural-Juice-Mango-Carton/dp/B00TFCPFVI',
				'https://www.amazon.in/Big-Soft-Drink-Orange-Bottle/dp/B01LAOGJFY',
				'https://www.amazon.in/Manpasand-Juice-Mango-1-2L-Bottle/dp/B06XX55RZR',
				'https://www.amazon.in/Bindu-Soda-Jeera-Masala-2L/dp/B00XBPO2WS',
				'https://www.amazon.in/Bindu-Soda-Jeera-Masala-Bottle/dp/B00XBPOG4W',
				'https://www.amazon.in/Bisleri-Soda-Fizzy-600ml-Bottle/dp/B01M62O2IY',
				'https://www.amazon.in/Bisleri-Water-Mineral-250ml-Bottle/dp/B01LZTAR2O']
				

		for url in urls:
			yield scrapy.Request(url, callback=self.parse)

	def parse(self, response):
		sel = Selector(response)
		title = ''.join(sel.xpath('//*[@id="productTitle"]/text()').extract()).strip()
		if ',' in title:
			title = title.split(',')
			prod_name = title[0]
			quantity = title[1]
		elif '-' in title:
			title = title.split('-')
			prod_name = title[0]
			quantity = title[1]
		prod_url = response.url
		decl = ''.join(sel.xpath('//div[@id="vnv-container"]//div[@id="text-veg"]//text()').extract()).replace('\n', '').strip()
		prod = ''.join(sel.xpath('//div[@id="feature-bullets"]/ul/li/span/text()').extract()).strip().replace('\n', '').replace('\t', '')
		prod_desc = ''.join(sel.xpath('//*[@id="productDescription"]/p/text()').extract()).replace('\n', '').replace('\t', '')
		country = ''.join(sel.xpath('//*[@id="prodDetails"]/div/div[1]/div/div[2]/div/div/table/tbody/tr[4]/td[2]//text()').extract())
		item_package = ''.join(sel.xpath('//*[@id="prodDetails"]/div/div[1]/div/div[2]/div/div/table/tbody/tr[5]/td[2]//text()').extract())
		unit = ''.join(sel.xpath('//*[@id="prodDetails"]/div/div[1]/div/div[2]/div/div/table/tbody/tr[8]/td[2]//text()').extract())
		if title:
			self.csv_file.writerow([repr(prod), repr(prod_url), repr(quantity), repr(decl), repr(prod), repr(prod_desc), repr(country), repr(item_package), repr(unit)])
