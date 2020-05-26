import scrapy
import csv
from scrapy.selector import Selector


class BigBasket(scrapy.Spider):
	name = "bindu"
	def start_requests(self):
		urls = ['https://www.bigbasket.com/pd/40019291/bindu-packaged-drinking-water-1-l-bottle/']
		for url in urls:
			yield scrapy.Request(url, callback=self.parse)

	def parse(self, response):
		data = {}
		sel = Selector(response)
		link = response.url
		title = sel.xpath('//*[@id="title"]/h1[@class="GrE04"]//text()').extract()
		title = title[0].strip()
		product_name_line2 = title.split(',')[0]
		product_name_line3 = title.split(',')[1].strip()
		other_info = ''.join((sel.xpath('//*[@id="about_0"]/div[2]/div/div/text()')).extract()).strip()
		List_of_ingredients = ''
		Nutritional_Information = ''
		Declaration_regarding_Veg_Non_Veg = ''
		Name_and_Complete_address_of_the_Manufacturer = other_info
		Shelf_Life = ''
		Country_of_Origin = 'India'
		How_to_Use = ''
		Net_Quantity = ''
		Product_capacity = product_name_line3
		Packing_Type = ''
		Packing_Units = ''
		Description = ''
		EAN = ''
		FSSAI = ''
		Units = ''
		Food_Non_Food = ''
		Benefits = ''
		Image_Front_name = ''
		Image_Back_Name = ''
		Image_Ingredients_Content_Info = ''
		Image_Nutriteints_Content_Info = ''
		FSSAI_Image = ''
		data = {'Product name': title, 'Links': link, 'product_name_line2': product_name_line2, 'product_name_line3': product_name_line3, 'List of ingredients': List_of_ingredients, 'Nutritional Information': Nutritional_Information, 'Declaration regarding Veg /Non Veg/NA': Declaration_regarding_Veg_Non_Veg, 'Name and Complete address of the Manufacturer': Name_and_Complete_address_of_the_Manufacturer, 'Shelf Life': Shelf_Life, 'Country of Origin': Country_of_Origin, 'How to Use?': How_to_Use, 'Net Quantity': Net_Quantity, 'Product capacity': Product_capacity, 'Packing Type': Packing_Type, 'Packing Units': Packing_Units, 'Description': Description, 'EAN': EAN, 'FSSAI': FSSAI, 'Units': Units, 'Food/ Non Food': Food_Non_Food, 'Benefits': Benefits, 'Image Front name': Image_Front_name, 'Image Back Name': Image_Back_Name, 'Image Ingredients Content Info': Image_Ingredients_Content_Info, 'Image Nutriteints Content Info': Image_Nutriteints_Content_Info, 'FSSAI Image': FSSAI_Image}
		csv_columns = ['Product name', 'Links', 'product_name_line2', 'product_name_line3', 'List of ingredients', 'Nutritional Information', 'Declaration regarding Veg /Non Veg/NA', 'Name and Complete address of the Manufacturer', 'Shelf Life', 'Country of Origin', 'How to Use?', 'Net Quantity', 'Product capacity', 'Packing Type', 'Packing Units', 'Description', 'EAN', 'FSSAI', 'Units', 'Food/ Non Food', 'Benefits', 'Image Front name', 'Image Back Name', 'Image Ingredients Content Info', 'Image Nutriteints Content Info', 'FSSAI Image']
		dict = [data]
		csv_file = "bigbasket_bindu.csv"
		try:
			with open(csv_file, 'w') as csvfile:
				writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
				writer.writeheader()
				for data in dict:
					writer.writerow(data)
		except IOError:
			print("I/O error")
		print(data)
