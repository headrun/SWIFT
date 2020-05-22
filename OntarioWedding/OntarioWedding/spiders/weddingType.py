import scrapy
from scrapy.http import Request, FormRequest
from urllib.parse import urlencode
import json
from scrapy.selector import Selector
import math
import requests
import re
from collections import OrderedDict
import csv
from pydispatch import dispatcher
from scrapy import signals
category_mapping_list = {"wedding-photography":"Wedding Photography",
						"wedding-catering":"Wedding Catering",
						"photobooth":"Wedding Photobooth",
						"wedding-videography":"Wedding Videography",
						"wedding-dj":"Wedding DJ",
						"wedding-music":"Wedding Musician",
						"wedding-transportation":"Wedding Transportation",
						"wedding-entertainer":"Wedding Entertainer",
						"wedding-flowers":"Wedding Flowers",
						"wedding-planning":"Wedding Planning",
						"wedding-decorations":"Wedding Decorations",
						"wedding-cakes":"Wedding Cakes",
						"event-rentals":"Wedding Rentals",
						"wedding-officiants":"Wedding Officiants"}
class WeddingWireClass(scrapy.Spider):
	name = "weddingWireTypeBrowser"
	def __init__(self, *args, **kwargs):
		csv_file = "data.csv"
		csvfile = open(csv_file, 'w')
		keys =  ['Business_Name','Category','Business_Badge','Phone_Number','Street_Address','Number_Of_Reviews','Rating','Quality_Of_Service','Professionalism','Flexibility','Value','Response_Time','Description','FAQSession','Business_PhotoGallery','ReferenceUrl']
		self.writer = csv.DictWriter(csvfile, fieldnames=keys)
		self.writer.writeheader()
		dispatcher.connect(self.spider_closed, signals.spider_closed)

	def spider_closed(self, spider):
		print("mohana")

	def start_requests(self):
		headers = {
			'authority': 'www.weddingwire.ca',
			'accept': 'text/html, */*; q=0.01',
			'x-requested-with': 'XMLHttpRequest',
			'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.129 Safari/537.36',
			'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
			'origin': 'https://www.weddingwire.ca',
			'sec-fetch-site': 'same-origin',
			'sec-fetch-mode': 'cors',
			'sec-fetch-dest': 'empty',
			'referer': 'https://www.weddingwire.ca/wedding-vendors/ontario',
			'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8'
		}

		data = {
			'id_provincia': '2052',
			'id_grupo': '2',
			'_lat': '49.2643623352',
			'_long': '-84.7476501465'
		}
		yield FormRequest('https://www.weddingwire.ca/busc-Filters.php',headers=headers,formdata=data,callback=self.parse)
	def parse(self,response):
		headers = {
			'authority': 'www.weddingwire.ca',
			'upgrade-insecure-requests': '1',
			'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.129 Safari/537.36',
			'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
			'sec-fetch-site': 'none',
			'sec-fetch-mode': 'navigate',
			'sec-fetch-dest': 'document',
			'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
		}
		res = Selector(response)
		weddingTypeUrls = res.xpath('//li[@data-name="id_sector"]')
		for weddingtype in weddingTypeUrls:
			data_url = ''.join(weddingtype.xpath('@data-url').extract()).strip()
			if data_url:
				request_url = 'https://www.weddingwire.ca/'+data_url+'/ontario'
				vendors_count = ''.join(weddingtype.xpath('./span[@class="vendorsFilters__count"]/text()').extract()).strip()
				total_count = int(vendors_count.replace(',',''))
				meta = {'dataUrl': request_url, 'page': 1, 'totalPage': math.ceil(total_count/14)}
				yield Request(request_url,headers=headers,callback=self.parse1, meta=meta)
				# break;
	def parse1(self,response):
		headers = {
			'authority': 'www.weddingwire.ca',
			'upgrade-insecure-requests': '1',
			'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.129 Safari/537.36',
			'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
			'sec-fetch-site': 'none',
			'sec-fetch-mode': 'navigate',
			'sec-fetch-dest': 'document',
			'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
		}
		product_headers = {
			'authority': 'www.weddingwire.ca',
			'upgrade-insecure-requests': '1',
			'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.129 Safari/537.36',
			'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
			'sec-fetch-site': 'none',
			'sec-fetch-mode': 'navigate',
			'sec-fetch-dest': 'document',
			'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
		}
		pageNumber = response.meta['page']
		totalPage = response.meta["totalPage"]
		dataUrl = response.meta["dataUrl"]
		pageNumber = pageNumber + 1
		res = Selector(response)
		products_urls = res.xpath('//div[contains(@class,"app-internal-tracking-item")]')
		for product in products_urls:
			businessType = ''.join(product.xpath('.//span[@class="vendorRibbon"]/text()').extract())
			business_class = ''
			if businessType:
				business_class = businessType
			detail_meta = {'businessClass':businessType}
			product_element_url = ''.join(product.xpath('.//a[contains(@id,"app_lnkEmp")]/@href').extract())
			yield Request(product_element_url,headers=product_headers,callback=self.parse2,meta=detail_meta)
		if pageNumber <= totalPage:
			url = dataUrl + '--' + str(pageNumber)
			print(url)
			meta = {'dataUrl': dataUrl, 'page': pageNumber, 'totalPage': totalPage}
			yield Request(url,headers=headers,callback=self.parse1, meta=meta)
	def parse2(self,response):
		businessClass = response.meta["businessClass"]
		res = Selector(response)
		category = response.url.split('/')[-2]
		category_name = category_mapping_list.get(category,'')
		if not category_name:
			return
		business_name = ''.join(res.xpath('//h1[@class="storefrontHeader__title"]/text()').extract()).strip()
		street_address = ''
		address_elemets=res.xpath('//div[@class="storefrontHeaderOnepage__address"]/text()').extract()
		for address in address_elemets:
			address_value = address.strip()
			address_value = re.sub(' +', ' ', address_value)
			address_value = address_value.replace('\n','')
			if address_value:
				street_address = street_address + ' ' + address_value
		description = ''
		des_paras = res.xpath('//div[contains(@class,"storefront__description")]/*')
		for pp in des_paras:
			span_element = ''.join(pp.xpath('./span/text()').extract()).strip()
			if span_element:description = description + span_element + ' '
			spn_strong_element = ''.join(pp.xpath('./span/span/strong/text()').extract()).strip()
			if spn_strong_element:description = description + spn_strong_element + ' '
			list_data = ','.join(pp.xpath('./li/text()').extract()).strip()
			if list_data:description = description + list_data + ' '
			strong_element = ''.join(pp.xpath('./strong/text()').extract()).strip()
			if strong_element:description = description + strong_element + ' '
			paragraph = ''.join(pp.xpath('./text()').extract()).strip()
			if paragraph:description = description + paragraph + ' '
		images = res.xpath('//img[@class="pointer"]')
		result_images = []
		for image in images:
			src_links = ''.join(image.xpath('./@src').extract()).strip()
			if src_links:
				result_images.append(src_links)
			data_links = ''.join(image.xpath('./@data-src').extract()).strip()
			if data_links:
				result_images.append(data_links)
		faqResultList = []
		FAQ_elements = res.xpath('//ul[@class="storefront-faqs"]/li')
		for element in FAQ_elements:
			question = ''.join(element.xpath('./span/text()').extract()).strip()
			div_element = element.xpath('./div/div/text()').extract()
			if div_element:aws = div_element
			else:aws = element.xpath('./div/div/div/text()').extract()
			faqResultList.append({"question":question,"answer":aws})
		numberOfReviews = ''.join(res.xpath('//span[@class="storefrontSummary__label"]/text()').extract()).strip()
		rating = ''.join(res.xpath('//div[contains(@class,"storefrontSummary__text")]/text()').extract()).strip()
		totalrating = ' '.join(res.xpath('//div[@class="storefrontRating pt20"]/div[@class="storefrontRatingBox"]/span/text()').extract()).strip()
		qualityOfService = ''.join(res.xpath('//div[@class="storefrontRating pt20"]/ul/li/span/span[contains(text(),"Quality of service")]/../strong/text()').extract()).strip()
		professionalism = ''.join(res.xpath('//div[@class="storefrontRating pt20"]/ul/li/span/span[contains(text(),"Professionalism")]/../strong/text()').extract()).strip()
		flexibility = ''.join(res.xpath('//div[@class="storefrontRating pt20"]/ul/li/span/span[contains(text(),"Flexibility")]/../strong/text()').extract()).strip()
		value = ''.join(res.xpath('//div[@class="storefrontRating pt20"]/ul/li/span/span[contains(text(),"Value")]/../strong/text()').extract()).strip()
		responseTime = ''.join(res.xpath('//div[@class="storefrontRating pt20"]/ul/li/span/span[contains(text(),"Response time")]/../strong/text()').extract()).strip()
		teliphone_id = ''.join(res.xpath('//span[@class="app-emp-phone-txt"]/@data-id-vendor').extract()).strip()
		teliphone_number = ''
		if teliphone_id:
			teliphone_number = self.teliphone(teliphone_id)
		teliphone_number = teliphone_number.replace('\xa0',' ')
		if len(result_images) > 10:
			images_list = result_images[:10]
		else:
			images_list = result_images
		totalrating = totalrating.replace('out of','/')
		numberOfReviews = numberOfReviews.replace('Availability','')
		final_sub_dict = OrderedDict()
		final_sub_dict["Business_Name"] = business_name
		final_sub_dict["Category"] = category_name
		final_sub_dict["Business_Badge"] = businessClass
		final_sub_dict["Phone_Number"] = teliphone_number
		final_sub_dict["Street_Address"] = street_address
		final_sub_dict["Number_Of_Reviews"] = numberOfReviews
		final_sub_dict["Rating"] = totalrating
		final_sub_dict["Quality_Of_Service"] = qualityOfService
		final_sub_dict["Professionalism"] = professionalism
		final_sub_dict["Flexibility"] = flexibility
		final_sub_dict["Value"] = value
		final_sub_dict["Response_Time"] = responseTime
		final_sub_dict["Description"] = description.replace('\n','')
		final_sub_dict["FAQSession"] = faqResultList
		final_sub_dict["Business_PhotoGallery"] = images_list
		final_sub_dict["ReferenceUrl"] = response.url
		self.writer.writerow(final_sub_dict)

	def teliphone(self,id):
		headers = {
		    'authority': 'www.weddingwire.ca',
		    'accept': 'text/html, */*; q=0.01',
		    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.129 Safari/537.36',
		    'x-requested-with': 'XMLHttpRequest',
		    'sec-fetch-site': 'same-origin',
		    'sec-fetch-mode': 'cors',
		    'sec-fetch-dest': 'empty',
		    'referer': 'https://www.weddingwire.ca/photobooth/take-my-photo--e7858',
		    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
		}

		params = (
		    ('id_empresa', id),
		    ('reduced', '/vendors/item/profile'),
		)
		response = requests.get('https://www.weddingwire.ca/emp-ShowTelefonoTrace.php', headers=headers, params=params)
		res = Selector(response)
		phoneNumber = ''.join(res.xpath('//p[@class="storefrontDrop__tag"]/text()').extract()).strip()
		return phoneNumber
