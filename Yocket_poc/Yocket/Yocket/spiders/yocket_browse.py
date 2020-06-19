import re
import csv
import scrapy
import json
from scrapy import signals
from collections import OrderedDict
from pydispatch import dispatcher
from urllib.parse import urljoin
from scrapy import Request

class Yocket(scrapy.Spider):
    name = 'yocket_browse'
    handle_httpstatus_list = [429,302]
    
    def __init__(self, *args, **kwargs):
        csv_file = "yocket_data.csv"
        csvfile = open(csv_file, 'w')
        csv_columns = ['ProfileName', 'UndergradDegree', 'UndergradUniversity', 'UndergradCgpa', 'Experience','WorkExperience','CompanyName', 'Jobtitle','Techpapers','Numberofresearch','Skills','InterestedTermandYear','InterestedCourse','GREScore(TOTAL)','GREScore(VERBAL)','GREScore(QUANT)','TOEFLSCORE','IELTSSCORE','Applieduniversity','Appliedcourse','Applieddate','Status','Link','SourceUniversity']
        self.writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
        self.writer.writeheader()
        dispatcher.connect(self.spider_closed, signals.spider_closed)

    def spider_closed(self, spider):
        print("details")
    
    def start_requests(self):
        headers = {
            'authority': 'yocket.in',
            'pragma': 'no-cache',
            'cache-control': 'no-cache',
            'accept': 'application/json, text/plain, */*',
            'x-requested-with': 'XMLHttpRequest',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36',
            'content-type': 'application/json;charset=UTF-8',
            'origin': 'https://yocket.in',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-mode': 'cors',
            'referer': 'https://yocket.in/account/login',
        }
        data = {"email":"sreenivas.dega1@gmail.com","password":"Headrun591!"}
        yield Request('https://yocket.in/users/login.json', headers=headers, body=json.dumps(data), method = 'POST',meta={'headers':headers})

    def parse(self, response):
        headers = response.meta['headers']
        data = {
            'pageCount': '1',
            'curr_url': 'https://yocket.in/universities'
        }
        meta = {'page': 1, 'headers':headers}
        yield Request('https://yocket.in/universities.json', headers=headers, body=json.dumps(data), method = 'POST', callback=self.parse_next,meta=meta)

    def parse_next(self,response):
        headers = response.meta['headers']
        page = response.meta['page']
        data = json.loads(response.text)
        json_data = data.get('universities',[])
        for i in json_data:
                name = i.get('url_alias','')
                id_ = i.get('id','')
                url = 'https://yocket.in/university-reviews/%s-%s/engineering'%(name,id_)
                yield Request(url,callback = self.parse_url,meta={'headers':headers,'id':id_,'name':name},headers=headers)

        if json_data:
            page = page+1
            data = {'pageCount': page, 'curr_url': 'https://yocket.in/universities'}
            url = 'https://yocket.in/universities.json'
            yield Request(url,callback = self.parse_next,headers=headers,meta={'page':page,'headers':headers},body=json.dumps(data), method = 'POST')
           
    def parse_url(self,response):
        headers = response.meta['headers']
        id_ = response.meta['id']
        name = response.meta['name']
        branches = response.xpath('//div[@class="btn-group"]//ul//li//a//@href').extract()
        for branch in branches:
            branch_ = branch.split('/')[-1]
            branch_url = 'https://yocket.in/university-reviews/%s-%s/%s'%(name,id_,branch_) 
            yield Request(branch_url,callback = self.parse_meta,meta={'headers':headers},headers=headers)

    def parse_meta(self,response):
        headers = response.meta['headers']
        urls = response.xpath('//a[contains(text(),"applied") or contains(text(), "admitted") or contains(text(), "interested")]//@href').extract()
        for url in urls:
            url = 'https://yocket.in'+url
            yield Request(url,callback=self.parse_meta_data,meta={'headers':headers},headers=headers)
        
    def parse_meta_data(self,response):
        headers = response.meta['headers']
        links = response.xpath('//div[@class="col-sm-9"]//h4//a//@href').extract()
        for link in links:
            link = urljoin('https://yocket.in/',link)
            yield Request(link,callback=self.parse_data,meta={'headers':headers},headers=headers)

        pagination = response.xpath('//ul[@class="pagination"]/li[contains(@class,"next")]/a/@href').extract_first()
        if pagination:
            page_nav = urljoin('https://yocket.in', pagination)
            yield Request(page_nav,callback=self.parse_meta_data,meta={'headers':headers},headers=headers)


    def parse_data(self,response):
        profile_name = ''.join(response.xpath('//div[@class="col-sm-12"]//h1//strong/text()').extract()).strip()
        email = ''.join(response.xpath('//div[@class="col-sm-12"]//h1//strong/small/text()').extract()).strip()
        skills = ', '.join(response.xpath('//div[@id="users-skills"]//div//h4/text()').extract()).replace('\n','')
        degree = ''.join(response.xpath('//p[@class="card-font"]//b//text()').extract())
        try:
            university = response.xpath('//div[@class="col-sm-12"]/h3/text()').extract()[0] 
        except:
            university = ''
        cgpa = ''.join(response.xpath('//p[@class="teal"]//b//text()').extract())
        try:
            college = ''.join(response.xpath('//p[@class="card-font"]/text()').extract()[2]).replace('\n','').strip()
        except:college = ''
        try:
            experience = response.xpath('//div[@class="col-md-2 col-xs-2"]//span//text()').extract()[0]
        except:
            experience = ''
        try:
            course = response.xpath('//div[@class="col-sm-12"]/h4/text()').extract()[0]
            if "Looking for help?" in course:
                course = ''
        except:
            course = ''
        year = ''.join(re.findall('\d+\d+', course))
        work_experience = []
        job_title = []
        companyname = []
        exp_nodes = response.xpath('//div[@id="work-experiences"]//div[contains(@class,"col-sm-12 border-top")]')
        for exp_node in exp_nodes:
            title = ''.join(exp_node.xpath('.//h4//text()').extract()).replace('\n','').strip()
            try:
                time = exp_node.xpath('.//span//text()').extract()[0].strip()
            except:time = ''
            try:
                place = exp_node.xpath('.//span//text()').extract()[1].strip()
            except:place = ''
            dic = {'jobtitle':title,'experience':time,'companyname':place}
            job_title.append(title)
            companyname.append(place) 
            work_experience.append(dic) 
        act_dic = []
        nodes = response.xpath('//div[@class="col-sm-3 col-xs-3"]')
        i = 0
        for node in nodes:
            key = ''.join(node.xpath('./h4/text()').extract()).strip()
            i = i+1
            act_dic.append(key)
        try:
            work_exp = act_dic[2]
        except:work_exp = ''
        try:
            papers = act_dic[3]
        except:papers = ''
        try:
            tofel = ''.join(response.xpath('//div[@class="col-sm-3 col-xs-3"][2]//h4//small//text()').extract())
        except:tofel = ''
        tofelscore,ielts_score = '',''
        if tofel == 'TOEFL':
            tofelscore = act_dic[1]
        elif tofel == 'IELTS':
            ielts_score = act_dic[1]
          
        try:
            exam = response.xpath('//div[@class="col-sm-3 col-xs-3"]//h4/small/text()').extract()[0]
        except:exam = ''
        totalscore,quant_score,scoreverbal = '','',''
        if exam == 'GRE' or exam == 'GRE/GMAT':            
            totalscore = response.xpath('//div[@class="col-sm-3 col-xs-3"]/h4/text()').extract()[1].strip()
            if not totalscore:
                totalscore = ' '.join(response.xpath('//div[@class="col-sm-3 col-xs-3"]/h4/small/text()')[1:3].extract())
            try:
                quant_score = response.xpath('//div[@class="col-sm-3 col-xs-3"]//h4//span/text()').extract()[0].strip()
            except:
                quant_score = ''
            try:
                scoreverbal = response.xpath('//div[@class="col-sm-3 col-xs-3"]//h4//span/text()').extract()[1].strip()
            except:
                scoreverbal = ''
        research = []
        projects = response.xpath('//div[@id="projects"]//div[contains(@class,"col-sm-12 border-top")]')
        for project in projects:
            project_name = ''.join(project.xpath('.//h4//text()').extract()).replace('\n','').strip()
            try:
                pro_exp = project.xpath('.//p//span//text()').extract()[0]
            except:
                pro_exp = ''
            research.append({'project_name':project_name,'project_experience':pro_exp})
        
        nodes = response.xpath('//div[@class="table-responsive"]//tr')
        for node in nodes:
            app_university = ''.join(node.xpath('.//h4//a//text()').extract()[0]).strip()
            app_course = ''.join(node.xpath('.//h4//a/small/text()').extract()).strip()
            try:
                app_date = node.xpath('.//div[@class="col-sm-6"]//small//text()').extract()[1]
            except:
                app_date = ''
            status = ''.join(node.xpath('.//td[@class="text-center "]//span//text()').extract()).replace('\n','')

            data = OrderedDict() 
            data['ProfileName'] = profile_name
            data['UndergradDegree'] = degree
            data['UndergradUniversity'] = college 
            data['UndergradCgpa'] = cgpa
            data['Experience'] = work_exp
            data['WorkExperience'] = work_experience
            data['CompanyName'] = ','.join(companyname)
            data['Jobtitle'] = ','.join(job_title)
            data['Techpapers'] = papers
            data['Numberofresearch'] = research 
            data['Skills'] = skills
            data['InterestedTermandYear'] = year
            data['InterestedCourse'] = course
            data['GREScore(TOTAL)'] = totalscore
            data['GREScore(VERBAL)'] = scoreverbal
            data['GREScore(QUANT)'] = quant_score
            data['TOEFLSCORE'] = tofelscore
            data['IELTSSCORE'] = ielts_score
            data['Applieduniversity'] = app_university
            data['Appliedcourse'] = app_course
            data['Applieddate'] = app_date
            data['Status'] = status
            data['Link'] = response.url
            data['SourceUniversity'] = university
            self.writer.writerow(data)
        
         
