from json import dumps, loads
from urllib.parse import urljoin
from scrapy.http import Request
from scrapy.selector import Selector
from Yocket.common_utils import GenSpider, extract_data,\
    extract_list_data

class YocketBrowse(GenSpider):
    name = 'yocket_browse'

    def __init__(self, *args, **kwargs):
        super(YocketBrowse, self).__init__(*args, **kwargs)
        self.headers = {
            "authority": "yocket.in",
            "pragma": "no-cache",
            "cache-control": "no-cache",
            "accept": "application/json, text/plain, */*",
            "x-requested-with": "XMLHttpRequest",
            "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36",
            "content-type": "application/json;charset=UTF-8",
            "origin": "https://yocket.in",
            "sec-fetch-site": "same-origin",
            "sec-fetch-mode": "cors",
            "referer": "https://yocket.in/account/login"
        }

    def start_requests(self):
        data = {"pageCount": "1", "curr_url": "https://yocket.in/universities-in-usa"}
        url = "https://yocket.in/universities-in-usa.json"
        yield Request(url, callback=self.parse, headers=self.headers, body=dumps(data), method="POST", meta={"page": 1})

    def parse(self, response):
        page = response.meta["page"]
        data = loads(response.body)
        universities = data.get("universities", [])
        for university in universities:
            university_name = university.get("url_alias", "")
            university_id = university.get("id", "")
            meta = {"university_id": university_id, "university_name": university_name}
            url = "https://yocket.in/university-reviews/{}-{}/engineering".format(university_name, university_id)
            yield Request(url, callback=self.parse_university, headers=self.headers, meta=meta)

        if universities:
            page += 1
            data = {"pageCount": page, "curr_url": "https://yocket.in/universities-in-usa"}
            url = 'https://yocket.in/universities-in-usa.json'
            yield Request(url, callback=self.parse, headers=self.headers, body=dumps(data), method="POST", meta={"page": page})

    def parse_university(self, response):
        sel = Selector(response)
        university_id = response.meta["university_id"]
        university_name = response.meta["university_name"]
        branches = extract_list_data(sel, '//div[@class="btn-group"]//ul//li//a//@href')
        for branch in branches:
            branch_id = branch.split("/")[-1]
            branch_url = "https://yocket.in/university-reviews/{}-{}/{}".format(university_name, university_id, branch_id)
            yield Request(branch_url, callback=self.parse_branch, headers=self.headers)

    def parse_branch(self, response):
        sel = Selector(response)
        categories_urls = extract_list_data(sel,\
            '//a[contains(text(),"applied") or contains(text(), "admitted") or contains(text(), "interested")]//@href')
        for category_url in categories_urls:
            if category_url and "http" not in category_url:
                category_url = urljoin("https://yocket.in", category_url)
            yield Request(category_url, callback=self.parse_category, headers=self.headers)

    def parse_category(self, response):
        sel = Selector(response)
        university = extract_data(sel, '//input[@id="users-view-search-universities"]/@value')
        students_links = extract_list_data(sel, '//div[@class="col-sm-9"]//h4//a//@href')
        for student_link in students_links:
            if student_link and "http" not in student_link:
                student_id = student_link.split('/')[-1]
                student_link = urljoin("https://yocket.in/", student_link)
                meta_data = {"university": university}
                self.get_page("yocket_students_terminal", student_link, student_id, meta_data=meta_data)
            #yield Request(student_link, callback=self.parse_data, headers=self.headers, meta={'university': university})

        pagination = extract_data(sel, '//ul[@class="pagination"]/li[contains(@class,"next")]/a/@href')
        if pagination:
            page_nav = urljoin("https://yocket.in", pagination)
            yield Request(page_nav, callback=self.parse_category, headers=self.headers)
