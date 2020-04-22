from time import sleep
from re import findall
from itertools import product
from urllib.parse import urljoin
from requests import post, get
from pydispatch import dispatcher
from scrapy import signals
from scrapy.spiders import Spider
from scrapy.selector import Selector
from scrapy.http import Request, FormRequest

class IncomeTaxSpider(Spider):
    name = "incometax_browse"
    domain_url = "https://portal.incometaxindiaefiling.gov.in"
    allowed_domains = ["incometaxindiaefiling.gov.in"]
    start_urls = []

    def __init__(self, **kwargs):
        super(IncomeTaxSpider, self).__init__(**kwargs)
        self.username = kwargs.get("username", "")
        self.password = kwargs.get("password", "")
        self.dbc_username = "Innominds"
        self.dbc_password = "Helloworld1234"
        self.home_url, self.logout_url = "", ""
        self.download_items = []
        dispatcher.connect(self.spider_closed, signals.spider_closed)

    def spider_closed(self, spider):
        headers = {
            "Connection": "keep-alive",
            "Pragma": "no-cache",
            "Cache-Control": "no-cache",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.113 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-User": "?1",
            "Sec-Fetch-Dest": "document",
            "Referer": self.home_url,
            'Accept-Language': 'en-US,en;q=0.9,fil;q=0.8,te;q=0.7',
        }
        if self.logout_url and 'http' in self.logout_url:
            Request(self.logout_url, headers=headers)

    def start_requests(self):
        headers = {
            "Connection": "keep-alive",
            "Pragma": "no-cache",
            "Cache-Control": "no-cache",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.113 Safari/537.36",
            "Sec-Fetch-Site": "same-site",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-User": "?1",
            "Sec-Fetch-Dest": "document",
            "Referer": "https://www.incometaxindiaefiling.gov.in/home",
            "Accept-Language": "en-US,en;q=0.9,fil;q=0.8,te;q=0.7"
        }
        url = "https://portal.incometaxindiaefiling.gov.in/e-Filing/UserLogin/LoginHome.html?lang=eng"
        yield Request(url, callback=self.parse, headers=headers)

    def parse(self, response):
        sel = Selector(response)
        captcha_url = ''.join(sel.xpath('//img[@id="captchaImg"]/@src').extract())
        request_id = ''.join(set(sel.xpath('//input[@name="requestId"]/@value').extract()))
        jsessionid = response.headers['Set-Cookie'].split(b';')[0]
        headers = {
            "Connection": "keep-alive",
            "Pragma": "no-cache",
            "Cache-Control": "no-cache",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.113 Safari/537.36",
            "Accept": "image/webp,image/apng,image/*,*/*;q=0.8",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-Mode": "no-cors",
            "Sec-Fetch-Dest": "image",
            "Referer": "https://portal.incometaxindiaefiling.gov.in/e-Filing/UserLogin/LoginHome.html?lang=eng",
            "Accept-Language": "en-US,en;q=0.9,fil;q=0.8,te;q=0.7",
        }
        if captcha_url and "http" not in captcha_url:
            captcha_url = urljoin(self.domain_url, captcha_url)
            meta = {"request_id": request_id, "login_url": response.url, "jsessionid": jsessionid}
            yield Request(captcha_url, callback=self.parse_captcha, headers=headers, meta=meta)

    def parse_captcha(self, response):
        jsessionid = response.meta["jsessionid"].decode("utf8").replace("JSESSIONID=", "")
        request_id = response.meta["request_id"]
        headers = {
            "Expect": ""
        }
        _data = {
            "username": self.dbc_username,
            "password": self.dbc_password,
            "captchafile": response.body
        }

        with open("captcha.png", "wb+") as _file:
            _file.write(response.body)

        captcha_text = ""
        sleep(3)
        captcha_retry = 0
        while (not captcha_text or len(captcha_text) < 6) and captcha_retry < 4:
            captcha_request = post("http://api.dbcapi.me/api/captcha", headers=headers, files=_data)
            captcha_request_id = "".join(findall("captcha=(\d+)\&", captcha_request.text))
            sleep(10)
            captcha_response = get("http://api.dbcapi.me/api/captcha/%s" % captcha_request_id)
            captcha_text = "".join(findall("text=(.*)&", captcha_response.text))
            print (captcha_text)
            if captcha_text and len(captcha_text) == 6:
                break
            captcha_retry += 1

        login_headers = {
            "Connection": "keep-alive",
            "Pragma": "no-cache",
            "Cache-Control": "no-cache",
            "Upgrade-Insecure-Requests": "1",
            "Origin": "https://portal.incometaxindiaefiling.gov.in",
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.113 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-User": "?1",
            "Sec-Fetch-Dest": "document",
            "Referer": "https://portal.incometaxindiaefiling.gov.in/e-Filing/UserLogin/LoginHome.html?lang=eng",
            "Accept-Language": "en-US,en;q=0.9,fil;q=0.8,te;q=0.7"
        }

        login_data = {
            "hindi": "N",
            "requestId": request_id,
            "nextPage": "",
            "userName": self.username,
            "userPan": '',
            "password": self.password,
            "rsaToken": '',
            "captchaCode": captcha_text.upper(),
        }
        if len(captcha_text) == 6:
            post_url = "https://portal.incometaxindiaefiling.gov.in/e-Filing/UserLogin/Login.html;jsessionid=%s" % jsessionid
            yield FormRequest(post_url, callback=self.parse_login, headers=login_headers, formdata=login_data, meta=response.meta)

    def parse_login(self, response):
        sel = Selector(response)
        self.home_url = response.url

        logout_url = "".join(findall('\(([^\)]+)\)', ''.join(sel.xpath('//input[contains(@value, "Logout")]/@onclick').extract()))).strip("'")
        if logout_url and "http" not in logout_url:
            self.logout_url = urljoin(self.domain_url, logout_url)

        headers = {
            'Connection': 'keep-alive',
            'Pragma': 'no-cache',
            'Cache-Control': 'no-cache',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.113 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-User': '?1',
            'Sec-Fetch-Dest': 'document',
            'Referer': response.url,
            'Accept-Language': 'en-US,en;q=0.9,fil;q=0.8,te;q=0.7',
        }

        if 'ForcedLogin' in response.url:
            request_id = ''.join(sel.xpath('//input[@id="ForcedLogin_requestId"]/@value').extract())
            data = {
                'requestId': request_id,
                'nextPage': '',
                'buttonType': 'Forced Login'
            }

            url = 'https://portal.incometaxindiaefiling.gov.in/e-Filing/UserLogin/ForcedLogin.html'
            yield FormRequest(url, callback=self.parse_login, headers=headers, formdata=data)

        else:
            download_xml_url = ''.join(sel.xpath('//a[contains(@href, "DownloadPrefilledXmlLink")]/@href').extract())
            if download_xml_url and 'http' not in download_xml_url:
                download_xml_url = urljoin(self.domain_url, download_xml_url)
                yield Request(download_xml_url, callback=self.parse_get_downloads, headers=headers)

    def parse_get_downloads(self, response):
        sel = Selector(response)
        financials = sel.xpath('//select[@name="asYear"]/option[not(contains(@value, "-1"))]/@value').extract()
        forms = sel.xpath('//select[@name="formId"]/option[contains(@value, "ITR")]/@value').extract()
        headers = {
            'Connection': 'keep-alive',
            'Pragma': 'no-cache',
            'Cache-Control': 'no-cache',
            'Upgrade-Insecure-Requests': '1',
            'Origin': 'https://portal.incometaxindiaefiling.gov.in',
            'Content-Type': 'application/x-www-form-urlencoded',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.113 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-User': '?1',
            'Sec-Fetch-Dest': 'document',
            'Referer': response.url,
            'Accept-Language': 'en-US,en;q=0.9,fil;q=0.8,te;q=0.7',
        }

        download_xml_url = ''.join(sel.xpath('//a[contains(@href, "DownloadPrefilledXmlLink")]/@href').extract())
        download_items = [item for item in list(product(financials, forms)) if item not in self.download_items]
        max_year = 0
        if download_items:
            download_item = download_items[0]
            self.download_items.append(download_item)
            year, form_id = download_item
            if year == max(financials):
                max_year = 1
            _id = ''.join(findall('ID=(.*)', download_xml_url))
            data = {
                'ID': _id,
                'panNo': self.username,
                'asYear': year,
                'formId': form_id
            }
            download_xml_url = 'https://portal.incometaxindiaefiling.gov.in/e-Filing/MyAccount/DownloadPrefilledXml.html?ID=%s' % _id
            meta = {'year': year, 'form_type': form_id, 'max_year': max_year}
            yield FormRequest(download_xml_url, callback=self.parse_xml_download_details, headers=headers, formdata=data, meta=meta)

    def parse_xml_download_details(self, response):
        sel = Selector(response)
        max_year = response.meta['max_year']
        download_xml_url = ''.join(sel.xpath('//a[contains(@href, "DownloadPrefilledXmlLink")]/@href').extract())
        form_type = ''.join(sel.xpath('//input[@name="formType"]/@value').extract())
        hp_flag = ''.join(sel.xpath('//input[@name="hpFlagChk"]/@value').extract())
        relief_flag = ''.join(sel.xpath('//input[@name="reliefFlagChk"]/@value').extract())
        checkbox_add = ''.join(sel.xpath('//input[@name="__checkbox_addFlagBankDtls[0]"]/@value').extract())
        checkbox_emp = ''.join(sel.xpath('//input[@name="__checkbox_empFlag"]/@value').extract())
        emp_flag = ''.join(sel.xpath('//input[@name="empFlagChk"]/@value').extract())
        salary_flag = ''.join(sel.xpath('//input[@name="salaryFlagChk"]/@value').extract())
        other_flag = ''.join(sel.xpath('//input[@name="otherSrcFlagChk"]/@value').extract())
        relief_flag = ''.join(sel.xpath('//input[@name="reliefFlagChk"]/@value').extract())
        foreign_flag = ''.join(sel.xpath('//input[@name="foreignFlagChk"]/@value').extract())
        checkbox_pri = ''.join(sel.xpath('//input[@name="__checkbox_priFlag"]/@value').extract())
        pri_flag = ''.join(sel.xpath('//input[@name="priFlagChk"]/@value').extract())
        add_flag = ''.join(sel.xpath('//input[@name="addFlagChk"]/@value').extract())
        headers = {
            'Connection': 'keep-alive',
            'Pragma': 'no-cache',
            'Cache-Control': 'no-cache',
            'Upgrade-Insecure-Requests': '1',
            'Origin': 'https://portal.incometaxindiaefiling.gov.in',
            'Content-Type': 'application/x-www-form-urlencoded',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.113 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-User': '?1',
            'Sec-Fetch-Dest': 'document',
            'Referer': response.url,
            'Accept-Language': 'en-US,en;q=0.9,fil;q=0.8,te;q=0.7',
        }

        _id = ''.join(findall('ID=(.*)', download_xml_url))
        itrs_texts = ''.join(findall('itrName == (?s).*', response.body.decode('utf8'))).split('</script>')[0].split(';')
        itrs_text = ''.join([item for item in itrs_texts if form_type in item])
        class_id = ''.join(findall("getPrefilledXmlwithITR1\('(.*)'\)", itrs_text))
        data = {
            'ID': _id,
            'formType': form_type,
            'prefilCncntFlag': 'Y',
            'hpFlagChk': hp_flag,
            'reliefFlagChk': relief_flag,
            '__checkbox_addFlagBankDtls[0]': checkbox_add
        }

        if not max_year:
            data = {
                'ID': _id,
                'formType': form_type,
                'prefilCncntFlag': 'Y',
                '__checkbox_empFlag': checkbox_emp,
                'empFlagChk': emp_flag,
                'salaryFlagChk': salary_flag,
                'hpFlagChk':  hp_flag,
                'otherSrcFlagChk': other_flag,
                'reliefFlagChk': relief_flag,
                'foreignFlagChk': foreign_flag,
                '__checkbox_priFlag': checkbox_pri,
                'priFlagChk': pri_flag,
                'addFlagChk': add_flag
            }

        url = 'https://portal.incometaxindiaefiling.gov.in/e-Filing/MyAccount/DownloadPrefilledXmlWithITR1.html?ID=%s&classId=%s' % (_id, class_id)
        yield FormRequest(url, callback=self.parse_download_file, headers=headers, formdata=data, meta=response.meta)

        if download_xml_url and 'http' not in download_xml_url:
            download_xml_url = urljoin(self.domain_url, download_xml_url)
            yield FormRequest(download_xml_url, callback=self.parse_get_downloads, headers=headers, formdata=data, dont_filter=True)

    def parse_download_file(self, response):
        form_type = response.meta['form_type']
        year = response.meta['year']
        if response.body:
            with open('%s_%s_%s.xml' % (self.username, form_type, year), 'wb+') as _file:
                _file.write(response.body)
