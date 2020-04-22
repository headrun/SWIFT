import re, json
from requests import get
from pydispatch import dispatcher
from scrapy import signals
from scrapy.spiders import Spider
from scrapy.selector import Selector
from scrapy.http import Request, FormRequest

class Linkedinpremiumapivoyager(Spider):
    name = "linkedinapivoyager_browse"
    allowed_domains = ["linkedin.com"]
    start_urls = ('https://www.linkedin.com/uas/login?goback=&trk=hb_signin',)
    handle_httpstatus_list = [403]

    def __init__(self, *args, **kwargs):
        super(Linkedinpremiumapivoyager, self).__init__(*args, **kwargs)
        self.login = kwargs.get('login', 'matt')
        self.logins_dict = {
            'matt': ['mattrobertsm@gmail.com', 'Roberts@11']
        }
        self.domain = "https://www.linkedin.com"
        dispatcher.connect(self.spider_closed, signals.spider_closed)

    def spider_closed(self, spider):
        cv = get('https://www.linkedin.com/logout/').text

    def parse(self, response):
        sel = Selector(response)
        logincsrf = ''.join(sel.xpath('//input[@name="loginCsrfParam"]/@value').extract())
        csrf_token = ''.join(sel.xpath('//input[@name="csrfToken"]/@value').extract())
        loginCsrfParam = ''.join(sel.xpath('//input[@name="loginCsrfParam"]/@value').extract())
        sIdString = ''.join(sel.xpath('//input[@name="sIdString"]/@value').extract())
        pageInstance = ''.join(sel.xpath('//input[@name="pageInstance"]/@value').extract())
        login_account = self.logins_dict.get(self.login, '')
        account_mail, account_password = login_account

        data = {
            'csrfToken': csrf_token,
            'session_key': account_mail,
            'ac': '0',
            'sIdString': sIdString,
            'parentPageKey': 'd_checkpoint_lg_consumerLogin',
            'pageInstance': 'urn:li:page:d_checkpoint_lg_consumerLogin;Jm6PyFVkTIaobu2ZmNrMDw==',
            'trk': 'hb_signin',
            'authUUID': '',
            'session_redirect': '',
            'loginCsrfParam': loginCsrfParam,
            '_d': 'd',
            'controlId': 'd_checkpoint_lg_consumerLogin-login_submit_button',
            'session_password': account_password,
        }

        headers = {
            'authority': 'www.linkedin.com',
            'cache-control': 'max-age=0',
            'origin': 'https://www.linkedin.com',
            'upgrade-insecure-requests': '1',
            'content-type': 'application/x-www-form-urlencoded',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36',
            'sec-fetch-user': '?1',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-mode': 'navigate',
            'referer': 'https://www.linkedin.com/uas/login?goback=&trk=hb_signin',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'cookie': response.headers.getlist('Set-Cookie'),
        }
        url = 'https://www.linkedin.com/checkpoint/lg/login-submit'
        yield FormRequest(url, callback=self.parse_next, formdata=data, headers=headers, method="POST", meta={"csrf_token": csrf_token})

    def parse_next(self, response):
        sel = Selector(response)
        cooki_list = response.request.headers.get('Cookie', [])
        li_at_cookie = ''.join(re.findall('li_at=(.*?); ', cooki_list.decode('utf8')))

        headers = {
            'authority': 'www.linkedin.com',
            'pragma': 'no-cache',
            'cache-control': 'no-cache',
            'accept': 'application/vnd.linkedin.normalized+json+2.1',
            'csrf-token': response.meta['csrf_token'],
            'x-restli-protocol-version': '2.0.0',
            'x-li-lang': 'en_US',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.92 Safari/537.36',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-mode': 'cors',
            'sec-fetch-dest': 'empty',
            'referer': 'https://www.linkedin.com/feed/',
            'accept-language': 'en-US,en;q=0.9,fil;q=0.8,te;q=0.7',
            'cookie': 'li_at=%s; JSESSIONID="%s"' % (li_at_cookie, response.meta['csrf_token']),
        }

        #html_url = "https://www.linkedin.com/in/george-eapen-39b03428"
        html_url = 'https://www.linkedin.com/in/karthikbalait/'
        user_id = html_url.strip('/').split('/')[-1]
        api_url = 'https://www.linkedin.com/voyager/api/identity/dash/profiles?q=memberIdentity&memberIdentity=%s&decorationId=com.linkedin.voyager.dash.deco.identity.profile.FullProfileWithEntities-42' % user_id
        meta = {'csrf_token': response.meta['csrf_token'], 'headers': headers, "user_id": user_id}
        yield Request(api_url, callback=self.parse_userdata, meta=meta, headers=headers)

    def parse_userdata(self, response):
        _data = json.loads(response.body)
        self.aux_data = _data.get('included', [])
        self.profile_data = [item for item in self.aux_data if item.get('publicIdentifier', '') == response.meta['user_id']]
        if self.profile_data:
            self.profile_data = self.profile_data[0]

        first_name = self.profile_data.get('firstName', '')
        last_name = self.profile_data.get('lastName', '')
        maiden_name = self.profile_data.get('maidenName', '')
        headline = self.profile_data.get('headline', '')
        location = self.profile_data.get('locationName', '')
        birth_date = self.profile_data.get('birthDateOn', '')
        background_picture = self.profile_data.get('backgroundPicture', '')
        volunteer_causes = self.profile_data.get('volunteerCauses', '')
        about = self.profile_data.get('summary', '')
        urn = self.profile_data.get('entityUrn', '')
        object_urn = self.profile_data.get('objectUrn', '')
        tracking_id = self.profile_data.get('trackingId', '')
        public_identifier = self.profile_data.get('publicIdentifier', '')
        version_tag = self.profile_data.get('versionTag', '')
        interests = self.profile_data.get('interests', '')

        profile_pic = ''
        profile_pic_details = self.profile_data.get('profilePicture', {}).get('displayImageReference', {}).get('vectorImage', {})
        if profile_pic_details:
            root_url = profile_pic_details.get('rootUrl', '')
            profile_pic_node = profile_pic_details.get('artifacts', [])[-1]
            image = profile_pic_node.get('fileIdentifyingUrlPathSegment', '')
            profile_pic = '%s%s' % (root_url, image) if image else ''

        industry = self.get_details('com.linkedin.voyager.dash.common.Industry') 
        certifications = self.get_details('com.linkedin.voyager.dash.identity.profile.Certification')
        courses = self.get_details('com.linkedin.voyager.dash.identity.profile.Course')
        honors = self.get_details('com.linkedin.voyager.dash.identity.profile.Honor')
        languages = self.get_details('com.linkedin.voyager.dash.identity.profile.Language')
        organisations = self.get_details('com.linkedin.voyager.dash.identity.profile.Organization')
        patents = self.get_details('com.linkedin.voyager.dash.identity.profile.Patent')
        skills = self.get_details('com.linkedin.voyager.dash.identity.profile.Skill')


        education = {}
        education_nodes = self.get_nodes('com.linkedin.voyager.dash.identity.profile.Education')
        for education_node in education_nodes:
            school_name = education_node.get('schoolName', '')
            degree_name = education_node.get('degreeName', '')
            decription = education_node.get('description', '')
            field = education_node.get('fieldOfStudy', '')
            start_date = education_node.get('dateRange', {}).get('start', {}).get('year', '')
            end_date = education_node.get('dateRange', {}).get('end', {}).get('year', '')

            education.setdefault(school_name, [])
            education.get(school_name, []).append({
                degree_name:{
                    'degree_name': degree_name,
                    'school name': school_name,
                    'decription': decription,
                    'field of study': field,
                    'duration': '%s-%s' % (start_date, end_date)
                    }
                })

        career = {}
        career_nodes = self.get_nodes('com.linkedin.voyager.dash.identity.profile.Position')
        for career_node in career_nodes:
            title = career_node.get('title', '')
            location = career_node.get('locationName', '')
            geo_location = career_node.get('geoLocationName', '')
            description = career_node.get('description', '')
            company_name = career_node.get('companyName', '')

            career.setdefault(company_name, [])
            career.get(company_name, []).append({
                'title': title,
                'location': location,
                'geo_location': geo_location,
                'description': description,
                'company_name': company_name
                })


    def get_details(self, key):
        values = []
        nodes = self.get_nodes(key)
        for node in nodes:
            values.append(node.get('name', ''))
        return values

    def get_nodes(self, key):
        nodes = [item for item in self.aux_data if item.get('$type', '') == key]
        return nodes

