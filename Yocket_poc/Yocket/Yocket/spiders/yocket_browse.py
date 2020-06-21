from json import dumps, loads
from urllib.parse import urljoin
from pydispatch import dispatcher
from scrapy import signals
from scrapy.spiders import Spider
from scrapy.http import Request
from scrapy.selector import Selector
from Yocket.items import CSVItem
from Yocket.common_utils import extract_data,\
    extract_list_data, get_nodes, re, get_csv_file,\
    create_default_dirs, move_file, CSV_FILES_PROCESSING_DIR

class YocketBrowse(Spider):
    name = 'yocket_browse'

    def __init__(self, *args, **kwargs):
        super(YocketBrowse, self).__init__(*args, **kwargs)
        create_default_dirs()
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
        self.csv_writer, self.csv_file = get_csv_file(self.name)
        self.csv_writer.writerow(CSVItem.fields.keys())
        dispatcher.connect(self.spider_closed, signals.spider_closed)

    def spider_closed(self):
        self.csv_file.close()
        move_file(self.csv_file, CSV_FILES_PROCESSING_DIR)

    def start_requests(self):
        data = {"pageCount": "1", "curr_url": "https://yocket.in/universities"}
        url = "https://yocket.in/universities.json"
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
            data = {"pageCount": page, "curr_url": "https://yocket.in/universities"}
            url = 'https://yocket.in/universities.json'
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
                student_link = urljoin("https://yocket.in/", student_link)
            yield Request(student_link, callback=self.parse_data, headers=self.headers, meta={'university': university})

        pagination = extract_data(sel, '//ul[@class="pagination"]/li[contains(@class,"next")]/a/@href')
        if pagination:
            page_nav = urljoin("https://yocket.in", pagination)
            yield Request(page_nav, callback=self.parse_category, headers=self.headers)

    def parse_data(self, response):
        sel = Selector(response)
        university = response.meta["university"]
        work_experiences, job_titles, company_names = [], [], []
        course_tests_dict, researches = {}, []
        profile_name = extract_data(sel, '//div[@class="col-sm-12"]//h1//strong/text()').strip() or response.url.split('/')[-1].title()
        degree = extract_data(sel, '//p[@class="card-font"]//b//text()')
        cgpa = extract_data(sel, '//p[@class="teal"]//b//text()')
        skills = [item.strip() for item in extract_list_data(sel, '//div[@id="users-skills"]//div//h4/text()')]
        college = extract_data(sel, '//div[@class="col-sm-9 col-xs-9"]/p[@class="card-font"][not(contains(text(), "backlog"))]/text()')
        course = extract_data(sel, '//div[@class="col-sm-12"]/h4[not(contains(text(), "Looking for help?"))]/text()')
        year = ''.join(re.findall(r'\d+\d+', course))

        experience_nodes = get_nodes(sel, '//div[@id="work-experiences"]//div[contains(@class,"col-sm-12 border-top")]')
        for experience_node in experience_nodes:
            title = extract_data(experience_node, './/h4//text()').replace('\n', '').strip()
            time_period = extract_data(experience_node, './/div[@class="col-sm-12"]//i[@class="fa fa-clock"]/../span/text()')
            organisation = extract_data(experience_node, './/div[@class="col-sm-12"]//i[@class="fa fa-building"]/../span/text()')

            work_experiences.append({'job_title': title, 'experience': time_period, 'company_name': organisation})
            job_titles.append(title)
            company_names.append(organisation)

        course_tests_nodes = get_nodes(sel, '//div[@class="col-sm-3 col-xs-3"]')
        for course_test_node in course_tests_nodes:
            key = extract_data(course_test_node, './h4/small[1]/text()').strip()
            value = extract_data(course_test_node, './h4/text()')

            scores = {}
            if "GRE" in key:
                sub_scores = extract_list_data(course_test_node, './h4/span/text()')
                for sub_score in sub_scores:
                    section, score = sub_score.split(':')
                    scores.update({section.strip(): score.strip()})

            course_tests_dict.update({key: {'total_value': value}})
            if scores:
                course_tests_dict.get(key, {}).update({'sub_scores': scores})

        projects_nodes = get_nodes(sel, '//div[@id="projects"]/div[contains(@class,"col-sm-12 border-top")]')
        for project_node in projects_nodes:
            project_name = extract_data(project_node, './/h4[@class="card-header"]//text()').replace('\n', '')
            project_experience = extract_data(project_node, './/p[@class="card-font"]/i[@class="fa fa-clock"]/../span/text()')
            researches.append({'project_name': project_name, 'project_experience': project_experience})

        applied_universities_nodes = get_nodes(sel, '//div[@id="application-statuses-div"]//div[@class="table-responsive"]//tr')
        for applied_university_node in applied_universities_nodes:
            applied_university = extract_data(applied_university_node, './td/h4/a/text()')
            applied_course = extract_data(applied_university_node, './td/h4/a/small/text()')
            applied_date = extract_data(applied_university_node, './td//div[@class="col-sm-6"]/small[contains(text(), "Applied")]//text()')
            application_status = extract_data(applied_university_node, './td[@class="text-center "]/h4/span//text()').replace('\n', '')

            csv_item = CSVItem()
            gre_details = course_tests_dict.get('GRE', {}) or course_tests_dict.get('GRE/GMAT', {})
            csv_item.update({
                "ProfileName": profile_name, "UndergradDegree": degree, "UndergradUniversity": college, "UndergradCgpa": cgpa,
                "Experience": course_tests_dict.get("Work Exp.", {}).get("total_value", ""), "WorkExperience": work_experiences,
                "CompanyName": ", ".join(company_names), "Jobtitle": ", ".join(job_titles), "Skills": skills, "Status": application_status,
                "Techpapers": course_tests_dict.get("Tech Papers", {}).get("total_value", ""), "Numberofresearch": researches,
                "InterestedTermandYear": year, "InterestedCourse": course, "GRETotalScore": gre_details.get("total_value", ""),
                "GREVerbalScore": gre_details.get("sub_scores", {}).get("Verbal", ""), "Applieduniversity": applied_university,
                "GREQuantScore": gre_details.get("sub_scores", {}).get("Quant", ""), "Appliedcourse": applied_course,
                "TOEFLSCORE": course_tests_dict.get("", {}).get("total_value", ""), "Applieddate": applied_date.split(':')[0].strip(),
                "IELTSSCORE": course_tests_dict.get("IELTS", {}).get("total_value", ""), "Link": response.url, "SourceUniversity": university
            })

            yield csv_item
