from json import dumps
from scrapy.http import Request
from scrapy.selector import Selector
from Yocket.items import CSVItem
from Yocket.common_utils import GenSpider, extract_data,\
    extract_list_data, get_nodes, re


class YocketTerminal(GenSpider):
    name = 'yocket_students_terminal'

    def __init__(self, *args, **kwargs):
        super(YocketTerminal, self).__init__(*args, **kwargs)
        self.request_headers = {
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
        self.get_metadata_file().write('%s\n' % ';'.join(['ProfileName', 'UndergradDegree', 'UndergradUniversity',
            'UndergradCgpa', 'Experience', 'WorkExperience', 'CompanyName', 'Jobtitle', 'Techpapers', 'Numberofresearch',
            'Skills', 'InterestedTermandYear', 'InterestedCourse', 'GREScore(TOTAL)', 'GREScore(VERBAL)', 'GREScore(QUANT)',
            'TOEFLSCORE', 'IELTSSCORE', 'Applieduniversity', 'Appliedcourse', 'Applieddate', 'Status', 'Link', 'SourceUniversity']))
        self.get_metadata_file().flush()
        data = {"email": "sreenivas.dega1@gmail.com",
                "password": "Headrun591!"}
        url = "https://yocket.in/users/login.json"
        yield Request(url, headers=self.request_headers, body=dumps(data), method="POST")

    def parse(self, response):
        source, content_type, crawl_type = self.get_source_content_and_crawl_type(
            self.name)
        requests = self.get_terminal_requests(content_type, [])
        return requests

    def parse_student_details(self, response):
        sel = Selector(response)
        sk = response.meta['sk']
        work_experiences, job_titles, company_names = [], [], []
        course_tests_dict, researches = {}, []
        profile_name = extract_data(
            sel, '//div[@class="col-sm-12"]//h1//strong/text()').strip()
        university = extract_data(
            sel, '//div[@class="col-sm-12 university-header"]//h3/text()')
        degree = extract_data(sel, '//p[@class="card-font"]//b//text()')
        cgpa = extract_data(sel, '//p[@class="teal"]//b//text()')
        skills = [item.strip() for item in extract_list_data(
            sel, '//div[@id="users-skills"]//div//h4/text()')]
        college = extract_data(
            sel, '//div[@class="col-sm-9 col-xs-9"]/p[@class="card-font"][not(contains(text(), "backlog"))]/text()')
        course = extract_data(
            sel, '//div[@class="col-sm-12"]/h4[not(contains(text(), "Looking for help?"))]/text()')
        year = ''.join(re.findall(r'\d+\d+', course))

        experience_nodes = get_nodes(
            sel, '//div[@id="work-experiences"]//div[contains(@class,"col-sm-12 border-top")]')
        for experience_node in experience_nodes:
            title = extract_data(
                experience_node, './/h4//text()').replace('\n', '').strip()
            time_period = extract_data(
                experience_node, './/div[@class="col-sm-12"]//i[@class="fa fa-clock"]/../span/text()')
            organisation = extract_data(
                experience_node, './/div[@class="col-sm-12"]//i[@class="fa fa-building"]/../span/text()')

            work_experiences.append(
                {'job_title': title, 'experience': time_period, 'company_name': organisation})
            job_titles.append(title)
            company_names.append(organisation)

        course_tests_nodes = get_nodes(
            sel, '//div[@class="col-sm-3 col-xs-3"]')
        for course_test_node in course_tests_nodes:
            key = extract_data(
                course_test_node, './h4/small[1]/text()').strip()
            value = extract_data(course_test_node, './h4/text()')

            scores = {}
            if "GRE" in key:
                sub_scores = extract_list_data(
                    course_test_node, './h4/span/text()')
                for sub_score in sub_scores:
                    section, score = sub_score.split(':')
                    scores.update({section.strip(): score.strip()})

            course_tests_dict.update({key: {'total_value': value}})
            if scores:
                course_tests_dict.get(key, {}).update({'sub_scores': scores})

        projects_nodes = get_nodes(
            sel, '//div[@id="projects"]/div[contains(@class,"col-sm-12 border-top")]')
        for project_node in projects_nodes:
            project_name = extract_data(
                project_node, './/h4[@class="card-header"]//text()').replace('\n', '')
            project_experience = extract_data(
                project_node, './/p[@class="card-font"]/i[@class="fa fa-clock"]/../span/text()')
            researches.append({'project_name': project_name,
                               'project_experience': project_experience})

        applied_universities_nodes = get_nodes(
            sel, '//div[@id="application-statuses-div"]//div[@class="table-responsive"]//tr')
        for applied_university_node in applied_universities_nodes:
            applied_university = extract_data(
                applied_university_node, './td/h4/a/text()')
            applied_course = extract_data(
                applied_university_node, './td/h4/a/small/text()')
            applied_date = extract_data(
                applied_university_node, './td//div[@class="col-sm-6"]/small[contains(text(), "Applied")]//text()')
            application_status = extract_data(
                applied_university_node, './td[@class="text-center "]/h4/span//text()').replace('\n', '')

            csv_item = CSVItem()
            gre_details = course_tests_dict.get(
                'GRE', {}) or course_tests_dict.get('GRE/GMAT', {})
            csv_item.update({
                "ProfileName": profile_name, "UndergradDegree": degree, "UndergradUniversity": college, "UndergradCgpa": cgpa,
                "Experience": course_tests_dict.get("Work Exp.", {}).get("total_value", ""), "WorkExperience": dumps(work_experiences),
                "CompanyName": ", ".join(company_names), "Jobtitle": ", ".join(job_titles), "Skills": ", ".join(skills),
                "Status": application_status, "Techpapers": course_tests_dict.get("Tech Papers", {}).get("total_value", ""),
                "Numberofresearch": dumps(researches), "InterestedTermandYear": year, "InterestedCourse": course,
                "GRETotalScore": gre_details.get("total_value", ""), "GREVerbalScore": gre_details.get("sub_scores", {}).get("Verbal", ""),
                "Applieduniversity": applied_university, "GREQuantScore": gre_details.get("sub_scores", {}).get("Quant", ""),
                "Appliedcourse": applied_course, "TOEFLSCORE": course_tests_dict.get("", {}).get("total_value", ""),
                "Applieddate": applied_date.split(':')[-1].strip(), "IELTSSCORE": course_tests_dict.get("IELTS", {}).get("total_value", ""),
                "Link": response.url, "SourceUniversity": university
            })

            yield csv_item

        if csv_item:
            self.got_page(sk, got_pageval=1)
