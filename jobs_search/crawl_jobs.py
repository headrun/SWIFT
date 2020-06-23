import time
import json,csv
import ast
import pandas as pd
from pandas import DataFrame
from sqlalchemy import create_engine
from datetime import date
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
import datetime
from dateutil.relativedelta import relativedelta
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By


def setUp():
        chromedriver = "/home/jaffrin/Downloads/chromedriver"
        driver = webdriver.Chrome(chromedriver)
        driver.maximize_window()
        search_keyword(driver)
        tearDown(driver)

def get_company_name():
        con = create_engine('mysql+mysqldb://root:root@localhost/jobs_search')
        df = pd.read_sql_query('''select company from company''', con)
        company_list = df['company'].tolist()
        return company_list

def get_past_date(str_days_ago):
    TODAY = datetime.date.today()
    splitted = str_days_ago.split()
    if len(splitted) == 1 and splitted[0].lower() == 'today':
        return str(TODAY.isoformat())
    elif len(splitted) == 1 and splitted[0].lower() == 'yesterday':
        date = TODAY - relativedelta(days=1)
        return str(date.isoformat())
    elif splitted[1].lower() in ['hour', 'hours', 'hr', 'hrs', 'h']:
        date = datetime.datetime.now() - relativedelta(hours=int(splitted[0]))
        return str(date.date().isoformat())
    elif splitted[1].lower() in ['day', 'days', 'd']:
        date = TODAY - relativedelta(days=int(splitted[0]))
        return str(date.isoformat())
    elif splitted[1].lower() in ['wk', 'wks', 'week', 'weeks', 'w']:
        date = TODAY - relativedelta(weeks=int(splitted[0]))
        return str(date.isoformat())
    elif splitted[1].lower() in ['mon', 'mons', 'month', 'months', 'm']:
        date = TODAY - relativedelta(months=int(splitted[0]))
        return str(date.isoformat())
    elif splitted[1].lower() in ['yrs', 'yr', 'years', 'year', 'y']:
        date = TODAY - relativedelta(years=int(splitted[0]))
        return str(date.isoformat())
    else:
        return "Wrong Argument format"

def get_scroll_data(driver, left_data):
    dict_list=[]
    dict_details = {}
    for data in left_data:
        title_xpath = './/div[@class="BjJfJf PUpOsf"]'
        Job_title = data.find_element_by_xpath(title_xpath)
        if Job_title.text:
            job_details = data.find_element_by_xpath(title_xpath)
            driver.execute_script("arguments[0].click();", job_details)
            time.sleep(2)
            right_container_xpath = '//div[@class="pE8vnd avtvi"]'
            right_data = driver.find_elements_by_xpath(right_container_xpath)
            right_data = right_data[len(right_data)-1]
            job_details = right_data.find_element_by_xpath('.//div[@class="OghIW"]').text.split('\n')
            try:
                Job_title = job_details[0]
                Company = right_data.find_element_by_xpath('.//div[@class="nJlQNd sMzDkb"]').text
                Job_Location = right_data.find_element_by_xpath('.//div[@class="sMzDkb"]').text
            except:
                Job_title, Company, Job_Location = ['']*3
            read_more_xpath = './/div[@class="CdXzFe j4kHIf"]'
            try:
                if right_data.find_element_by_xpath(read_more_xpath).text == 'READ MORE':
                    element = right_data.find_element_by_xpath(read_more_xpath).click()
            except NoSuchElementException:
                print("No ReadMore Found")

            description = right_data.find_element_by_xpath('.//div[@class="YgLbBe"]').text
            description = description.encode('ASCII','ignore').decode('UTF-8')
            Additional_details = right_data.find_element_by_xpath('.//div[@class="ocResc icFQAc"]').text.split('\n')
            if Additional_details[0] == '':
                try:
                    element = WebDriverWait(driver, 10).until_not(EC.presence_of_element_located((By.XPATH, './/div[@class="ocResc icFQAc"]')))
                except: continue
            Additional_details = right_data.find_element_by_xpath('.//div[@class="ocResc icFQAc"]').text.split('\n')
            posted_on_det = [det.strip('Over ') for det in Additional_details if 'ago' in det]
            posted_on = get_past_date(posted_on_det[0]) if posted_on_det else ''
            job_type = [j for j in Additional_details if 'time' in j or 'Internship' in j]
            job_type = job_type[0] if job_type else ''
            job_type = job_type.encode('ASCII','ignore').decode('UTF-8')
            source_url_part1 = right_data.find_element_by_xpath('.//div[@class="B8oxKe"]/span/a').get_attribute('href')
            source_url_part2 = right_data.find_elements_by_xpath('.//div[@class="B8oxKe BQC79e"]/div/div/span/a')
            source_url_list = [url.get_attribute('href') for url in source_url_part2]
            source_url_list.append(source_url_part1)
               
            if Job_title:
                dict_details = {'Job_Title' : Job_title, 'Company_Name' : Company, 'Job_Location' : Job_Location,
                'Posted_On': posted_on, 'Job_Type': job_type, 'Job_Url': source_url_list, 'Industry' : 'IT', 'Functional_Area' : '',
                'Description': description}
                dict_list.append(dict_details) #list of dict
    return dict_list
	         
def search_keyword(driver):
    company_list = get_company_name()
    keyword = "jobs"
    driver.get("https://www.google.com/search?q=" + keyword)
    driver.find_element_by_xpath('//*[@id="fMGJ3e"]/a').click()
    for company_name in company_list:
        print(company_name)
        driver.find_element_by_id("hs-qsb").click()
        driver.find_element_by_id("hs-qsb").clear()
        driver.find_element_by_id("hs-qsb").send_keys(company_name,Keys.ENTER)
        location_xpath = './/span[@data-facet="city"]'
        location_detail_xpath = './/div[@data-display-value="Anywhere"]'
        city_xpath = './/div[@data-name="Bengaluru, KA"]'
        time.sleep(3)
        driver.find_element_by_xpath(location_xpath).click()
        time.sleep(4)
        driver.find_element_by_xpath(location_detail_xpath).click()
        time.sleep(4)
        driver.find_element_by_xpath(city_xpath).click()
        time.sleep(4)
        left_bar_xpath = './/div[@class="Fol1qc"]'
        first, second = 1, 0
        init_left_data = driver.find_elements_by_xpath(left_bar_xpath)
        initial_data = get_scroll_data(driver,init_left_data) #intial 20 records
        final_data = []
        while first - second:
            scr1 = driver.find_element_by_xpath('//*[@id="immersive_desktop_root"]/div/div[3]/div[1]')
            time.sleep(4)
            driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", scr1)
            time.sleep(4)
            left_data = driver.find_elements_by_xpath(left_bar_xpath)
            left_data = left_data[:-1] #Removing the repeatative data
            print ('first:%s'%(first), 'second:%s'%(second))
            print(len(left_data))
            second = first
            first = len(left_data)
            scrolled_data = get_scroll_data(driver,left_data) # Every scroll records
            final_data = final_data + scrolled_data

        result = {}
        final_data = initial_data + final_data
        final_data = [ast.literal_eval(el1) for el1 in set([str(el2) for el2 in final_data])] 
        result[company_name] = final_data
        df = json.dumps(result)
        df = json.loads(df)
        csv_file = '/home/jaffrin/json_files/'+company_name+'.csv'
        reorder_file = csv_file = '/home/jaffrin/files/'+company_name+'.csv'
        path = '/home/jaffrin/files/'+company_name+'.json'
        with open(path, 'w') as outfile:
            json.dump(df, outfile, indent=4)
        with open(path) as json_file: 
            data = json.load(json_file) 
        job_data = data['PHP developer'] 
        data_file = open(csv_file, 'w') 
        csv_writer = csv.writer(data_file) 
        count = 0
        for job in job_data: 
            if count == 0: 
                header = job.keys()
                csv_writer.writerow(header) 
                count += 1
            csv_writer.writerow(job.values()) 
        data_file.close()
        # Rearranging the columns in csv file
        df = pd.read_csv(csv_file)
        df_reorder = df[['Company_Name', 'Job_Title', 'Functional_Area', 'Industry', 'Job_Location', 'Posted_On', 'Job_Type', 'Job_Url', 'Description']] # rearrange column here
        df_reorder.to_csv(reorder_file, index=False)

def tearDown(driver):
        time.sleep(2)
        driver.close()

setUp()
