import time
import json
import ast
import pandas as pd
from pandas import DataFrame
from sqlalchemy import create_engine
from datetime import date
from selenium import webdriver
from selenium.webdriver.common.keys import Keys

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

def get_scroll_data(driver, left_data):
    dict_list=[]
    dict_details = {}
    for data in left_data:
        title_xpath = './/div[@class="BjJfJf PUpOsf"]'
        Job_title = data.find_element_by_xpath(title_xpath)
        if Job_title.text:
            job_details = data.find_element_by_xpath(title_xpath)
            driver.execute_script("arguments[0].click();", job_details)
            time.sleep(1)
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
            Additional_details = right_data.find_element_by_xpath('.//div[@class="ocResc icFQAc"]').text
            source_url_part1 = right_data.find_element_by_xpath('.//div[@class="B8oxKe"]/span/a').get_attribute('href')
            source_url_part2 = right_data.find_elements_by_xpath('.//div[@class="B8oxKe BQC79e"]/div/div/span/a')
            source_url_list = [url.get_attribute('href') for url in source_url_part2]
            source_url_list.append(source_url_part1)
               
            if Job_title:
                dict_details = {'Job_title' : Job_title,'Company' : Company, 'Job_Location' : Job_Location, 'Additional_details': Additional_details, 'Job_Url': source_url_list, 'Last_seen': date.today().isoformat()}
                dict_list.append(dict_details) #list of dict
    return dict_list
	         
def search_keyword(driver):
    company_list = get_company_name()
    keyword = "jobs"
    driver.get("https://www.google.com/search?q=" + keyword)
    driver.find_element_by_xpath('//*[@id="fMGJ3e"]/a').click()
    result = {}
    company_list = ['True Caller']
    for company_name in company_list:
        print(company_name)
        driver.find_element_by_id("hs-qsb").click()
        driver.find_element_by_id("hs-qsb").clear()
        driver.find_element_by_id("hs-qsb").send_keys(company_name,Keys.ENTER)
        location_xpath = './/span[@data-facet="city"]'
        location_detail_xpath = './/div[@data-display-value="Anywhere"]'
        time.sleep(3)
        driver.find_element_by_xpath(location_xpath).click()
        time.sleep(4)
        driver.find_element_by_xpath(location_detail_xpath).click()
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
       
        final_data = initial_data + final_data
        final_data = [ast.literal_eval(el1) for el1 in set([str(el2) for el2 in final_data])] 
        result[company_name] = final_data
        df = json.dumps(result)
        df = json.loads(df)
        path = '/home/jaffrin/json_files/'+company_name+'.json'
        with open(path, 'w') as outfile:
            json.dump(df, outfile, indent=4)

def tearDown(driver):
        time.sleep(2)
        driver.close()

setUp()
