import time
from datetime import date
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

def setUp():
        chromedriver = "/home/jaffrin/Downloads/chromedriver"
        driver = webdriver.Chrome(chromedriver)
        driver.maximize_window()
        search_keyword(driver)
        tearDown(driver)

def search_keyword(driver):
        keyword = "jobs"
        driver.get("https://www.google.com/search?q=" + keyword)
        driver.find_element_by_xpath('//*[@id="fMGJ3e"]/a').click()
        driver.find_element_by_id("hs-qsb").click()
        driver.find_element_by_id("hs-qsb").clear()
        driver.find_element_by_id("hs-qsb").send_keys("accenture jobs",Keys.ENTER)
        location_xpath = '//*[@id="choice_box_root"]/div[1]/div[1]/span[3]'
        location_detail_xpath = ('.//div[@class="TRwkpf GbaVB yjYmLb"]')
        time.sleep(2)
        driver.find_element_by_xpath(location_xpath).click()
        time.sleep(2)
        driver.find_element_by_xpath(location_detail_xpath).click()
        left_bar_xpath = './/div[@class="Fol1qc"]'
        while True:
            scr1 = driver.find_element_by_xpath('//*[@id="immersive_desktop_root"]/div/div[3]/div[1]')
            driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", scr1)
            time.sleep(2)
            left_data = driver.find_elements_by_xpath(left_bar_xpath)
            print(len(left_data))
            dict_details = {}
            for data in left_data:
                title_xpath = './/div[@class="BjJfJf PUpOsf"]'
                Job_title = data.find_element_by_xpath(title_xpath)
                if Job_title.text:
                    comp_details = data.find_element_by_xpath('.//div[@class="oNwCmf"]').text.split('\n')
                    Company = comp_details[0]
                    try:
                        Job_Location = comp_details[1]
                        Updated_via = comp_details[2].strip('via ')
                        Updated_On = comp_details[3]
                    except: continue
                    if len(comp_details) > 4:
                        Job_Type_Package = comp_details[4]
                    else:
                        Job_Type_Package = ''
                    time.sleep(1)
                    job_details = data.find_element_by_xpath(title_xpath)
                    driver.execute_script("arguments[0].click();", job_details)
                    time.sleep(1)
                    source_url_xpath = './/a[@class="pMhGee j0vryd"]'
                    urls = driver.find_elements_by_xpath(source_url_xpath)
                    source_urls = [url.get_attribute('href') for url in urls]
                    #print(source_urls)
                    if Job_title.text:
                        dict_details = {'Job_title' : Job_title.text,'Comapny' : Company, 'Job_Location' : Job_Location, 'Updated_via' : Updated_via,'Posted_On' : Updated_On,'Job_Type_Package' : Job_Type_Package, 'Updated_at': date.today().isoformat()}
                        print(dict_details)
def tearDown(driver):
        time.sleep(2)
        driver.close()

setUp()
