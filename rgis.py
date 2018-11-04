from selenium import webdriver
from multiprocessing import Process, Manager, Queue, Pool, Value
import pandas as pd
import requests as r
from bs4 import BeautifulSoup as bs
import pandas as pd 
import re
from parse import *
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import ElementNotInteractableException
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.proxy import Proxy, ProxyType
from selenium.webdriver.common.action_chains import ActionChains
import os
import time
import binascii
import mammoth
import random
import json, codecs
#import ends here

failed = []
profile = webdriver.FirefoxProfile()
profile.set_preference('browser.download.folderList', 2)
profile.set_preference('browser.download.manager.showWhenStarting', False)
profile.set_preference('browser.download.dir', 'E:\w\rgis\base_data') 
profile.set_preference('browser.helperApps.neverAsk.saveToDisk', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document')
browser = webdriver.Firefox(firefox_profile=profile)
browser.get('https://reestr-gis.spb.ru/rgis/#gis')
mypath = 'E:/w/rgis/base_data/'

#scraping starts here
for j in range (2,7):
    time.sleep(10)
    input_ = browser.find_element_by_class_name('z-paging-input')
    input_.clear()
    input_.send_keys(str(j))
    input_.send_keys(Keys.ENTER)
    time.sleep(5)
    html = browser.page_source
    soup = bs(html, 'html.parser')
    c = []
    a = soup.find_all('a', {'style': "white-space: normal; text-align: left; padding: 5px 0;"})
    for b in a:
        c.append(b.get_text())
    print(c)
    for i in range(len(c)):
        m = c[i]
        print(m)
        input_ = browser.find_element_by_class_name('z-paging-input')
        input_.clear()
        input_.send_keys(str(j))
        input_.send_keys(Keys.ENTER)
        time.sleep(5)
        xpath = "//*[text()='{}']".format(m)
        browser.find_element_by_xpath(xpath).click()
        time.sleep(3)
        button = browser.find_elements_by_xpath("//*[contains(text(), ' Экспорт сведений')]")
        browser.execute_script("window.scrollTo(0, 0)") 
        button[0].click()
        time.sleep(5)
        try:
            browser.switch_to.window(browser.window_handles[1])
            print('fail')
            browser.close() 
            browser.switch_to.window(browser.window_handles[0])
            failed.append(m)
        except IndexError:
            print('done')
        browser.back()  #вместо бэка возврат через инпут
        time.sleep(3)
#scrappings ends here        
        
#work with data
onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
dfs = [[] for _ in range(len(onlyfiles))]
file = '.doc'
for i in range(len(onlyfiles)):
    if file in onlyfiles[i]:
        docx_file = open("E:/w/rgis/base_data/" + onlyfiles[i], "rb")
        result = mammoth.convert_to_html(docx_file)
        html = result.value # The generated HTML
        messages = result.messages # Any messages, such as warnings during conversion
        soup = bs(html, "html.parser")
        tables = []
        a = soup.find_all('table')
        for b in a:
            tables.append(b)
            
    for k in range (len(tables)):
        dfs[i].extend(pd.read_html(str(tables[k])))    

gis_name = []
a = 'Наименование ГИС'
for i in range(len(dfs)):
    for index, row in dfs[i][0].iterrows():
        for row in row:
            try:
                if a in row:
                    gis_name.append(dfs[i][0][2][index])
            except TypeError:
                continue
#шингловая магия                
result = {el:[[] for x in range(len(dfs[0]))] for el in gis_name}
for n in range(len(dfs)):
    for j in range(len(dfs)):
        for g in range(len(dfs[0])):
        #шингловая магия
            str1 = dfs[n][g].to_string(na_rep = '')
            str1 = str1.replace('\n','')
            str1 = re.sub(r'\s+', ' ', str1)
            str2 = dfs[j][g].to_string(na_rep = '')
            str2 = str2.replace('\n','')
            str2 = re.sub(r'\s+', ' ', str2)
            current_gis_name = gis_name[g]
            stop_symbols = '.,!?:;-\n\r()'
            stop_words = (' ')
            result1 = [x for x in [y.strip(stop_symbols) for y in str1.lower().split()] if x and (x not in stop_words)]
            result2 = [x for x in [y.strip(stop_symbols) for y in str2.lower().split()] if x and (x not in stop_words)] 
            shingleLen = 3 #длина шингла
            cmp1 = [] 
            for i in range(len(result1)-(shingleLen-1)):
                cmp1.append (binascii.crc32(' '.join( [x for x in result1[i:i+shingleLen]] ).encode('utf-8'))) # хэширование
            cmp1 = list(set(cmp1))
            cmp2 = [] 
            for i in range(len(result2)-(shingleLen-1)):
                cmp2.append (binascii.crc32(' '.join( [x for x in result2[i:i+shingleLen]] ).encode('utf-8')))
            same = 0 #надо
            cmp2 = list(set(cmp2))
            for i in range(len(cmp1)):
                if cmp1[i] in cmp2:
                    same +=1
            
            magic_value = same*2/float(len(cmp1) + len(cmp2))*100
            result[gis_name[n]][j].append({current_gis_name:magic_value})
        


with open('E:/w/rgis/result.json', 'wb') as f:
    json.dump(result, codecs.getwriter('utf-8')(f), ensure_ascii=False)