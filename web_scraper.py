from bs4 import BeautifulSoup
from selenium import webdriver
import time
import requests
import concurrent.futures
import csv
import pandas as pd

##------------------------ changes in this part only --------------------------------- ##

driver = webdriver.Chrome(executable_path=r'C:/tools/drivers/chromedriver.exe')
driver.get('https://www.pmindia.gov.in/en/tag/pmspeech/')
# https://stackoverflow.com/a/27760083

SCROLL_PAUSE_TIME = 5.0
# Get scroll height
last_height = driver.execute_script("return document.body.scrollHeight")
print('height',last_height)

while True:
    # Scroll down to bottom
    time.sleep(SCROLL_PAUSE_TIME)

    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

    # Wait to load page
    time.sleep(SCROLL_PAUSE_TIME)

    # Calculate new scroll height and compare with last scroll height
    new_height = driver.execute_script("return document.body.scrollHeight")
    print('height',new_height)
    if new_height == last_height:
        time.sleep(SCROLL_PAUSE_TIME+ 5.0)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        else:
            continue

    last_height = new_height
    

res = driver.execute_script("return document.documentElement.outerHTML")
driver.quit()

soup = BeautifulSoup(res, 'lxml')

box = soup.find('ul', {'class': 'news-holder'})

all_speeches = box.find_all('li', {'class': '6u'})
description=[]
date=[]
links=[]
for speeches in all_speeches:
    description.append(speeches.find('a').text.replace('\n', ''))
    date.append(speeches.find('span', {'class': 'date'}).text.replace('\n', ''))
    links.append(speeches.find('a').attrs["href"])
print('We have scrapped ',len(date),'number of lines')


with open('links.csv', 'w') as myfile:
     wr = csv.writer(myfile)
     for link in links:
         wr.writerow(link)

with open('dates.csv', 'w') as myfile:
     wr = csv.writer(myfile)
     for day in date:
         wr.writerow(day)        
         
with open('description.csv', 'w') as myfile:
     wr = csv.writer(myfile)
     for descrip in description:
         wr.writerow(descrip)        
         
        
# =============================================================================
# text={}
# for link in links:
#     try:
#         response = requests.get(link)
#         soup = BeautifulSoup(response.text, "lxml")
#         soup=soup.find('div', {'class':'news-bg'})
#         text[link]=soup.text
#     except:
#         print(link,'not opening')
#         text[link]='content not found!'
#         pass
# #this takes a lot of time, instead you can use  multi threading
# print("===Done====")
# 
# =============================================================================

#https://beckernick.github.io/faster-web-scraping-python/


MAX_THREADS = 30
text={}
def download_url(link):
    try:
        response = requests.get(link)
        soup = BeautifulSoup(response.text, "lxml")
        soup=soup.find('div', {'class':'news-bg'})
        text[link]=soup.text
        print(link,'done!')
    except:
        print(link,'not opening')
        text[link]='content not found!'
        pass
        
    time.sleep(0.2)
    
def download_stories(story_urls):
    threads = min(MAX_THREADS, len(story_urls))
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        executor.map(download_url, story_urls)

t0 = time.time()
download_stories(links)
t1 = time.time()
print(f"{t1-t0} seconds to download {len(links)} stories.")


print(text[links[1]])

df=pd.DataFrame([links,date,description])
print(df.shape)
df=df.transpose()
print(df.shape)

df.columns=['link','date','description']

df['text']=df.link.map(text)


print(df.shape)


print(df.head(1))

df.to_csv('narendra_modi_speeches.csv')