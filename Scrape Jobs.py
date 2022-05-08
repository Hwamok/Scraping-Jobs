# -*- coding: utf-8 -*-

import requests 
import numpy as np
from bs4 import BeautifulSoup
import pandas as pd
import pymysql 
pymysql.install_as_MySQLdb()
from sqlalchemy import create_engine

# User-Agent가 적힌 파일 불러오기  
dir = r"User-Agent가 적힌 txt 파일의 경로"

open_txt = open(dir + "user-agent.txt")
for line in open_txt :
    user_agent = line 

headers = {"User-Agent" : user_agent}
def access_res(url) : 
  res = requests.get(url, headers = headers)
  res.raise_for_status()
  soup = BeautifulSoup(res.text, 'lxml')

  return soup

base_url = "https://www.jobkorea.co.kr/Search/?stext=%EB%8D%B0%EC%9D%B4%ED%84%B0%20%EB%B6%84%EC%84%9D&dkwrd=10001092949&tabType=recruit&Page_No="

# 총 검색 건수 
soup = access_res(base_url)
total_nb = int(soup.find("strong", attrs = {"class" : "dev_tot"}).get_text().replace(",",""))
total_page = np.ceil(total_nb/20).astype(int)

def process_blank(elem) :
  if type(elem) == type(soup.find("-----")) : 
    out = "None"
  else : 
    out = elem.get_text()
  return out 

corp_list = [] 
title_list = []
time_list = []
career_list = []
degree_list = []
loc_list = []
date_list = []
key_list = []
url_list = []

for i in list(range(1, total_page+1)) :
  None_sign = soup.find("div", attrs = {"class" : "title"}).p.get_text()
  
  soup = access_res(base_url + str(i))
  corp_page = soup.find_all("a", attrs = {"class" : "name dev_view"})
  title_page = soup.find_all("a", attrs = {"class" : "title dev_view"})
  info_page = soup.find_all("p", attrs = {"class" : "option"})
  key_page = soup.find_all("p", attrs = {"class" : "etc"})
  url_page = soup.find_all("div", attrs = {"class" : "post-list-info"})
  
  print("\n{} ________________________________________________".format(i))
  
  for t in range(20) :
    try : 
      corp = corp_page[t].get_text()
      title = process_blank(title_page[t]).replace("\n", "").replace("\r", "").strip()
      time = process_blank(info_page[t].find_all("span")[2])
      career = process_blank(info_page[t].find("span", attrs = {"class" : "exp"}))
      degree = process_blank(info_page[t].find("span", attrs = {"class" : "edu"}))
      loc = process_blank(info_page[t].find("span", attrs = {"class" : "loc long"}))
      date = process_blank(info_page[t].find("span", attrs = {"class" : "date"}))
      key = process_blank(key_page[t])
      url = "https://www.jobkorea.co.kr/" + url_page[t].a["href"]
    except : 
      break 
    
    else : 
      print("{}번째, {} / {} / {} / {} / {} / {} / {} / {} / {}".format(t, corp, title, time, career, degree, loc, date, key, url))
      corp_list.append(corp) ; title_list.append(title) ; time_list.append(time) ; career_list.append(career) ; 
      degree_list.append(degree) ; loc_list.append(loc) ; date_list.append(date) ; key_list.append(key) ; url_list.append(url) 

print("\n---- 탐색 종료 ----")

scrape_dic = {"corp" : corp_list, 
              "title" : title_list,
              "time" : time_list,
              "career" : career_list,
              "degree" : degree_list,
              "loc" : loc_list,
              "date" : date_list,
              "key" : key_list,
              "url" : url_list }

scrape_df = pd.DataFrame(scrape_dic)
print("\nShape of DF : {}".format(scrape_df.shape))

key_collection = [] 
for keys in key_list :
  splited_key = keys.split(", ")
  key_collection += splited_key

key_df = pd.DataFrame({"keyword" : key_collection})

mysql = pymysql.connect(host = 'MySQL 호스트 주소', port = 3306, user = 'MySQL ID', password = 'MySQL Password', db = "데이터베이스명")
cursor = mysql.cursor()
try :
  cursor.execute("drop table jobkor_table ;")
except Exception as err : 
  print(err)

try :
  cursor.execute("drop table jobkor_keywords ;")
except Exception as err : 
  print(err)

engine = create_engine("mysql://Mok:" + "599125" + "@127.0.0.1/jobscraping", encoding = 'utf-8')

scrape_df.to_sql("jobkor_table", con = engine, index = False)
key_df.to_sql("jobkor_keywords", con = engine, index = False)
#scrape_df.to_csv(dir + "DA_Scraping_data.csv", index = False, encoding = "utf-8")

cursor.execute("""
  update jobkor_table 
  set loc = replace(loc, ' 외', ' ') 
  where loc like '% 외' ;
""") 

cursor.execute(""" 
  update jobkor_table 
  set time = 
  case 
  when time like '%외' then '복합'
  when time like '정규직' then '정규'
  when time like '인턴' then '인턴'
  when time like '계약직' then '계약'
  else '기타'
  end ; 
""")

cursor.execute("""
  update jobkor_table 
  set career = 
  case 
  when career like "신입·경력%" then "무관"
  when career like "%년%" then "경력" 
  else "신입"
  end ; 
""")  

cursor.execute(""" 
  update jobkor_table 
  set degree = 
  case 
  when degree like '초대졸%' then '전문대'
  when degree like '대졸%' then '학사'
  when degree like '석사%' then '석사'
  when degree like '박사%' then '박사'
  else '무관'
  end ; 
""")

mysql.close()
print("MySQL 처리 완료")

