import multiprocessing as mp
from multiprocessing import Manager
import time
import pandas as pd
from bs4 import BeautifulSoup
import re
import requests
from urllib.request import Request, urlopen
from selenium import webdriver
from lxml import etree
from urllib import parse




def download_and_parse_web(web):   
    req=Request(web,headers={'User-Agent':'Mozilla/5.0'})  
    downloaded_web=urlopen(req).read()
    soup=BeautifulSoup(downloaded_web)  
    return soup    


import re
def detail_information(soup): 
    a=[]
    b= soup.find('i',attrs={'class':'house_code'}).text  
    a.append(b)
    b= soup.find('p',attrs={'class':'content__title'}).text.strip().split(' ')[0]
    a.append(b)
    b= soup.find('p',attrs={'class':'content__title'}).text.strip().split(' ')[1]
    a.append(b)
    
    b= soup.find('div',attrs={'class':'content__aside--title'}).find('span').text.strip()
    a.append(b)
    
    try:
        b = re.findall( r'\(.*\)',soup.find('div',attrs={'class':'content__aside--title'}).get_text())[0]
    except:
        a.append('无信息')
    else:
        a.append(b)
 
    date_next = soup.find(string='租赁方式：')
    b = date_next.parent.parent.get_text().split('：')[1]
    a.append(b)
    
    b = soup.find('li',string=re.compile("面积")).get_text().split('：')[1]
    a.append(b)
    b = soup.find('li',string=re.compile("朝向")).get_text().split('：')[1]
    a.append(b)
    b = soup.find('li',string=re.compile("维护")).get_text().split('：')[1]
    a.append(b)
    b = soup.find('li',string=re.compile("楼层")).get_text().split('：')[1]
    a.append(b)
    b = soup.find('li',string=re.compile("车位")).get_text().split('：')[1]
    a.append(b)
    b = soup.find('li',string=re.compile("租期")).get_text().split('：')[1]
    a.append(b)
    b = soup.find('li',string=re.compile("入住")).get_text().split('：')[1]
    a.append(b)
    b = soup.find('li',string=re.compile("电梯")).get_text().split('：')[1]
    a.append(b)
    
    try:
        b = soup.find('p',attrs={'data-el':"houseComment"})['data-desc']
    except:
        a.append('无信息')
    else:
        a.append(b)
    
    try:
        b = soup.find('span',string=re.compile("号线")).get_text()  
    except:
        a.append('无信息')
    else:
        a.append(b)
        
    try:
        b = soup.find('span',string=re.compile("号线")).parent.get_text().strip().split('\n')[2]  
    except:
        a.append('无信息')
    else:
        a.append(b)
    return a


def scrawle(web,num,room_info):
    print(web)
    soup=download_and_parse_web(web)
    temp=detail_information(soup)
    temp.append(web)
    room_info[num] = temp
    print('第'+str(num+1)+'个网站爬取成功')
    


if __name__ == '__main__':
    import os
    room_info=pd.DataFrame(columns=['房源编号','名称','户型','月租','付款方式','租赁方式','面积','朝向','维护'
                                ,"楼层","车位",'租期','入住','电梯','描述','地铁站','离地铁站距离','网站'])

    web=pd.read_csv('web.csv')   #此处导入每个房源的链接信息
    manager = Manager()
    room_info = manager.dict()
    jobs = []
    s_time = time.time()
    for i in range(0,200):      #获取前200个房源的信息   
        p = mp.Process(target=scrawle, args=(web['网站'][i],i,room_info))
        jobs.append(p)
        p.start()
    for proc in jobs:
        proc.join()

    e_time = time.time()
    print('the program runs for {} seconds'.format(e_time-s_time))
    df = pd.DataFrame.from_dict(room_info, orient='index',columns=['房源编号','名称','户型','月租','付款方式','租赁方式','面积','朝向','维护'
                             ,"楼层","车位",'租期','入住','电梯','描述','地铁站','离地铁站距离','网站'])
    print(df)
    df.to_csv('多线程爬取链家房源信息.csv',index=False,encoding='utf-8-sig')


