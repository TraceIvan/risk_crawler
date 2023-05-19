from myLog import MyLog as mylog
from queue import Queue
import os
import requests
import time
from multiprocessing.dummy import Pool as ThreadPool
from bs4 import BeautifulSoup
import re
import csv
import pandas as pd
from urllib import request
import urllib3
import shutil
import pickle

THREADS = 16
HEADER = {
          "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
          }
PROXIES = {
    'https': 'https://127.0.0.1:7890',
    'http': 'http://127.0.0.1:7890'
}
LOG_DIR='./log_files/'
EPOCH_DIR='./start_epochs/'
class znWiki(object):
    def __init__(self,log_file):
        self.log = mylog(LOG_DIR+log_file)
        self.timeout = 10
        self.readtimeout = 30
        self.request_sleep = 1
        self.sep = '\t'
        self.opener = request.build_opener(request.ProxyHandler(PROXIES))
        request.install_opener(self.opener)
        self.get_list_from_csv_3()


    def getListOfMovie(self, url):
        htmlContent = self.getResponseContent(url)
        if htmlContent == None:
            return
        soup = BeautifulSoup(htmlContent, 'lxml')
        tables = soup.find_all("table", attrs={"class": "wikitable"})
        trs = []
        for i in tables:
            trs.extend(i.find_all("tr"))
        movies = []
        for tr in trs:
            chs = list(tr.children)
            if chs[1].find('a'):
                cur = chs[1].a
                if "title" in cur.attrs.keys():
                    if re.search("页面不存在", cur.attrs["title"]) == None:
                        movies.append([cur.attrs["href"], cur.attrs["title"]])
        return movies

    def getLists1(self):
        url = "https://wiki.tw.wjbk.site/baike-https://wiki.tw.wjbk.site/w/index.php?title=Category:2013%E5%B9%B4%E7%94%B5%E5%BD%B1&pagefrom=%E6%84%9B%E6%83%85%E7%84%A1%E5%85%A8%E9%A0%86#mw-pagesCategory:%E5%90%84%E5%B9%B4%E7%94%B5%E5%BD%B1%E5%88%97%E8%A1%A8"
        htmlContent = self.getResponseContent(url)
        if htmlContent == None:
            return
        soup = BeautifulSoup(htmlContent, 'lxml')
        lists = soup.find_all("li", text=re.compile("列表"))
        tmp = []
        for i in lists:
            tmp.append(i.a.attrs["href"])
        return tmp

    def getResponseContent(self, url):
        time.sleep(self.request_sleep)
        cnt=0
        while True:
            try:
                #response = requests.get(url, headers=self.Header,proxies=self.proxy)#timeout=(self.timeout, self.readtimeout)
                req = request.Request(url, headers=HEADER)
                response = request.urlopen(req,timeout=self.timeout)
                if int(response.code) != 200:
                    self.log.error("%s :%d!" % (url, int(response.code)))
                    if int(response.code) == 404:
                        return None
                    if int(response.code) == 500:
                        return None
                    break
                html_text=response.read().decode('utf8')
            except Exception as e:
                self.log.error("%s : %s"%(url,e))
                cnt+=1
                try:
                    if e.code==404:
                        return None
                except Exception as e2:
                    pass
                # if cnt>20:
                #     return None
            else:
                self.log.info("返回url:%s 成功, status_code:%d" % (url, int(response.code)))
                return html_text
        self.log.error("返回url:%s 失败" % url)
        return None

    def getLists(self):
        save_csv = 'event_all_2.csv'
        if os.path.exists(save_csv):
            data = pd.read_csv(save_csv, sep='\t')
            urls = data['url'].values.tolist()
            titles = data['title'].values.tolist()
            tot_urls = list(zip(urls, titles))
            return tot_urls
        base_url = [
            'https://zh.wikipedia.org/wiki/Category:%E6%94%BF%E6%B2%BB%E9%9A%90%E5%96%BB',
            'https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%9B%BD%E6%94%BF%E6%B2%BB',
            'https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%9B%BD%E6%94%BF%E6%B2%BB%E4%BA%8B%E4%BB%B6',
            'https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%9B%BD%E7%A4%BE%E4%BC%9A%E8%BF%90%E5%8A%A8',
            'https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%8D%8E%E4%BA%BA%E6%B0%91%E5%85%B1%E5%92%8C%E5%9B%BD%E4%BA%8B%E6%95%85',
            'https://zh.wikipedia.org/zh-hans/Category:%E4%B8%AD%E5%8D%8E%E4%BA%BA%E6%B0%91%E5%85%B1%E5%92%8C%E5%9B%BD%E7%81%BE%E9%9A%BE',
            'https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%9B%BD%E5%AE%97%E6%95%99%E4%BA%8B%E4%BB%B6',
            'https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%9B%BD%E6%81%90%E6%80%96%E4%B8%BB%E4%B9%89',
            'https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%9C%8B%E5%88%86%E9%9B%A2%E4%B8%BB%E7%BE%A9',
            'https://zh.wikipedia.org/wiki/Category:%E6%81%90%E6%80%96%E7%B5%84%E7%B9%94',
            'https://zh.wikipedia.org/wiki/Category:%E9%82%AA%E6%95%99',
            'https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%8D%8E%E4%BA%BA%E6%B0%91%E5%85%B1%E5%92%8C%E5%9B%BD%E8%A2%AB%E7%A6%81%E5%BD%B1%E8%A7%86%E4%BD%9C%E5%93%81',
        ]
        viewed = set()
        for i in range(len(base_url)):
            viewed.add(base_url[i])
        queue_movie = Queue(maxsize=0)
        for i in range(len(base_url)):
            queue_movie.put(base_url[i])
        Tot_urls = []
        if not os.path.exists(save_csv):
            with open(save_csv, 'w', encoding="utf8", newline='') as fi:
                writer = csv.writer(fi, delimiter=self.sep)
                writer.writerow(["url", "title", "pre_url", "isleaf"])
        while not queue_movie.empty():
            self.log.info('tot get url:%d' % (len(Tot_urls)))
            tot_urls = []
            cur_url = queue_movie.get()
            htmlContent = self.getResponseContent(cur_url)
            if htmlContent == None:
                continue
            soup = BeautifulSoup(htmlContent, 'lxml')
            subcate = soup.find('div', attrs={"id": "mw-subcategories"})
            if subcate != None:
                uls = subcate.find_all("ul")
                for ul in uls:
                    lis = ul.find_all('li')
                    for cur_li in lis:
                        alink = cur_li.find('a')
                        href = 'https://zh.wikipedia.org' + alink.attrs['href']
                        tot_urls.append([href, alink.attrs['title'], cur_url, 0])
                        if href not in viewed:
                            self.log.info('subcate:%s' % alink.attrs['title'])
                            if href!="https://zh.wikipedia.org/wiki/Category:%E4%BD%BF%E7%94%A8%E9%83%A8%E9%A6%96%E7%9A%84%E7%AD%86%E5%8A%83%E6%95%B8%E4%BD%9C%E7%82%BA%E6%8E%92%E5%BA%8F%E9%8D%B5%E5%80%BC%E7%9A%84%E5%88%86%E9%A1%9E":
                                queue_movie.put(href)
                            viewed.add(href)


                next_page = subcate.find('a', text=re.compile('下一页'))
                if next_page == None:
                    next_page = subcate.find('a', text=re.compile('下壹頁'))
                if next_page != None:
                    href = 'https://zh.wikipedia.org' + next_page.attrs['href']
                    tot_urls.append([href, next_page.attrs['title'], cur_url, 0])
                    if href not in viewed:
                        queue_movie.put(href)
                        viewed.add(href)
                        self.log.info('%s:下一页' % href)

            mwpages = soup.find('div', attrs={"id": "mw-pages"})
            if mwpages != None:
                uls = mwpages.find_all("ul")
                for ul in uls:
                    lis = ul.find_all('li')
                    for cur_li in lis:
                        href = 'https://zh.wikipedia.org' + cur_li.a.attrs['href']
                        title = cur_li.a.attrs['title']
                        if [href, title, cur_url, 1] not in Tot_urls:
                            tot_urls.append([href, title, cur_url, 1])
                            self.log.info("get page:%s" % title)
                next_page = mwpages.find('a', text=re.compile('下一页'))
                if next_page == None:
                    next_page = mwpages.find('a', text=re.compile('下壹頁'))
                if next_page != None:
                    href = 'https://zh.wikipedia.org' + next_page.attrs['href']
                    tot_urls.append([href, next_page.attrs['title'], cur_url, 0])
                    if href not in viewed:
                        queue_movie.put(href)
                        viewed.add(href)
                        self.log.info('%s:下一页' % href)

            Tot_urls.extend(tot_urls)
            with open(save_csv, 'a+', encoding="utf8", newline='') as fi:
                writer = csv.writer(fi, delimiter=self.sep)
                writer.writerows(tot_urls)
        num_urls = len(Tot_urls)
        self.log.info("tot urls:%d" % num_urls)
        # urls, names,pres,isleaf = [], [],[],[]
        # for i in range(num_urls):
        #     urls.append(Tot_urls[i][0])
        #     names.append(Tot_urls[i][1])
        #     pres.append(Tot_urls[i][2])
        #     isleaf.append(Tot_urls[i][3])
        # df = pd.DataFrame({'url': urls, 'title': names,'pre_url':pres,'isleaf':isleaf})
        # df.to_csv('Allevents.csv', sep='\t', encoding='utf8')
        # self.log.info("save Allevents.csv complete!")
        return Tot_urls

    def get_list_2(self):
        lists=[
            # ["https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%9C%8B%E6%94%BF%E6%B2%BB%E8%BF%AB%E5%AE%B3","中国政治迫害"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%9B%BD%E4%BA%BA%E6%9D%83","中国人权"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%9B%BD%E5%8F%B3%E7%BF%BC%E6%94%BF%E6%B2%BB","中国右翼政治"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%9B%BD%E5%AE%97%E6%95%99%E4%B8%8E%E6%94%BF%E6%B2%BB","中国宗教与政治"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%9B%BD%E6%81%90%E6%80%96%E4%B8%BB%E4%B9%89","中国恐怖主义"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%9B%BD%E6%94%BF%E6%B2%BB%E4%BA%89%E8%AE%AE","中国政治争议"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%9B%BD%E6%94%BF%E6%B2%BB%E6%9C%AF%E8%AF%AD","中国政治术语"],
            #   ["https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%9B%BD%E6%94%BF%E6%B2%BB%E8%BF%90%E5%8A%A8","中国政治运动"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%9B%BD%E6%94%BF%E6%B2%BB%E9%A2%98%E6%9D%90%E4%BD%9C%E5%93%81","中国政治题材作品"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%9C%8B%E8%87%AA%E7%94%B1%E6%B4%BE","中国自由派"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%9C%8B%E8%B2%AA%E6%B1%A1","中国贪污"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%9B%BD%E5%90%84%E7%9C%81%E6%94%BF%E6%B2%BB%E4%BA%8B%E4%BB%B6","中国各省政治事件"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%9B%BD%E5%90%84%E6%9C%9D%E4%BB%A3%E6%94%BF%E6%B2%BB%E4%BA%8B%E4%BB%B6","中国各朝代政治事件"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%9B%BD%E5%A4%96%E4%BA%A4%E4%BA%8B%E4%BB%B6","中国外交事件"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%9B%BD%E6%94%BF%E5%8F%98","中国政变"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%9B%BD%E6%94%BF%E5%BA%9C%E4%BA%8B%E4%BB%B6","中国政府事件"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%9B%BD%E6%94%BF%E6%B2%BB%E4%B8%91%E9%97%BB","中国政治丑闻"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%9B%BD%E6%94%BF%E6%B2%BB%E4%BC%9A%E8%AE%AE","中国政治会议"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%9B%BD%E6%94%BF%E6%B2%BB%E6%A1%88%E4%BB%B6","中国政治案件"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%9B%BD%E7%BD%A2%E5%B7%A5%E4%BA%8B%E4%BB%B6","中国罢工事件"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%9B%BD%E9%80%89%E4%B8%BE%E4%BA%8B%E4%BB%B6","中国选举事件"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E5%8D%97%E5%B7%A1","南巡"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E5%90%84%E4%B8%96%E7%BA%AA%E4%B8%AD%E5%9B%BD%E6%94%BF%E6%B2%BB%E4%BA%8B%E4%BB%B6","各世纪中国政治事件"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E5%90%84%E5%B9%B4%E4%BB%A3%E4%B8%AD%E5%9B%BD%E6%94%BF%E6%B2%BB%E4%BA%8B%E4%BB%B6","各年代中国政治事件"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%8D%8E%E4%BA%BA%E6%B0%91%E5%85%B1%E5%92%8C%E5%9B%BD%E7%A4%BE%E4%BC%9A%E8%BF%90%E5%8A%A8","中华人民共和国社会运动"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%8D%8E%E6%B0%91%E5%9B%BD%E7%A4%BE%E4%BC%9A%E8%BF%90%E5%8A%A8","中华民国社会运动"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%9B%BD%E5%AD%A6%E7%94%9F%E8%BF%90%E5%8A%A8","中国学生运动"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%9B%BD%E5%B7%A5%E4%BA%BA%E8%BF%90%E5%8A%A8","中国工人运动"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%9B%BD%E6%96%87%E5%8C%96%E8%BF%90%E5%8A%A8","中国文化运动"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%9B%BD%E7%A4%BE%E4%BC%9A%E8%BF%90%E5%8A%A8%E8%80%85","中国社会运动者"],
            #    ["https://zh.wikipedia.org/wiki/Category:1950%E5%B9%B4%E4%BB%A3%E4%B8%AD%E5%9B%BD%E7%81%BE%E9%9A%BE","1950年代中国灾难"],
            #    ["https://zh.wikipedia.org/wiki/Category:1960%E5%B9%B4%E4%BB%A3%E4%B8%AD%E5%9B%BD%E7%81%BE%E9%9A%BE","1960年代中国灾难"],
            #    ["https://zh.wikipedia.org/wiki/Category:1970%E5%B9%B4%E4%BB%A3%E4%B8%AD%E5%9B%BD%E7%81%BE%E9%9A%BE","1970年代中国灾难"],
            #    ["https://zh.wikipedia.org/wiki/Category:1980%E5%B9%B4%E4%BB%A3%E4%B8%AD%E5%9B%BD%E7%81%BE%E9%9A%BE","1980年代中国灾难"],
            #    ["https://zh.wikipedia.org/wiki/Category:1990%E5%B9%B4%E4%BB%A3%E4%B8%AD%E5%9B%BD%E7%81%BE%E9%9A%BE","1990年代中国灾难"],
            #    ["https://zh.wikipedia.org/wiki/Category:21%E4%B8%96%E7%BA%AA%E4%B8%AD%E5%9B%BD%E7%81%BE%E9%9A%BE","21世纪中国灾难"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%8D%8E%E4%BA%BA%E6%B0%91%E5%85%B1%E5%92%8C%E5%9B%BD%E4%BA%8B%E6%95%85","中华人民共和国事故"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%8D%8E%E4%BA%BA%E6%B0%91%E5%85%B1%E5%92%8C%E5%9B%BD%E5%90%84%E7%9C%81%E7%81%BE%E9%9A%BE","中华人民共和国各省灾难"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%8D%8E%E4%BA%BA%E6%B0%91%E5%85%B1%E5%92%8C%E5%9B%BD%E6%B0%B4%E7%81%BE","中华人民共和国水灾"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%8D%8E%E4%BA%BA%E6%B0%91%E5%85%B1%E5%92%8C%E5%9B%BD%E7%81%AB%E7%81%BE","中华人民共和国火灾"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%8D%8E%E4%BA%BA%E6%B0%91%E5%85%B1%E5%92%8C%E5%9B%BD%E7%81%BE%E5%AE%B3%E7%AE%A1%E7%90%86","中华人民共和国灾害管理"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E8%8F%AF%E4%BA%BA%E6%B0%91%E5%85%B1%E5%92%8C%E5%9C%8B%E5%B1%A0%E6%AE%BA%E4%BA%8B%E4%BB%B6","中华人民共和国屠杀事件"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E5%A4%A7%E8%B7%83%E8%BF%9B","大跃进"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%8D%8E%E4%BA%BA%E6%B0%91%E5%85%B1%E5%92%8C%E5%9B%BD%E5%AE%97%E6%95%99%E4%BA%8B%E4%BB%B6","中华人民共和国宗教事件"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%9B%BD%E4%BC%8A%E6%96%AF%E5%85%B0%E6%95%99%E4%BA%8B%E4%BB%B6","中国伊斯兰教事件"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%9B%BD%E4%BD%9B%E6%95%99%E4%BA%8B%E4%BB%B6","中国佛教事件"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%9B%BD%E5%9F%BA%E7%9D%A3%E6%95%99%E4%BA%8B%E4%BB%B6","中国基督教事件"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%8D%8E%E4%BA%BA%E6%B0%91%E5%85%B1%E5%92%8C%E5%9B%BD%E6%81%90%E6%80%96%E6%B4%BB%E5%8A%A8","中华人民共和国恐怖活动"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%9B%BD%E6%A0%A1%E5%9B%AD%E8%A2%AD%E5%87%BB%E4%BA%8B%E4%BB%B6","中国校园袭击事件"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E6%96%B0%E7%96%86%E6%81%90%E6%80%96%E4%B8%BB%E4%B9%89","新疆恐怖主义"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%9B%BD%E5%88%86%E7%A6%BB%E4%B8%BB%E4%B9%89%E4%BA%BA%E7%89%A9","中国分离主义人物"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%9B%BD%E5%88%86%E7%A6%BB%E4%B8%BB%E4%B9%89%E7%BB%84%E7%BB%87","中国分离主义组织"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%9B%BD%E5%8F%8D%E5%8F%9B%E7%BB%84%E7%BB%87","中国反叛组织"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E8%8F%AF%E4%BA%BA%E6%B0%91%E5%85%B1%E5%92%8C%E5%9C%8B%E5%88%86%E9%9B%A2%E4%B8%BB%E7%BE%A9","中华人民共和国分离主义"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E8%8F%AF%E6%B0%91%E5%9C%8B%E5%88%86%E9%9B%A2%E4%B8%BB%E7%BE%A9","中华民国分离主义"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E8%92%99%E5%8F%A4%E7%8D%A8%E7%AB%8B%E9%81%8B%E5%8B%95","蒙古独立运动"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E5%90%84%E6%81%90%E6%80%96%E7%BB%84%E7%BB%87%E6%88%90%E5%91%98","各恐怖组织成员"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E5%90%84%E6%8C%87%E5%AE%9A%E8%80%85%E6%89%80%E5%AE%9A%E6%81%90%E6%80%96%E7%B5%84%E7%B9%94","各指定者所定恐怖组织"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E5%90%84%E7%BB%84%E7%BB%87%E5%8F%91%E5%8A%A8%E7%9A%84%E6%81%90%E6%80%96%E6%B4%BB%E5%8A%A8","各组织发动的恐怖活动"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E7%B4%85%E8%89%B2%E9%AB%98%E6%A3%89","红色高棉"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E5%80%8B%E4%BA%BA%E5%B4%87%E6%8B%9C","个人崇拜"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E6%96%B0%E8%88%88%E5%AE%97%E6%95%99","新兴宗教"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E8%A2%AB%E6%94%BF%E5%BA%9C%E8%AA%8D%E5%AE%9A%E7%82%BA%E9%82%AA%E6%95%99%E7%9A%84%E5%9C%98%E9%AB%94","被政府认定为邪教的团体"],
            #    ["https://zh.wikipedia.org/wiki/Category:%E9%82%AA%E6%95%99%E9%A2%98%E6%9D%90%E4%BD%9C%E5%93%81","邪教题材作品"]
        ]
        # lists = [['https://zh.wikipedia.org/wiki/Category:%E6%94%BF%E6%B2%BB%E9%9A%90%E5%96%BB', '政治隐喻'],
        #          [
        #              'https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%8D%8E%E4%BA%BA%E6%B0%91%E5%85%B1%E5%92%8C%E5%9B%BD%E8%A2%AB%E7%A6%81%E5%BD%B1%E8%A7%86%E4%BD%9C%E5%93%81',
        #              '中华人民共和国被禁影视作品']]


        for cur_base in lists:
            save_csv_dir="ori_csvs_4/"
            if not os.path.exists(save_csv_dir):
                os.mkdir(save_csv_dir)
            save_csv=save_csv_dir+cur_base[1]+".csv"
            # if os.path.exists(save_csv):
            #     continue
            base_url = []
            base_url.append(cur_base[0])

            if os.path.exists(save_csv_dir + "my_saved_viewed.pkl"):
                with open(save_csv_dir + "my_saved_viewed.pkl", "rb") as viewd_save_file:
                    viewed = pickle.load(viewd_save_file)
                    #+207767
            else:
                viewed = set()
                for i in range(len(lists)):
                    viewed.add(lists[i][0])


            if os.path.exists(save_csv_dir+"my_saved_queue.pkl"):
                with open(save_csv_dir+"my_saved_queue.pkl", "rb") as queue_save_file:
                    queue_movie = pickle.load(queue_save_file)
            else:
                queue_movie = list()
                for i in range(len(base_url)):
                    queue_movie.append(base_url[i])

            if os.path.exists(save_csv_dir+"my_saved_tot_urls.pkl"):
                with open(save_csv_dir+"my_saved_tot_urls.pkl", "rb") as tot_save_file:
                    Tot_urls = pickle.load(tot_save_file)
                    #7969024+1119538
            else:
                Tot_urls = []

            if not os.path.exists(save_csv):
                with open(save_csv, 'w', encoding="utf8", newline='') as fi:
                    writer = csv.writer(fi, delimiter=self.sep)
                    writer.writerow(["url", "title", "pre_url", "isleaf"])
            viewed_url_cnt=len(viewed)
            while queue_movie!=[]:
                shutil.copyfile(save_csv_dir+"my_saved_queue.pkl", save_csv_dir+"my_saved_queue_bak.pkl")
                with open(save_csv_dir+"my_saved_queue.pkl", "wb") as queue_save_file:
                    pickle.dump(queue_movie, queue_save_file)
                shutil.copyfile(save_csv_dir + "my_saved_viewed.pkl", save_csv_dir + "my_saved_viewed_bak.pkl")
                with open(save_csv_dir+"my_saved_viewed.pkl", "wb") as viewed_save_file:
                    pickle.dump(viewed, viewed_save_file)
                shutil.copyfile(save_csv_dir + "my_saved_tot_urls.pkl", save_csv_dir + "my_saved_tot_urls_bak.pkl")
                with open(save_csv_dir+"my_saved_tot_urls.pkl", "wb") as tot_save_file:
                    pickle.dump(Tot_urls, tot_save_file)
                viewed_url_cnt+=1
                self.log.info('tot get url:%d' % (len(Tot_urls)))
                self.log.info("Queue size:%d, viewd urls:%d"%(len(queue_movie),viewed_url_cnt))
                tot_urls = []
                cur_url = queue_movie.pop(0)
                htmlContent = self.getResponseContent(cur_url)
                if htmlContent == None:
                    continue
                soup = BeautifulSoup(htmlContent, 'lxml')
                subcate = soup.find('div', attrs={"id": "mw-subcategories"})
                if subcate != None:
                    uls = subcate.find_all("ul")
                    for ul in uls:
                        lis = ul.find_all('li')
                        for cur_li in lis:
                            alink = cur_li.find('a')
                            href = 'https://zh.wikipedia.org' + alink.attrs['href']
                            tot_urls.append([href, alink.attrs['title'], cur_url, 0])
                            if href not in viewed:
                                self.log.info('subcate:%s' % alink.attrs['title'])
                                if href != "https://zh.wikipedia.org/wiki/Category:%E4%BD%BF%E7%94%A8%E9%83%A8%E9%A6%96%E7%9A%84%E7%AD%86%E5%8A%83%E6%95%B8%E4%BD%9C%E7%82%BA%E6%8E%92%E5%BA%8F%E9%8D%B5%E5%80%BC%E7%9A%84%E5%88%86%E9%A1%9E":
                                    queue_movie.append(href)
                                viewed.add(href)

                    next_page = subcate.find('a', text=re.compile('下一页'))
                    if next_page == None:
                        next_page = subcate.find('a', text=re.compile('下壹頁'))
                    if next_page != None:
                        href = 'https://zh.wikipedia.org' + next_page.attrs['href']
                        tot_urls.append([href, next_page.attrs['title'], cur_url, 0])
                        if href not in viewed:
                            queue_movie.append(href)
                            viewed.add(href)
                            self.log.info('%s:下一页' % href)

                mwpages = soup.find('div', attrs={"id": "mw-pages"})
                if mwpages != None:
                    uls = mwpages.find_all("ul")
                    for ul in uls:
                        lis = ul.find_all('li')
                        for cur_li in lis:
                            href = 'https://zh.wikipedia.org' + cur_li.a.attrs['href']
                            title = cur_li.a.attrs['title']
                            if [href, title, cur_url, 1] not in Tot_urls:
                                tot_urls.append([href, title, cur_url, 1])
                                self.log.info("get page:%s" % title)
                    next_page = mwpages.find('a', text=re.compile('下一页'))
                    if next_page == None:
                        next_page = mwpages.find('a', text=re.compile('下壹頁'))
                    if next_page != None:
                        href = 'https://zh.wikipedia.org' + next_page.attrs['href']
                        tot_urls.append([href, next_page.attrs['title'], cur_url, 0])
                        if href not in viewed:
                            queue_movie.append(href)
                            viewed.add(href)
                            self.log.info('%s:下一页' % href)

                Tot_urls.extend(tot_urls)
                with open(save_csv, 'a+', encoding="utf8", newline='') as fi:
                    writer = csv.writer(fi, delimiter=self.sep)
                    writer.writerows(tot_urls)
            num_urls = len(Tot_urls)
            self.log.info("%s tot urls:%d" %(cur_base[1],num_urls))

    def get_list_from_csv_3(self):
        from_csv_dir='d:/hwz/code/pt_risk/'
        file_list= os.listdir(from_csv_dir)
        for cur_file in file_list:
            if cur_file=="时政.csv":
                continue
            if cur_file == "领导人.csv":
                continue
            if os.path.isfile(os.path.join(from_csv_dir,cur_file)) and cur_file.endswith("csv"):
                data=pd.read_csv(os.path.join(from_csv_dir,cur_file),sep=',')
                data_list=data.values.tolist()
                Tot_urls=[]
                queue_movie = list()
                viewed = set()
                for cur_data in data_list:
                    Tot_urls.append([cur_data[0],cur_data[1],cur_data[2],cur_data[3]])
                    viewed.add(cur_data[0])
                    if cur_data[3]==0:
                        queue_movie.append(cur_data[0])

                save_csv_dir = "d:/hwz/code/pt_risk/csvs_re_3/"
                if not os.path.exists(save_csv_dir):
                    os.makedirs(save_csv_dir)
                save_csv = save_csv_dir + cur_file
                if not os.path.exists(save_csv):
                    with open(save_csv, 'w', encoding="utf8", newline='') as fi:
                        writer = csv.writer(fi, delimiter=self.sep)
                        writer.writerow(["url", "title", "pre_url", "isleaf"])
                with open(save_csv, 'a+', encoding="utf8", newline='') as fi:
                    writer = csv.writer(fi, delimiter=self.sep)
                    writer.writerows(Tot_urls)

                viewed_url_cnt = len(viewed)
                while queue_movie != []:
                    if os.path.exists(save_csv_dir + "my_saved_queue.pkl"):
                        shutil.copyfile(save_csv_dir + "my_saved_queue.pkl", save_csv_dir + "my_saved_queue_bak.pkl")
                    with open(save_csv_dir + "my_saved_queue.pkl", "wb") as queue_save_file:
                        pickle.dump(queue_movie, queue_save_file)
                    if os.path.exists(save_csv_dir + "my_saved_viewed.pkl"):
                        shutil.copyfile(save_csv_dir + "my_saved_viewed.pkl", save_csv_dir + "my_saved_viewed_bak.pkl")
                    with open(save_csv_dir + "my_saved_viewed.pkl", "wb") as viewed_save_file:
                        pickle.dump(viewed, viewed_save_file)
                    if os.path.exists(save_csv_dir + "my_saved_tot_urls.pkl"):
                        shutil.copyfile(save_csv_dir + "my_saved_tot_urls.pkl", save_csv_dir + "my_saved_tot_urls_bak.pkl")
                    with open(save_csv_dir + "my_saved_tot_urls.pkl", "wb") as tot_save_file:
                        pickle.dump(Tot_urls, tot_save_file)
                    viewed_url_cnt += 1
                    self.log.info('tot get url:%d' % (len(Tot_urls)))
                    self.log.info("Queue size:%d, viewd urls:%d" % (len(queue_movie), viewed_url_cnt))
                    tot_urls = []
                    cur_url = queue_movie.pop(0)
                    htmlContent = self.getResponseContent(cur_url)
                    if htmlContent == None:
                        continue
                    soup = BeautifulSoup(htmlContent, 'lxml')
                    subcate = soup.find('div', attrs={"id": "mw-subcategories"})
                    if subcate != None:
                        uls = subcate.find_all("ul")
                        for ul in uls:
                            lis = ul.find_all('li')
                            for cur_li in lis:
                                alink = cur_li.find('a')
                                href = 'https://zh.wikipedia.org' + alink.attrs['href']
                                tot_urls.append([href, alink.attrs['title'], cur_url, 0])
                                if href not in viewed:
                                    self.log.info('subcate:%s' % alink.attrs['title'])
                                    if href != "https://zh.wikipedia.org/wiki/Category:%E4%BD%BF%E7%94%A8%E9%83%A8%E9%A6%96%E7%9A%84%E7%AD%86%E5%8A%83%E6%95%B8%E4%BD%9C%E7%82%BA%E6%8E%92%E5%BA%8F%E9%8D%B5%E5%80%BC%E7%9A%84%E5%88%86%E9%A1%9E":
                                        queue_movie.append(href)
                                    viewed.add(href)

                        next_page = subcate.find('a', text=re.compile('下一页'))
                        if next_page == None:
                            next_page = subcate.find('a', text=re.compile('下壹頁'))
                        if next_page != None:
                            href = 'https://zh.wikipedia.org' + next_page.attrs['href']
                            tot_urls.append([href, next_page.attrs['title'], cur_url, 0])
                            if href not in viewed:
                                queue_movie.append(href)
                                viewed.add(href)
                                self.log.info('%s:下一页' % href)

                    mwpages = soup.find('div', attrs={"id": "mw-pages"})
                    if mwpages != None:
                        uls = mwpages.find_all("ul")
                        for ul in uls:
                            lis = ul.find_all('li')
                            for cur_li in lis:
                                href = 'https://zh.wikipedia.org' + cur_li.a.attrs['href']
                                title = cur_li.a.attrs['title']
                                if [href, title, cur_url, 1] not in Tot_urls:
                                    tot_urls.append([href, title, cur_url, 1])
                                    self.log.info("get page:%s" % title)
                        next_page = mwpages.find('a', text=re.compile('下一页'))
                        if next_page == None:
                            next_page = mwpages.find('a', text=re.compile('下壹頁'))
                        if next_page != None:
                            href = 'https://zh.wikipedia.org' + next_page.attrs['href']
                            tot_urls.append([href, next_page.attrs['title'], cur_url, 0])
                            if href not in viewed:
                                queue_movie.append(href)
                                viewed.add(href)
                                self.log.info('%s:下一页' % href)

                    Tot_urls.extend(tot_urls)
                    with open(save_csv, 'a+', encoding="utf8", newline='') as fi:
                        writer = csv.writer(fi, delimiter=self.sep)
                        writer.writerows(tot_urls)
                num_urls = len(Tot_urls)
                self.log.info("%s tot urls:%d" % (cur_file, num_urls))




    def get_list_from_csv_2(self):
        from_csv_dir='d:/hwz/code/pt_risk/'
        file_list= ['领导人.csv']
        for cur_file in file_list:
            if os.path.isfile(os.path.join(from_csv_dir,cur_file)) and cur_file.endswith("csv"):
                save_csv_dir = "d:/hwz/code/pt_risk/csvs_re_2/"
                data=pd.read_csv(os.path.join(from_csv_dir,cur_file),sep=',')
                data_list=data.values.tolist()

                if os.path.exists(save_csv_dir + "my_saved_viewed.pkl"):
                    with open(save_csv_dir + "my_saved_viewed.pkl", "rb") as viewd_save_file:
                        viewed = pickle.load(viewd_save_file)
                else:
                    viewed = set()
                    for cur_data in data_list:
                        viewed.add(cur_data[0])

                if os.path.exists(save_csv_dir + "my_saved_queue.pkl"):
                    with open(save_csv_dir + "my_saved_queue.pkl", "rb") as queue_save_file:
                        queue_movie = pickle.load(queue_save_file)
                else:
                    queue_movie = list()
                    for cur_data in data_list:
                        if cur_data[3] == 0:
                            queue_movie.append(cur_data[0])

                if os.path.exists(save_csv_dir + "my_saved_tot_urls.pkl"):
                    with open(save_csv_dir + "my_saved_tot_urls.pkl", "rb") as tot_save_file:
                        Tot_urls = pickle.load(tot_save_file)
                else:
                    Tot_urls = []
                    for cur_data in data_list:
                        Tot_urls.append([cur_data[0], cur_data[1], cur_data[2], cur_data[3]])

                if not os.path.exists(save_csv_dir):
                    os.makedirs(save_csv_dir)
                save_csv = save_csv_dir + cur_file
                # if not os.path.exists(save_csv):
                #     with open(save_csv, 'w', encoding="utf8", newline='') as fi:
                #         writer = csv.writer(fi, delimiter=self.sep)
                #         writer.writerow(["url", "title", "pre_url", "isleaf"])
                # with open(save_csv, 'a+', encoding="utf8", newline='') as fi:
                #     writer = csv.writer(fi, delimiter=self.sep)
                #     writer.writerows(Tot_urls)

                viewed_url_cnt = len(viewed)
                while queue_movie != []:
                    if os.path.exists(save_csv_dir + "my_saved_queue.pkl"):
                        shutil.copyfile(save_csv_dir + "my_saved_queue.pkl", save_csv_dir + "my_saved_queue_bak.pkl")
                    with open(save_csv_dir + "my_saved_queue.pkl", "wb") as queue_save_file:
                        pickle.dump(queue_movie, queue_save_file)
                    if os.path.exists(save_csv_dir + "my_saved_viewed.pkl"):
                        shutil.copyfile(save_csv_dir + "my_saved_viewed.pkl", save_csv_dir + "my_saved_viewed_bak.pkl")
                    with open(save_csv_dir + "my_saved_viewed.pkl", "wb") as viewed_save_file:
                        pickle.dump(viewed, viewed_save_file)
                    if os.path.exists(save_csv_dir + "my_saved_tot_urls.pkl"):
                        shutil.copyfile(save_csv_dir + "my_saved_tot_urls.pkl", save_csv_dir + "my_saved_tot_urls_bak.pkl")
                    with open(save_csv_dir + "my_saved_tot_urls.pkl", "wb") as tot_save_file:
                        pickle.dump(Tot_urls, tot_save_file)
                    viewed_url_cnt += 1
                    self.log.info('tot get url:%d' % (len(Tot_urls)))
                    self.log.info("Queue size:%d, viewd urls:%d" % (len(queue_movie), viewed_url_cnt))
                    tot_urls = []
                    cur_url = queue_movie.pop(0)
                    htmlContent = self.getResponseContent(cur_url)
                    if htmlContent == None:
                        continue
                    soup = BeautifulSoup(htmlContent, 'lxml')
                    subcate = soup.find('div', attrs={"id": "mw-subcategories"})
                    if subcate != None:
                        uls = subcate.find_all("ul")
                        for ul in uls:
                            lis = ul.find_all('li')
                            for cur_li in lis:
                                alink = cur_li.find('a')
                                href = 'https://zh.wikipedia.org' + alink.attrs['href']
                                tot_urls.append([href, alink.attrs['title'], cur_url, 0])
                                if href not in viewed:
                                    self.log.info('subcate:%s' % alink.attrs['title'])
                                    if href != "https://zh.wikipedia.org/wiki/Category:%E4%BD%BF%E7%94%A8%E9%83%A8%E9%A6%96%E7%9A%84%E7%AD%86%E5%8A%83%E6%95%B8%E4%BD%9C%E7%82%BA%E6%8E%92%E5%BA%8F%E9%8D%B5%E5%80%BC%E7%9A%84%E5%88%86%E9%A1%9E":
                                        queue_movie.append(href)
                                    viewed.add(href)

                        next_page = subcate.find('a', text=re.compile('下一页'))
                        if next_page == None:
                            next_page = subcate.find('a', text=re.compile('下壹頁'))
                        if next_page != None:
                            href = 'https://zh.wikipedia.org' + next_page.attrs['href']
                            tot_urls.append([href, next_page.attrs['title'], cur_url, 0])
                            if href not in viewed:
                                queue_movie.append(href)
                                viewed.add(href)
                                self.log.info('%s:下一页' % href)

                    mwpages = soup.find('div', attrs={"id": "mw-pages"})
                    if mwpages != None:
                        uls = mwpages.find_all("ul")
                        for ul in uls:
                            lis = ul.find_all('li')
                            for cur_li in lis:
                                href = 'https://zh.wikipedia.org' + cur_li.a.attrs['href']
                                title = cur_li.a.attrs['title']
                                if [href, title, cur_url, 1] not in Tot_urls:
                                    tot_urls.append([href, title, cur_url, 1])
                                    self.log.info("get page:%s" % title)
                        next_page = mwpages.find('a', text=re.compile('下一页'))
                        if next_page == None:
                            next_page = mwpages.find('a', text=re.compile('下壹頁'))
                        if next_page != None:
                            href = 'https://zh.wikipedia.org' + next_page.attrs['href']
                            tot_urls.append([href, next_page.attrs['title'], cur_url, 0])
                            if href not in viewed:
                                queue_movie.append(href)
                                viewed.add(href)
                                self.log.info('%s:下一页' % href)

                    Tot_urls.extend(tot_urls)
                    with open(save_csv, 'a+', encoding="utf8", newline='') as fi:
                        writer = csv.writer(fi, delimiter=self.sep)
                        writer.writerows(tot_urls)
                num_urls = len(Tot_urls)
                self.log.info("%s tot urls:%d" % (cur_file, num_urls))

    def get_list_from_csv(self):
        from_csv_dir='d:/hwz/code/pt_risk/'
        file_list= ['时政.csv']
        for cur_file in file_list:
            if os.path.isfile(os.path.join(from_csv_dir,cur_file)) and cur_file.endswith("csv"):
                save_csv_dir = "d:/hwz/code/pt_risk/csvs_re/"
                data = pd.read_csv(os.path.join(from_csv_dir, cur_file), sep=',')
                data_list = data.values.tolist()

                if os.path.exists(save_csv_dir + "my_saved_viewed.pkl"):
                    with open(save_csv_dir + "my_saved_viewed.pkl", "rb") as viewd_save_file:
                        viewed = pickle.load(viewd_save_file)
                else:
                    viewed = set()
                    for cur_data in data_list:
                        viewed.add(cur_data[0])

                if os.path.exists(save_csv_dir + "my_saved_queue.pkl"):
                    with open(save_csv_dir + "my_saved_queue.pkl", "rb") as queue_save_file:
                        queue_movie = pickle.load(queue_save_file)
                else:
                    queue_movie = list()
                    for cur_data in data_list:
                        if cur_data[3] == 0:
                            queue_movie.append(cur_data[0])

                if os.path.exists(save_csv_dir + "my_saved_tot_urls.pkl"):
                    with open(save_csv_dir + "my_saved_tot_urls.pkl", "rb") as tot_save_file:
                        Tot_urls = pickle.load(tot_save_file)
                else:
                    Tot_urls = []
                    for cur_data in data_list:
                        Tot_urls.append([cur_data[0], cur_data[1], cur_data[2], cur_data[3]])

                # Tot_urls=[]
                # queue_movie = list()
                # viewed = set()
                # for cur_data in data_list:
                #     Tot_urls.append([cur_data[0],cur_data[1],cur_data[2],cur_data[3]])
                #     viewed.add(cur_data[0])
                #     if cur_data[3]==0:
                #         queue_movie.append(cur_data[0])


                if not os.path.exists(save_csv_dir):
                    os.makedirs(save_csv_dir)
                save_csv = save_csv_dir + cur_file
                # if not os.path.exists(save_csv):
                #     with open(save_csv, 'w', encoding="utf8", newline='') as fi:
                #         writer = csv.writer(fi, delimiter=self.sep)
                #         writer.writerow(["url", "title", "pre_url", "isleaf"])
                # with open(save_csv, 'a+', encoding="utf8", newline='') as fi:
                #     writer = csv.writer(fi, delimiter=self.sep)
                #     writer.writerows(Tot_urls)

                viewed_url_cnt = len(viewed)
                while queue_movie != []:
                    if os.path.exists(save_csv_dir + "my_saved_queue.pkl"):
                        shutil.copyfile(save_csv_dir + "my_saved_queue.pkl", save_csv_dir + "my_saved_queue_bak.pkl")
                    with open(save_csv_dir + "my_saved_queue.pkl", "wb") as queue_save_file:
                        pickle.dump(queue_movie, queue_save_file)
                    if os.path.exists(save_csv_dir + "my_saved_viewed.pkl"):
                        shutil.copyfile(save_csv_dir + "my_saved_viewed.pkl", save_csv_dir + "my_saved_viewed_bak.pkl")
                    with open(save_csv_dir + "my_saved_viewed.pkl", "wb") as viewed_save_file:
                        pickle.dump(viewed, viewed_save_file)
                    if os.path.exists(save_csv_dir + "my_saved_tot_urls.pkl"):
                        shutil.copyfile(save_csv_dir + "my_saved_tot_urls.pkl", save_csv_dir + "my_saved_tot_urls_bak.pkl")
                    with open(save_csv_dir + "my_saved_tot_urls.pkl", "wb") as tot_save_file:
                        pickle.dump(Tot_urls, tot_save_file)
                    viewed_url_cnt += 1
                    self.log.info('tot get url:%d' % (len(Tot_urls)))
                    self.log.info("Queue size:%d, viewd urls:%d" % (len(queue_movie), viewed_url_cnt))
                    tot_urls = []
                    cur_url = queue_movie.pop(0)
                    htmlContent = self.getResponseContent(cur_url)
                    if htmlContent == None:
                        continue
                    soup = BeautifulSoup(htmlContent, 'lxml')
                    subcate = soup.find('div', attrs={"id": "mw-subcategories"})
                    if subcate != None:
                        uls = subcate.find_all("ul")
                        for ul in uls:
                            lis = ul.find_all('li')
                            for cur_li in lis:
                                alink = cur_li.find('a')
                                href = 'https://zh.wikipedia.org' + alink.attrs['href']
                                tot_urls.append([href, alink.attrs['title'], cur_url, 0])
                                if href not in viewed:
                                    self.log.info('subcate:%s' % alink.attrs['title'])
                                    if href != "https://zh.wikipedia.org/wiki/Category:%E4%BD%BF%E7%94%A8%E9%83%A8%E9%A6%96%E7%9A%84%E7%AD%86%E5%8A%83%E6%95%B8%E4%BD%9C%E7%82%BA%E6%8E%92%E5%BA%8F%E9%8D%B5%E5%80%BC%E7%9A%84%E5%88%86%E9%A1%9E":
                                        queue_movie.append(href)
                                    viewed.add(href)

                        next_page = subcate.find('a', text=re.compile('下一页'))
                        if next_page == None:
                            next_page = subcate.find('a', text=re.compile('下壹頁'))
                        if next_page != None:
                            href = 'https://zh.wikipedia.org' + next_page.attrs['href']
                            tot_urls.append([href, next_page.attrs['title'], cur_url, 0])
                            if href not in viewed:
                                queue_movie.append(href)
                                viewed.add(href)
                                self.log.info('%s:下一页' % href)

                    mwpages = soup.find('div', attrs={"id": "mw-pages"})
                    if mwpages != None:
                        uls = mwpages.find_all("ul")
                        for ul in uls:
                            lis = ul.find_all('li')
                            for cur_li in lis:
                                href = 'https://zh.wikipedia.org' + cur_li.a.attrs['href']
                                title = cur_li.a.attrs['title']
                                if [href, title, cur_url, 1] not in Tot_urls:
                                    tot_urls.append([href, title, cur_url, 1])
                                    self.log.info("get page:%s" % title)
                        next_page = mwpages.find('a', text=re.compile('下一页'))
                        if next_page == None:
                            next_page = mwpages.find('a', text=re.compile('下壹頁'))
                        if next_page != None:
                            href = 'https://zh.wikipedia.org' + next_page.attrs['href']
                            tot_urls.append([href, next_page.attrs['title'], cur_url, 0])
                            if href not in viewed:
                                queue_movie.append(href)
                                viewed.add(href)
                                self.log.info('%s:下一页' % href)

                    Tot_urls.extend(tot_urls)
                    with open(save_csv, 'a+', encoding="utf8", newline='') as fi:
                        writer = csv.writer(fi, delimiter=self.sep)
                        writer.writerows(tot_urls)
                num_urls = len(Tot_urls)
                self.log.info("%s tot urls:%d" % (cur_file, num_urls))

    def get_list_3(self):
        save_csv='csvs/一二级链接.csv'
        level1_lists=['https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%9B%BD%E6%94%BF%E6%B2%BB',#中国政治
                      'https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%9B%BD%E6%94%BF%E6%B2%BB%E4%BA%8B%E4%BB%B6',#中国政治事件
                      'https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%9B%BD%E7%A4%BE%E4%BC%9A%E8%BF%90%E5%8A%A8',#中国社会运动
                      'https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%8D%8E%E4%BA%BA%E6%B0%91%E5%85%B1%E5%92%8C%E5%9B%BD%E4%BA%8B%E6%95%85',#中华人民共和国事故
                      'https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%8D%8E%E4%BA%BA%E6%B0%91%E5%85%B1%E5%92%8C%E5%9B%BD%E7%81%BE%E9%9A%BE',#中华人民共和国灾难
                      'https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%9B%BD%E5%AE%97%E6%95%99%E4%BA%8B%E4%BB%B6',#中国宗教事件
                      'https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%9B%BD%E6%81%90%E6%80%96%E4%B8%BB%E4%B9%89',#中国恐怖主义
                      'https://zh.wikipedia.org/wiki/Category:%E4%B8%AD%E5%9C%8B%E5%88%86%E9%9B%A2%E4%B8%BB%E7%BE%A9',#中国分离主义
                      'https://zh.wikipedia.org/wiki/Category:%E6%81%90%E6%80%96%E7%B5%84%E7%B9%94',#恐怖组织
                      'https://zh.wikipedia.org/wiki/Category:%E9%82%AA%E6%95%99',#邪教
                      ]

        viewed = set()
        for i in range(len(level1_lists)):
            viewed.add(level1_lists[i])
        queue_movie = Queue(maxsize=0)
        for i in range(len(level1_lists)):
            queue_movie.put(level1_lists[i])
        Tot_urls = []
        if not os.path.exists(save_csv):
            with open(save_csv, 'w', encoding="utf8", newline='') as fi:
                writer = csv.writer(fi, delimiter=self.sep)
                writer.writerow(["url", "title", "pre_url", "isleaf"])
        while not queue_movie.empty():
            self.log.info('tot get url:%d' % (len(Tot_urls)))
            tot_urls = []
            cur_url = queue_movie.get()
            htmlContent = self.getResponseContent(cur_url)
            if htmlContent == None:
                continue
            soup = BeautifulSoup(htmlContent, 'lxml')
            subcate = soup.find('div', attrs={"id": "mw-subcategories"})
            if subcate != None:
                uls = subcate.find_all("ul")
                for ul in uls:
                    lis = ul.find_all('li')
                    for cur_li in lis:
                        alink = cur_li.find('a')
                        href = 'https://zh.wikipedia.org' + alink.attrs['href']
                        tot_urls.append([href, alink.attrs['title'], cur_url, 0])
                        if href not in viewed:
                            self.log.info('subcate:%s' % alink.attrs['title'])
                            # if href != "https://zh.wikipedia.org/wiki/Category:%E4%BD%BF%E7%94%A8%E9%83%A8%E9%A6%96%E7%9A%84%E7%AD%86%E5%8A%83%E6%95%B8%E4%BD%9C%E7%82%BA%E6%8E%92%E5%BA%8F%E9%8D%B5%E5%80%BC%E7%9A%84%E5%88%86%E9%A1%9E":
                            #     queue_movie.put(href)
                            viewed.add(href)

                next_page = subcate.find('a', text=re.compile('下一页'))
                if next_page == None:
                    next_page = subcate.find('a', text=re.compile('下壹頁'))
                if next_page != None:
                    href = 'https://zh.wikipedia.org' + next_page.attrs['href']
                    tot_urls.append([href, next_page.attrs['title'], cur_url, 0])
                    if href not in viewed:
                        queue_movie.put(href)
                        viewed.add(href)
                        self.log.info('%s:下一页' % href)

            mwpages = soup.find('div', attrs={"id": "mw-pages"})
            if mwpages != None:
                uls = mwpages.find_all("ul")
                for ul in uls:
                    lis = ul.find_all('li')
                    for cur_li in lis:
                        href = 'https://zh.wikipedia.org' + cur_li.a.attrs['href']
                        title = cur_li.a.attrs['title']
                        if [href, title, cur_url, 1] not in Tot_urls:
                            tot_urls.append([href, title, cur_url, 1])
                            self.log.info("get page:%s" % title)
                next_page = mwpages.find('a', text=re.compile('下一页'))
                if next_page == None:
                    next_page = mwpages.find('a', text=re.compile('下壹頁'))
                if next_page != None:
                    href = 'https://zh.wikipedia.org' + next_page.attrs['href']
                    tot_urls.append([href, next_page.attrs['title'], cur_url, 0])
                    if href not in viewed:
                        queue_movie.put(href)
                        viewed.add(href)
                        self.log.info('%s:下一页' % href)

            Tot_urls.extend(tot_urls)
            with open(save_csv, 'a+', encoding="utf8", newline='') as fi:
                writer = csv.writer(fi, delimiter=self.sep)
                writer.writerows(tot_urls)
        num_urls = len(Tot_urls)
        self.log.info("tot urls:%d" % num_urls)


if __name__ == "__main__":
    zhwiki = znWiki("zhwiki_get_leaves_0907.log")
