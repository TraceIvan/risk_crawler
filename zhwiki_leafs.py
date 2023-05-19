from myLog import MyLog as mylog
from queue import Queue
import os
import requests
import time
from multiprocessing.dummy import Pool as ThreadPool
from bs4 import BeautifulSoup,Tag
import re
import csv
import pandas as pd
from urllib import request
import urllib3
import json
from tqdm import tqdm
from langconv import Converter
from urllib.parse import quote, unquote

THREADS = 8
HEADER = {
          "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
          }
PROXIES = {
    'https': 'https://127.0.0.1:7890',
    'http': 'http://127.0.0.1:7890'
}
LOG_DIR='./log_files/'
EPOCH_DIR='./start_epochs/'
class Item():
    title=None
    zhwiki_url=None
    enwiki_url=None
    imgs=None
    infos=None
    catalog=None
    page_text=None
    thumbs=None
    first_par=None

class znWiki(object):
    def __init__(self,log_file,epoch_file,save_pre_dir,is_download_img,is_check_crawled,leaves_from):
        self.is_download_img=is_download_img
        self.is_check_crawled=is_check_crawled
        self.log = mylog(LOG_DIR+log_file)
        self.epoch_file=EPOCH_DIR+epoch_file
        self.timeout = 10
        self.readtimeout = 30
        self.request_sleep = 1
        self.sep = '\t'
        self.save_pre_dir = save_pre_dir
        if not os.path.exists(self.save_pre_dir):
            os.makedirs(self.save_pre_dir)

        self.pic_remain_csv=self.save_pre_dir+'pic_remain.csv'
        self.remain_pic_nums=0
        if not os.path.exists(self.pic_remain_csv):
            with open(self.pic_remain_csv, 'w', encoding="utf8", newline='') as fi:
                writer = csv.writer(fi, delimiter='\t')
                writer.writerow(["title", "url", "file"])

        self.opener = request.build_opener(request.ProxyHandler(PROXIES))
        request.install_opener(self.opener)

        if leaves_from=='KG_nodes_0825':
            self.tot_list = self.get_leaves_title()
        elif leaves_from=='local_csvs':
            self.tot_list = self.get_leaves()
        tot_urls = len(self.tot_list)
        self.log.info("total items:%d" % tot_urls)
        self.log.info("start!")
        epochs = tot_urls // (THREADS * 4)
        if tot_urls % (THREADS * 4):
            epochs += 1
        start_epoch = 0
        if os.path.exists(self.epoch_file):
            with open(self.epoch_file, "r", encoding="utf8") as fi:
                lines = fi.readlines()
                start_epoch = int(lines[0]) + 1
        for i in tqdm(range(start_epoch, epochs), desc='Processing'):
        #for i in range(start_epoch, epochs):
            start_id = i * (THREADS * 4)
            end_id = (i + 1) * (THREADS * 4)
            self.log.info("start %d(%d):%d--%d" % (i,epochs,start_id, end_id))
            pool = ThreadPool(processes=THREADS)
            pool.map(self.spider, self.tot_list[start_id:end_id])
            pool.close()
            pool.join()
            with open(self.epoch_file, "w", encoding="utf8") as fi:
                fi.write(str(i))

    def get_par_text(self, cur_par):
        par_str = ''
        cur_par_contents = cur_par.contents
        for cur_par_content in cur_par_contents:
            if isinstance(cur_par_content, str):
                par_str = par_str + self.str_re(str(cur_par_content))
            elif cur_par_content.name == 'a':
                if '#cite_note' in cur_par_content.attrs['href']:
                    continue
                par_str = par_str + self.str_re(cur_par_content.get_text())
            elif cur_par_content.name == 'sup':
                continue
            elif cur_par_content.name == 'span' and 'id' in cur_par_content.attrs.keys() and 'noteTag-cite_ref-sup' in \
                    cur_par_content.attrs['id']:
                continue
            else:
                par_str = par_str + self.str_re(cur_par_content.get_text())
        return par_str

    def spider(self, data):
        url = data[0]
        title = data[1]

        save_name = title
        save_name = str(save_name)
        save_name = save_name.replace(':', '-')
        save_name = save_name.replace('/', '--')
        save_dir = self.save_pre_dir + save_name
        media_img_dir = save_dir + '/media_img'
        downloads_info_csv = media_img_dir + '/downloads_info.csv'
        if self.is_check_crawled:
            if os.path.exists(downloads_info_csv):
                self.log.info("%s:已经爬取"%title)
                return

        htmlContent = self.getResponseContent(url)
        if htmlContent == None:
            return
        item=Item()
        item.title=title
        item.zhwiki_url=url
        soup = BeautifulSoup(htmlContent, 'lxml')

        page_txt = soup.find('div', attrs={"class": "mw-parser-output"})
        if page_txt==None:
            return
        item.page_text = str(page_txt)

        sentence = ""
        try:
            page_txt_ps = page_txt.find_all('p', recursive=False)
            for cur_p in page_txt_ps:
                cur_p_text = self.get_par_text(cur_p)
                if cur_p_text != '':
                    sentence = Converter('zh-hans').convert(cur_p_text)
                    break
        except Exception as e:
            sentence = ""
        item.first_par = sentence

        catelog=soup.find('div', attrs={"id": "toc","class":"toc","aria-labelledby":"mw-toc-heading"})
        if catelog!=None:
            item.catalog=[]
            links=catelog.find_all('a')
            for cur_link in links:
                tocnumber=cur_link.find('span',attrs={"class":"tocnumber"})
                toctext=cur_link.find('span',attrs={"class":"toctext"})
                item.catalog.append([tocnumber.get_text().strip(),toctext.get_text().strip()])

        all_img=page_txt.find_all('a',attrs={"class":"image"})
        if all_img!=[]:
            item.imgs=[]
            for cur_img_link in all_img:
                cur_img = cur_img_link.find('img')
                item.imgs.append(['https://zh.wikipedia.org' + cur_img_link.attrs['href'],'https:' + cur_img.attrs['src'],cur_img.attrs['alt']])
            imgs_set=set()
            new_imgs_list=[]
            for cur_img in item.imgs:
                if cur_img[0] not in imgs_set:
                    imgs_set.add(cur_img[0])
                    new_imgs_list.append(cur_img)
            item.imgs=new_imgs_list

        item.enwiki_url =""
        lang_h3=soup.find('h3',attrs={"id":"p-lang-label"})
        if lang_h3!=None:
            lang_h3 = lang_h3.next_sibling.next_sibling
            english_url=lang_h3.find('li',attrs={"class":"interwiki-en"})
            if english_url!=None:
                item.enwiki_url=english_url.find('a').attrs['href']


        info_table = soup.find('table',attrs={"class":"infobox"})#re.compile("infobox.*vcard")
        if info_table != None:
            info_table_flag = True
            if 'mbox-small' in info_table.attrs['class'] or 'sisterproject' in info_table.attrs['class']:
                info_table_flag = False
            if info_table_flag:
                item.infos=self.get_table_info_3(item.zhwiki_url,info_table)

        thumbs = page_txt.find_all('div', attrs={"class": "thumbinner"})
        if thumbs!=[]:
            item.thumbs=[]
            for cur_thumb in thumbs:
                cur_info=[]
                thumb_head=cur_thumb.find('div',attrs={"class": "theader"})
                if thumb_head!=None:
                    cur_info.append(thumb_head.get_text().strip())
                else:
                    cur_info.append('')
                thumb_imgs=cur_thumb.find_all('a',attrs={"class": "image"})
                if thumb_imgs!=[]:
                    tmp_img_urls=[]
                    for cur_thumb_img in thumb_imgs:
                        tmp_img_urls.append('https://zh.wikipedia.org'+cur_thumb_img.attrs['href'])
                    tmp_img_urls='||'.join(tmp_img_urls)
                    cur_info.append(tmp_img_urls)
                else:
                    cur_info.append('')
                thumb_caption=cur_thumb.find('div',attrs={"class": "thumbcaption"})
                if thumb_caption!=None:
                    cur_info.append(thumb_caption.get_text().strip())
                else:
                    cur_info.append('')
                item.thumbs.append(cur_info)

        self.pipeline_save(item)

    def clean_blank_table_info(self, info_table):
        new_info_table = info_table.copy()
        for cur_key, cur_value in info_table.items():
            if isinstance(cur_value, dict):
                new_dict = self.clean_blank_table_info(cur_value)
                if len(new_dict) == 0 and (
                        'th_links' == cur_key or cur_key.startswith("td_content") or cur_key.startswith("table_td")):
                    new_info_table.pop(cur_key)
                else:
                    new_info_table[cur_key] = new_dict
            elif isinstance(cur_value, list):
                if len(cur_value) == 0:
                    new_info_table.pop(cur_key)
        return new_info_table

    def str_re(self, str):
        if str == '：':
            str = ''
        return str.strip().replace('\u2003', '').replace('\ufeff', '').replace("\u00a0", "")

    def get_links_title(self, url, base_pre_url, links):
        links_url, links_title, links_ori_title = [], [], []
        for cur_link in links:
            if "class" in cur_link.attrs.keys() and 'mw-selflink' in cur_link.attrs["class"] and 'selflink' in \
                    cur_link.attrs["class"]:
                continue
            tmp_link = base_pre_url + cur_link.attrs['href']
            url_ori_title = ''
            if '#cite_note' in cur_link.attrs['href'] or '#/map' in cur_link.attrs['href']:
                tmp_link = url + cur_link.attrs['href']
            elif cur_link.attrs['href'].startswith('//'):
                tmp_link = 'https:' + cur_link.attrs['href']
            elif cur_link.attrs['href'].startswith('http'):
                tmp_link = cur_link.attrs['href']
            elif cur_link.attrs['href'].startswith('/wiki/'):
                url_ori_title = unquote(cur_link.attrs['href'][6:], encoding='utf8')
                if url_ori_title.startswith('File:'):
                    url_ori_title = url_ori_title[5:]
            tmp_link_title = self.str_re(cur_link.get_text())
            if tmp_link_title == '':
                if "title" in cur_link.attrs.keys():
                    tmp_link_title = unquote(cur_link.attrs['title'], encoding="utf8")
                elif "class" in cur_link.attrs.keys() and 'image' in cur_link.attrs["class"]:
                    tmp_link_title = cur_link.find('img').attrs['alt']
                elif "class" in cur_link.attrs.keys() and 'mw-kartographer-map' in cur_link.attrs["class"]:
                    tmp_link_title = 'mw-kartographer-map'
                if tmp_link_title == '':
                    tmp_link_title = tmp_link.split('/')[-1]
            if url_ori_title == '':
                url_ori_title = tmp_link_title
            links_url.append(tmp_link)
            links_title.append(tmp_link_title)
            links_ori_title.append(url_ori_title)
        return links_url, links_title, links_ori_title

    def our_merge_dict(self, main_dict, add_dict):
        if main_dict == {}:
            return add_dict
        else:
            for add_key in add_dict.keys():
                new_key = self.check_repeat_key(add_key, main_dict.keys())
                main_dict[new_key] = add_dict[add_key]
            return main_dict

    def get_one_tag_content_br(self, url, base_pre_url, cur_tag):
        cur_tag_contents = cur_tag.contents
        res_str = ''
        res_dict = {}
        res_links, res_links_title, res_links_ori_title = [], [], []
        for cur_tag_content in cur_tag_contents:
            if cur_tag_content.name == 'br':
                res_str = res_str + '\n'
            elif isinstance(cur_tag_content, str):
                res_str = res_str + self.str_re(str(cur_tag_content))
            elif cur_tag_content.find('div', attrs={"class": "NavFrame"}, recursive=False):
                tmp_dict, _, _, _ = self.get_td_contents(url, base_pre_url, cur_tag_content)
                res_dict = self.our_merge_dict(res_dict, tmp_dict)
            elif cur_tag_content.name == "table":
                tmp_dict = self.get_table_info_3(url, cur_tag_content)
                res_dict = self.our_merge_dict(res_dict, tmp_dict)
            elif cur_tag_content.name == 'a':
                cur_tag_content_text = self.str_re(cur_tag_content.get_text())
                res_str = res_str + cur_tag_content_text
                links_url, links_title, links_ori_title = self.get_links_title(url, base_pre_url, [cur_tag_content])
                res_links.extend(links_url)
                res_links_title.extend(links_title)
                res_links_ori_title.extend(links_ori_title)
            else:
                add_str, add_dict, links_url, links_title, links_ori_title = self.get_one_tag_content_br(url,
                                                                                                         base_pre_url,
                                                                                                         cur_tag_content)
                res_str = res_str + add_str
                res_dict = self.our_merge_dict(res_dict, add_dict)
                res_links.extend(links_url)
                res_links_title.extend(links_title)
                res_links_ori_title.extend(links_ori_title)
        return self.str_re(res_str), res_dict, res_links, res_links_title, res_links_ori_title

    def get_td_contents(self, url, base_pre_url, cur_td):
        cur_td_contents = cur_td.contents
        new_cur_td_contents = []
        for tmp_content in cur_td_contents:
            if isinstance(tmp_content, Tag) or str(tmp_content).strip() != '':
                new_cur_td_contents.append(tmp_content)
        cur_td_contents = new_cur_td_contents
        if len(cur_td_contents) == 0:
            return None, None, None, None
        cur_td_dict = {'list': [], 'links': [], 'links_title': [], 'links_ori_title': []}
        default_td_head = 'td_content'
        should_under_first_key = False
        is_table_th_background = False
        list_add_str = ''

        if 'style' in cur_td.attrs.keys() and re.search('background:\s*#e6e9ff', cur_td.attrs['style']) \
                and re.search('text-align:\s*right', cur_td.attrs['style']) \
                and re.search('font-size:\s*xx-small', cur_td.attrs['style']):
            # 其实并不在乎你-最后一行模板
            return None, None, None, None

        if len(cur_td_contents) == 2 and cur_td_contents[0].name == 'a' and cur_td_contents[1].name == 'div':
            default_td_head = "封面图片"
        elif len(cur_td_contents) == 1 and cur_td_contents[0].name == 'a' and cur_td_contents[0].find('img'):
            default_td_head = "封面图片"
        elif len(cur_td_contents) == 2 and len(cur_td.find_all('div', recursive=False)) == 2 and \
                cur_td.find_all('div', recursive=False)[0].find('img'):
            default_td_head = "封面图片"
        elif cur_td.find('div', attrs={"class": "thumbinner"}):
            default_td_head = "封面图片"

        if 'style' in cur_td.attrs.keys() and 'border-top' in cur_td.attrs['style']:
            should_under_first_key = True
        if len(cur_td_contents) == 1 and cur_td_contents[0].name == 'div' and len(
                cur_td_contents[0].find_all('div', recursive=False)) == 2 \
                and cur_td_contents[0].find_all('div', recursive=False)[0].find('img'):
            should_under_first_key = True

        first_content = True
        td_middle_b = ''
        for cur_td_content in cur_td_contents:
            if isinstance(cur_td_content, str):
                cur_td_content_text = self.str_re(str(cur_td_content))
                list_add_str = list_add_str + cur_td_content_text
            elif isinstance(cur_td_content, Tag):
                if cur_td_content.name == 'br' and list_add_str != '':  # 换行
                    if td_middle_b == '':
                        cur_td_dict['list'].append(list_add_str)
                        list_add_str = ''
                    else:
                        cur_td_dict[td_middle_b]['list'].append(list_add_str)
                        list_add_str = ''
                elif cur_td_content.name == 'div' and "class" in cur_td_content.attrs.keys() \
                        and 'plainlinks' in cur_td_content.attrs['class'] and 'hlist' in cur_td_content.attrs['class'] \
                        and 'navbar' in cur_td_content.attrs['class'] and 'mini' in cur_td_content.attrs['class']:
                    continue
                elif cur_td_content.name == 'div' and 'class' in cur_td_content.attrs.keys() and 'NavFrame' in \
                        cur_td_content.attrs['class']:
                    '''含有Navframe'''
                    default_td_head = "NavFrame"
                    NavHead_tag = cur_td_content.find('div', attrs={"class": "NavHead"})
                    if NavHead_tag != None and 'style' in NavHead_tag.attrs.keys() and 'background' in \
                            NavHead_tag.attrs['style'] \
                            and re.search("text-align:\s*center", NavHead_tag.attrs['style']):
                        should_under_first_key = True
                        is_table_th_background = True
                    nav_head = self.str_re(cur_td_content.find('div', attrs={"class": "NavHead"}).get_text())
                    new_nav_head = nav_head
                    cnt = 2
                    while new_nav_head in cur_td_dict.keys():
                        new_nav_head = nav_head + '_' + str(cnt)
                        cnt += 1
                    nav_head = new_nav_head
                    cur_td_dict[nav_head] = {'list': [], 'links': [], 'links_title': [], 'links_ori_title': []}
                    ul = cur_td_content.find('ul', attrs={"class": "NavContent"})
                    if ul == None:
                        if cur_td_content.find('div', attrs={"class": "NavContent"}):
                            ul = cur_td_content.find('div', attrs={"class": "NavContent"}).find('ul')
                        else:
                            ul = cur_td_content.find('ul')
                    if ul != None:
                        lis = ul.find_all('li')
                        for cur_li in lis:
                            plainlist = cur_li.find('div', attrs={"class": "plainlist"})
                            if plainlist != None:
                                plainlist_contents = plainlist.contents
                            else:
                                plainlist_contents = cur_li.contents
                            nav_list_add_str = ''
                            for cur_plainlist_content in plainlist_contents:
                                if isinstance(cur_plainlist_content, str):
                                    cur_plainlist_content_text = str(cur_plainlist_content).strip()
                                    nav_list_add_str = nav_list_add_str + cur_plainlist_content_text
                                elif isinstance(cur_plainlist_content, Tag):
                                    if cur_plainlist_content.name == 'br' and nav_list_add_str != '':
                                        cur_td_dict[nav_head]['list'].append(nav_list_add_str)
                                        nav_list_add_str = ''
                                    elif cur_plainlist_content.name == 'table':
                                        tmp_dict = self.get_table_info_3(url, cur_plainlist_content)
                                        for tmp_dict_key in tmp_dict.keys():
                                            new_tmp_dict_key = self.check_repeat_key(tmp_dict_key,
                                                                                     cur_td_dict[nav_head].keys())
                                            cur_td_dict[nav_head][new_tmp_dict_key] = tmp_dict[tmp_dict_key]
                                    else:
                                        cur_plainlist_content_text = self.str_re(cur_plainlist_content.get_text())
                                        nav_list_add_str = nav_list_add_str + cur_plainlist_content_text

                                        links = cur_plainlist_content.find_all('a')
                                        if cur_plainlist_content.name == 'a':
                                            links.insert(0, cur_plainlist_content)
                                        links_url, links_title, links_ori_title = self.get_links_title(url,
                                                                                                       base_pre_url,
                                                                                                       links)
                                        cur_td_dict[nav_head]['links'].extend(links_url)
                                        cur_td_dict[nav_head]['links_title'].extend(links_title)
                                        cur_td_dict[nav_head]['links_ori_title'].extend(links_title)
                            if nav_list_add_str != '':
                                cur_td_dict[nav_head]['list'].append(nav_list_add_str)
                                nav_list_add_str = ''

                    else:
                        NavContent = cur_td_content.find('div', attrs={"class": "NavContent"})
                        NavContent_contents = NavContent.contents
                        nav_list_add_str = ''
                        for cur_NavContent_content in NavContent_contents:
                            if isinstance(cur_NavContent_content, str):
                                cur_NavContent_content_text = str(cur_NavContent_content).strip()
                                nav_list_add_str = nav_list_add_str + cur_NavContent_content_text
                            elif isinstance(cur_NavContent_content, Tag):
                                if cur_NavContent_content.name == 'br' and nav_list_add_str != '':
                                    cur_td_dict[nav_head]['list'].append(nav_list_add_str)
                                    nav_list_add_str = ''
                                elif cur_NavContent_content.name == 'table':
                                    tmp_dict = self.get_table_info_3(url, cur_NavContent_content)
                                    for tmp_dict_key in tmp_dict.keys():
                                        new_tmp_dict_key = self.check_repeat_key(tmp_dict_key,
                                                                                 cur_td_dict[nav_head].keys())
                                        cur_td_dict[nav_head][new_tmp_dict_key] = tmp_dict[tmp_dict_key]
                                else:
                                    cur_NavContent_content_text = self.str_re(cur_NavContent_content.get_text())
                                    nav_list_add_str = nav_list_add_str + cur_NavContent_content_text

                                    links = cur_NavContent_content.find_all('a')
                                    if cur_NavContent_content.name == 'a':
                                        links.insert(0, cur_NavContent_content)
                                    links_url, links_title, links_ori_title = self.get_links_title(url, base_pre_url,
                                                                                                   links)
                                    cur_td_dict[nav_head]['links'].extend(links_url)
                                    cur_td_dict[nav_head]['links_title'].extend(links_title)
                                    cur_td_dict[nav_head]['links_ori_title'].extend(links_title)
                        if nav_list_add_str != '':
                            cur_td_dict[nav_head]['list'].append(nav_list_add_str)
                            nav_list_add_str = ''


                elif cur_td_content.name == 'p' and 'style' in cur_td.attrs.keys() and re.search('width:\s*50%',
                                                                                                 cur_td.attrs['style']):
                    # 1964年巴西政变-参战方
                    td_middle_b = self.str_re(cur_td_content.get_text())
                    cur_td_dict[td_middle_b] = {'list': [], 'links': [], 'links_title': [], 'links_ori_title': []}
                    td_middle_b_links = cur_td_content.find_all('a')
                    links_url, links_title, links_ori_title = self.get_links_title(url, base_pre_url, td_middle_b_links)
                    cur_td_dict[td_middle_b]['th_links'] = {'links': links_url, 'links_title': links_title,
                                                            'links_ori_title': links_ori_title}
                elif cur_td_content.name == 'ul' or (
                        cur_td_content.name == 'div' and 'class' in cur_td_content.attrs.keys() and 'plainlist' in
                        cur_td_content.attrs['class']) \
                        or (cur_td_content.name == 'div' and 'class' in cur_td_content.attrs.keys() and 'hlist' in
                            cur_td_content.attrs['class']):
                    # 含有多个li
                    lis = cur_td_content.find_all('li')
                    for cur_li in lis:
                        li_text = self.str_re(cur_li.get_text())
                        if li_text != '':
                            if td_middle_b == '':
                                cur_td_dict['list'].append(li_text)
                            else:
                                cur_td_dict[td_middle_b]['list'].append(li_text)
                        links = cur_li.find_all('a')
                        links_url, links_title, links_ori_title = self.get_links_title(url, base_pre_url, links)
                        if td_middle_b == '':
                            cur_td_dict['links'].extend(links_url)
                            cur_td_dict['links_title'].extend(links_title)
                            cur_td_dict['links_ori_title'].extend(links_ori_title)
                        else:
                            cur_td_dict[td_middle_b]['links'].extend(links_url)
                            cur_td_dict[td_middle_b]['links_title'].extend(links_title)
                            cur_td_dict[td_middle_b]['links_ori_title'].extend(links_ori_title)
                elif cur_td_content.name == 'span' and "class" in cur_td_content.attrs.keys() and 'street-address' in \
                        cur_td_content.attrs['class']:
                    cur_td_b = ''
                    cur_td_add_str = ''
                    cur_td_content_contents = cur_td_content.contents
                    for cur_td_content_content in cur_td_content_contents:
                        if isinstance(cur_td_content_content, str):
                            cur_td_add_str = cur_td_add_str + self.str_re(str(cur_td_content_content))
                        elif cur_td_content_content.name == 'b':
                            if cur_td_add_str != '':
                                if cur_td_b != '':
                                    cur_td_dict[cur_td_b]['list'].append(cur_td_add_str)
                                else:
                                    cur_td_dict['list'].append(cur_td_add_str)
                                cur_td_add_str = ''
                            cur_td_b = self.str_re(cur_td_content_content.get_text())
                            cur_td_b_links = cur_td_content_content.find_all('a')
                            links_url, links_title, links_ori_title = self.get_links_title(url, base_pre_url,
                                                                                           cur_td_b_links)
                            cur_td_dict[cur_td_b] = {'list': [], 'links': [], 'links_title': [], 'links_ori_title': []}
                            cur_td_dict[cur_td_b]['th_links'] = {'links': links_url, 'links_title': links_title,
                                                                 'links_ori_title': links_ori_title}
                        else:
                            cur_td_add_str = cur_td_add_str + self.str_re(cur_td_content_content.get_text())
                            links = cur_td_content_content.find_all('a')
                            if cur_td_content_content.name == 'a':
                                links.insert(0, cur_td_content_content)
                            links_url, links_title, links_ori_title = self.get_links_title(url, base_pre_url, links)
                            if cur_td_b != '':
                                cur_td_dict[cur_td_b]['links'].extend(links_url)
                                cur_td_dict[cur_td_b]['links_title'].extend(links_title)
                                cur_td_dict[cur_td_b]['links_ori_title'].extend(links_ori_title)
                            else:
                                cur_td_dict['links'].extend(links_url)
                                cur_td_dict['links_title'].extend(links_title)
                                cur_td_dict['links_ori_title'].extend(links_ori_title)
                    if cur_td_add_str != '':
                        if cur_td_b != '':
                            cur_td_dict[cur_td_b]['list'].append(cur_td_add_str)
                        else:
                            cur_td_dict['list'].append(cur_td_add_str)
                        cur_td_add_str = ''

                elif first_content and cur_td_content.name == 'span' and cur_td_content.find('b'):
                    default_td_head = self.str_re(cur_td_content.get_text())
                elif cur_td_content.name == 'b':
                    # default_td_head = self.str_re(cur_td_content.get_text())
                    td_middle_b = self.str_re(cur_td_content.get_text())
                    cur_td_dict[td_middle_b] = {'list': [], 'links': [], 'links_title': [], 'links_ori_title': []}
                    td_middle_b_links = cur_td_content.find_all('a')
                    links_url, links_title, links_ori_title = self.get_links_title(url, base_pre_url, td_middle_b_links)
                    cur_td_dict[td_middle_b]['th_links'] = {'links': links_url, 'links_title': links_title,
                                                            'links_ori_title': links_ori_title}
                    if first_content and cur_td_content.get_text().endswith(
                            '：') and 'style' in cur_td.attrs.keys() and re.search('text-align:\s*center',
                                                                                  cur_td.attrs['style']):
                        should_under_first_key = True
                elif first_content and cur_td_content.find_all('div', attrs={"class": "NavFrame"}) != []:
                    if 'style' in cur_td_content.attrs.keys() and re.search('display:\s*none',
                                                                            cur_td_content.attrs['style']):
                        continue
                    should_under_first_key = True
                    NavFrames = cur_td_content.find_all('div', attrs={"class": "NavFrame"})
                    for cur_NavFrames in NavFrames:
                        nav_head = self.str_re(cur_NavFrames.find('div', attrs={"class": "NavHead"}).get_text())
                        new_nav_head = nav_head
                        cnt = 2
                        while new_nav_head in cur_td_dict.keys():
                            new_nav_head = nav_head + '_' + str(cnt)
                            cnt += 1
                        nav_head = new_nav_head
                        cur_td_dict[nav_head] = {'list': [], 'links': [], 'links_title': [], 'links_ori_title': []}
                        ul = cur_NavFrames.find('ul', attrs={"class": "NavContent"})
                        if ul == None:
                            continue
                        lis = ul.find_all('li')
                        for cur_li in lis:
                            plainlist = cur_li.find('div', attrs={"class": "plainlist"})
                            if plainlist != None:
                                plainlist_contents = plainlist.contents
                            else:
                                plainlist_contents = cur_li.contents
                            nav_list_add_str = ''
                            for cur_plainlist_content in plainlist_contents:
                                if isinstance(cur_plainlist_content, str):
                                    cur_plainlist_content_text = str(cur_plainlist_content).strip()
                                    nav_list_add_str = nav_list_add_str + cur_plainlist_content_text
                                elif isinstance(cur_plainlist_content, Tag):
                                    if cur_plainlist_content.name == 'br' and nav_list_add_str != '':
                                        cur_td_dict[nav_head]['list'].append(nav_list_add_str)
                                        nav_list_add_str = ''
                                    else:
                                        cur_plainlist_content_text = self.str_re(cur_plainlist_content.get_text())
                                        nav_list_add_str = nav_list_add_str + cur_plainlist_content_text

                                        links = cur_plainlist_content.find_all('a')
                                        if cur_plainlist_content.name == 'a':
                                            links.insert(0, cur_plainlist_content)
                                        links_url, links_title, links_ori_title = self.get_links_title(url,
                                                                                                       base_pre_url,
                                                                                                       links)
                                        cur_td_dict[nav_head]['links'].extend(links_url)
                                        cur_td_dict[nav_head]['links_title'].extend(links_title)
                                        cur_td_dict[nav_head]['links_ori_title'].extend(links_ori_title)
                            if nav_list_add_str != '':
                                cur_td_dict[nav_head]['list'].append(nav_list_add_str)
                elif cur_td_content.name == 'table':
                    # 含有table
                    table_body = cur_td_content.find('tbody')
                    if table_body == None:
                        continue
                    table_body_trs = table_body.find_all('tr')
                    if len(table_body_trs) == 1 and table_body.find_all('th') == []:
                        if ('class' in cur_td_content.attrs.keys() and 'toccolours' in cur_td_content.attrs['class']) \
                                or ('class' in cur_td.attrs.keys() and 'toccolours' in cur_td.attrs['class']):
                            should_under_first_key = True
                            is_table_th_background = True
                        cur_table_body_tr = table_body_trs[0]
                        cur_table_body_tr_tds = cur_table_body_tr.find_all('td')
                        for cur_table_body_tr_td in cur_table_body_tr_tds:
                            cur_table_td = 'table_td'
                            new_cur_table_td = cur_table_td + '_1'
                            cnt = 2
                            while new_cur_table_td in cur_td_dict.keys():
                                new_cur_table_td = cur_table_td + '_' + str(cnt)
                                cnt += 1
                            cur_table_td = new_cur_table_td
                            cur_td_dict[cur_table_td] = {'list': [], 'links': [], 'links_title': [],
                                                         'links_ori_title': []}
                            cur_table_body_tr_td_contents = cur_table_body_tr_td.contents
                            table_b = ''
                            cur_table_td_add_str = ''
                            for cur_table_body_tr_td_content in cur_table_body_tr_td_contents:
                                if cur_table_body_tr_td_content.name == 'div':
                                    div_contents = cur_table_body_tr_td_content.contents
                                    for cur_div_content in div_contents:
                                        if cur_div_content.name == 'p':
                                            cur_p_contents = cur_div_content.contents
                                            add_str = ''
                                            for cur_p_content in cur_p_contents:
                                                if isinstance(cur_p_content, str):
                                                    cur_td_content_text = self.str_re(str(cur_p_content))
                                                    add_str = add_str + cur_td_content_text
                                                elif isinstance(cur_p_content, Tag):
                                                    if cur_p_content.name == 'br' and add_str != '':
                                                        if table_b == '':
                                                            cur_td_dict[cur_table_td]['list'].append(add_str)
                                                        else:
                                                            cur_td_dict[cur_table_td][table_b]['list'].append(add_str)
                                                        add_str = ''
                                                    elif cur_p_content.name == 'b':
                                                        table_b = self.str_re(cur_p_content.get_text())
                                                        cur_td_dict[cur_table_td][table_b] = {'list': [], 'links': [],
                                                                                              'links_title': [],
                                                                                              'links_ori_title': []}
                                                    else:
                                                        cur_p_content_text = self.str_re(cur_p_content.get_text())
                                                        add_str = add_str + cur_p_content_text
                                                        links = cur_p_content.find_all('a')
                                                        if cur_p_content.name == 'a':
                                                            links.insert(0, cur_p_content)
                                                        links_url, links_title, links_ori_title = self.get_links_title(
                                                            url, base_pre_url, links)
                                                        if table_b == '':
                                                            cur_td_dict[cur_table_td]['links'].extend(links_url)
                                                            cur_td_dict[cur_table_td]['links_title'].extend(links_title)
                                                            cur_td_dict[cur_table_td]['links_ori_title'].extend(
                                                                links_ori_title)
                                                        else:
                                                            cur_td_dict[cur_table_td][table_b]['links'].extend(
                                                                links_url)
                                                            cur_td_dict[cur_table_td][table_b]['links_title'].extend(
                                                                links_title)
                                                            cur_td_dict[cur_table_td][table_b][
                                                                'links_ori_title'].extend(links_ori_title)
                                            if add_str != '':
                                                if table_b == '':
                                                    cur_td_dict[cur_table_td]['list'].append(add_str)
                                                else:
                                                    cur_td_dict[cur_table_td][table_b]['list'].append(add_str)
                                        elif cur_div_content.name == 'ul':
                                            lis = cur_div_content.find_all('li')
                                            if table_b == '':
                                                new_table_b = 'table_ul_1'
                                                cnt = 2
                                                while new_table_b in cur_td_dict[cur_table_td].keys():
                                                    new_table_b = 'table_ul' + '_' + str(cnt)
                                                    cnt += 1
                                                table_b = new_table_b
                                                cur_td_dict[cur_table_td][table_b] = {'list': [], 'links': [],
                                                                                      'links_title': [],
                                                                                      'links_ori_title': []}
                                            for cur_li in lis:
                                                cur_li_text = self.str_re(cur_li.get_text())
                                                cur_td_dict[cur_table_td][table_b]['list'].append(cur_li_text)
                                                links = cur_li.find_all('a')
                                                links_url, links_title, links_ori_title = self.get_links_title(url,
                                                                                                               base_pre_url,
                                                                                                               links)
                                                cur_td_dict[cur_table_td][table_b]['links'].extend(links_url)
                                                cur_td_dict[cur_table_td][table_b]['links_title'].extend(links_title)
                                                cur_td_dict[cur_table_td][table_b]['links_ori_title'].extend(
                                                    links_ori_title)
                                        elif isinstance(cur_div_content, str):
                                            cur_div_content_text = self.str_re(str(cur_div_content))
                                            if cur_div_content_text != '':
                                                if table_b != '':
                                                    cur_td_dict[cur_table_td][table_b]['list'].append(
                                                        cur_div_content_text)
                                                else:
                                                    cur_td_dict[cur_table_td]['list'].append(cur_div_content_text)
                                        elif isinstance(cur_div_content, Tag):
                                            cur_div_content_text = self.str_re(cur_div_content.get_text())
                                            if cur_div_content_text != '':
                                                if table_b != '':
                                                    cur_td_dict[cur_table_td][table_b]['list'].append(
                                                        cur_div_content_text)
                                                else:
                                                    cur_td_dict[cur_table_td]['list'].append(cur_div_content_text)
                                            links = cur_div_content.find_all('a')
                                            if cur_div_content.name == 'a':
                                                links.insert(0, cur_div_content)
                                            links_url, links_title, links_ori_title = self.get_links_title(url,
                                                                                                           base_pre_url,
                                                                                                           links)
                                            if table_b != '':
                                                cur_td_dict[cur_table_td][table_b]['links'].extend(links_url)
                                                cur_td_dict[cur_table_td][table_b]['links_title'].extend(links_title)
                                                cur_td_dict[cur_table_td][table_b]['links_ori_title'].extend(
                                                    links_ori_title)
                                            else:
                                                cur_td_dict[cur_table_td]['links'].extend(links_url)
                                                cur_td_dict[cur_table_td]['links_title'].extend(links_title)
                                                cur_td_dict[cur_table_td]['links_ori_title'].extend(links_ori_title)
                                elif isinstance(cur_table_body_tr_td_content, str):
                                    cur_table_td_add_str = cur_table_td_add_str + self.str_re(
                                        str(cur_table_body_tr_td_content))
                                elif cur_table_body_tr_td_content.name == 'br' and cur_table_td_add_str != '':
                                    cur_td_dict[cur_table_td]['list'].append(cur_table_td_add_str)
                                    cur_table_td_add_str = ''
                                elif cur_table_body_tr_td_content.name == 'img':
                                    img_src = cur_table_body_tr_td_content.attrs['src']
                                    if img_src.startswith('//'):
                                        img_src = 'https:' + img_src
                                    img_alt = cur_table_body_tr_td_content.attrs['alt']
                                    if img_alt == "":
                                        img_alt = img_src.split("wikipedia/")[-1]
                                    cur_td_dict[cur_table_td]['links'].append(img_src)
                                    cur_td_dict[cur_table_td]['links_title'].append(img_alt)
                                    cur_td_dict[cur_table_td]['links_ori_title'].append(img_alt)
                                else:
                                    cur_table_td_add_str = cur_table_td_add_str + self.str_re(
                                        cur_table_body_tr_td_content.get_text())
                                    links = cur_table_body_tr_td_content.find_all('a')
                                    if cur_table_body_tr_td_content.name == 'a':
                                        links.insert(0, cur_table_body_tr_td_content)
                                    links_url, links_title, links_ori_title = self.get_links_title(url, base_pre_url,
                                                                                                   links)
                                    cur_td_dict[cur_table_td]['links'].extend(links_url)
                                    cur_td_dict[cur_table_td]['links_title'].extend(links_title)
                                    cur_td_dict[cur_table_td]['links_ori_title'].extend(links_ori_title)
                            if cur_table_td_add_str != '':
                                cur_td_dict[cur_table_td]['list'].append(cur_table_td_add_str)
                    else:
                        # 含有th、td形式的table,如安徽财经大学：位置
                        tmp_dict = self.get_table_info_3(url, cur_td_content)
                        for tmp_dict_key in tmp_dict.keys():
                            new_tmp_dict_key = self.check_repeat_key(tmp_dict_key, cur_td_dict.keys())
                            cur_td_dict[new_tmp_dict_key] = tmp_dict[tmp_dict_key]
                        cur_table_body_tr_th = table_body_trs[0].find('th')
                        if (
                                cur_table_body_tr_th != None and 'style' in cur_table_body_tr_th.attrs.keys() and 'background' in
                                cur_table_body_tr_th.attrs['style']) \
                                or ('style' in cur_td_content.attrs.keys() and 'background' in cur_td_content.attrs[
                            'style']):
                            should_under_first_key = True
                            is_table_th_background = True
                        # cur_table_first_key=''
                        # cur_table_second_key=''
                        # for cur_table_body_tr in table_body_trs:
                        #     cur_table_body_tr_th = cur_table_body_tr.find('th')
                        #     cur_table_body_tr_tds = cur_table_body_tr.find_all('td')
                        #     if cur_table_body_tr_th != None and cur_table_body_tr_tds==[]:
                        #         if 'style' in cur_table_body_tr_th.attrs.keys() and 'background' in cur_table_body_tr_th.attrs['style']:
                        #             should_under_first_key = True
                        #             is_table_th_background = True
                        #             if default_td_head == 'td_content':
                        #                 default_td_head = self.str_re(cur_table_body_tr_th.get_text())
                        #                 cur_table_first_key='table_content'
                        #         if cur_table_first_key=='':
                        #             cur_table_first_key=self.str_re(cur_table_body_tr_th.get_text())
                        #         cur_td_dict[cur_table_first_key] = {'list': [], 'links': [], 'links_title': [],'links_ori_title': []}
                        #         th_links = cur_table_body_tr_th.find_all('a')
                        #         links_url, links_title, links_ori_title = self.get_links_title(url, base_pre_url, th_links)
                        #         cur_td_dict[cur_table_first_key]['th_links'] = {'links': links_url, 'links_title': links_title,'links_ori_title': links_ori_title}
                        #     elif cur_table_body_tr_th != None:
                        #         if cur_table_first_key=='':
                        #             cur_table_first_key='table_content'
                        #             cur_td_dict[cur_table_first_key] = {'list': [], 'links': [], 'links_title': [], 'links_ori_title': []}
                        #         cur_table_second_key = self.str_re(cur_table_body_tr_th.get_text())
                        #         cur_td_dict[cur_table_first_key][cur_table_second_key] = {'list': [], 'links': [], 'links_title': [], 'links_ori_title': []}
                        #         th_links = cur_table_body_tr_th.find_all('a')
                        #         links_url, links_title, links_ori_title = self.get_links_title(url, base_pre_url, th_links)
                        #         cur_td_dict[cur_table_first_key][cur_table_second_key]['th_links'] = {'links': links_url, 'links_title': links_title, 'links_ori_title': links_ori_title}
                        #     for cur_table_body_tr_td in cur_table_body_tr_tds:  # 一级key下的td内容
                        #         cur_table_body_tr_td_contents = cur_table_body_tr_td.contents
                        #         cur_table_body_tr_td_dict = {'list': [], 'links': [], 'links_title': [],'links_ori_title': []}
                        #         cur_table_body_tr_td_list_add_str = ''
                        #         for cur_table_body_tr_td_content in cur_table_body_tr_td_contents:
                        #             if isinstance(cur_table_body_tr_td_content, str):
                        #                 cur_table_body_tr_td_content_text =self.str_re(str(cur_table_body_tr_td_content))
                        #                 cur_table_body_tr_td_list_add_str = cur_table_body_tr_td_list_add_str + cur_table_body_tr_td_content_text
                        #             elif isinstance(cur_table_body_tr_td_content, Tag):
                        #                 if cur_table_body_tr_td_content.name == 'br' and cur_table_body_tr_td_list_add_str != '':  # 换行
                        #                     cur_table_body_tr_td_dict['list'].append(cur_table_body_tr_td_list_add_str)
                        #                     cur_table_body_tr_td_list_add_str = ''
                        #                 else:
                        #                     cur_table_body_tr_td_content_text = self.str_re(cur_table_body_tr_td_content.get_text())
                        #                     cur_table_body_tr_td_list_add_str = cur_table_body_tr_td_list_add_str + cur_table_body_tr_td_content_text
                        #                     links = cur_table_body_tr_td_content.find_all('a')
                        #                     if cur_table_body_tr_td_content.name == 'a':
                        #                         links.insert(0,cur_table_body_tr_td_content)
                        #                     links_url, links_title, links_ori_title = self.get_links_title(url, base_pre_url, links)
                        #                     cur_table_body_tr_td_dict['links'].extend(links_url)
                        #                     cur_table_body_tr_td_dict['links_title'].extend(links_title)
                        #                     cur_table_body_tr_td_dict['links_ori_title'].extend(links_ori_title)
                        #         if cur_table_body_tr_td_list_add_str != '':
                        #             cur_table_body_tr_td_dict['list'].append(cur_table_body_tr_td_list_add_str)
                        #         cur_td_b = 'td_content'
                        #         new_cur_td_b = cur_td_b
                        #         cnt = 2
                        #         if cur_table_body_tr_th != None:
                        #             while new_cur_td_b in cur_td_dict[cur_table_first_key][cur_table_second_key].keys():
                        #                 new_cur_td_b = cur_td_b + '_' + str(cnt)
                        #                 cnt += 1
                        #             cur_td_b = new_cur_td_b
                        #             cur_td_dict[cur_table_first_key][cur_table_second_key][cur_td_b] = cur_table_body_tr_td_dict
                        #         else:
                        #             while new_cur_td_b in cur_td_dict[cur_table_first_key].keys():
                        #                 new_cur_td_b = cur_td_b + '_' + str(cnt)
                        #                 cnt += 1
                        #             cur_td_b = new_cur_td_b
                        #             cur_td_dict[cur_table_first_key][cur_td_b] = cur_table_body_tr_td_dict
                elif len(
                        cur_td_contents) == 1 and first_content and cur_td_content.name == 'div' and cur_td_content.find(
                    'b', recursive=False):
                    # 含有一个div节点，且含有b子节点，中华人民共和国-网站;外伶仃岛-CHN
                    cur_div_contents = cur_td_contents[0].contents
                    for cur_div_content in cur_div_contents:
                        if isinstance(cur_div_content, str):
                            cur_td_content_text = str(cur_div_content).strip()
                            list_add_str = list_add_str + cur_td_content_text
                        elif isinstance(cur_div_content, Tag):
                            if cur_div_content.name == 'br' and list_add_str != '':
                                cur_td_dict['list'].append(list_add_str)
                                list_add_str = ''
                            elif cur_div_content.name == 'b':
                                cur_b = self.str_re(cur_div_content.get_text())
                                default_td_head = cur_b
                                if '网站' in default_td_head or '網站' in default_td_head:
                                    should_under_first_key = True
                                th_links = cur_div_content.find_all('a')
                                links_url, links_title, links_ori_title = self.get_links_title(url, base_pre_url,
                                                                                               th_links)
                                cur_td_dict['th_links'] = {'links': links_url, 'links_title': links_title,
                                                           'links_ori_title': links_ori_title}
                            else:
                                cur_td_content_text = self.str_re(cur_div_content.get_text())
                                if cur_div_content.name == 'div' and 'class' in cur_div_content.attrs.keys() and 'NavFrame' in \
                                        cur_div_content.attrs['class'] and list_add_str != '':
                                    cur_td_content_text = '\n' + cur_td_content_text
                                list_add_str = list_add_str + cur_td_content_text
                                links = cur_div_content.find_all('a')
                                if cur_div_content.name == 'a':
                                    links.insert(0, cur_div_content)
                                links_url, links_title, links_ori_title = self.get_links_title(url, base_pre_url, links)
                                cur_td_dict['links'].extend(links_url)
                                cur_td_dict['links_title'].extend(links_title)
                                cur_td_dict['links_ori_title'].extend(links_ori_title)

                    if list_add_str != '':
                        cur_td_dict['list'].append(list_add_str)
                        list_add_str = ''
                elif cur_td_content.name == 'img':
                    img_src = cur_td_content.attrs['src']
                    if img_src.startswith('//'):
                        img_src = 'https:' + img_src
                    img_alt = cur_td_content.attrs['alt']
                    if img_alt == "":
                        img_alt = img_src.split("wikipedia/")[-1]
                    cur_td_dict['links'].append(img_src)
                    cur_td_dict['links_title'].append(img_alt)
                    cur_td_dict['links_ori_title'].append(img_alt)
                else:
                    cur_td_content_text, _, _, _, _ = self.get_one_tag_content_br(url, base_pre_url, cur_td_content)
                    cur_td_content_text = self.str_re(cur_td_content_text)
                    if cur_td_content.name == 'div' and 'class' in cur_td_content.attrs.keys() and 'NavFrame' in \
                            cur_td_content.attrs['class'] and list_add_str != '':
                        cur_td_content_text = '\n' + cur_td_content_text
                    list_add_str = list_add_str + cur_td_content_text
                    links = cur_td_content.find_all('a')
                    if cur_td_content.name == 'a':
                        links.insert(0, cur_td_content)
                    links_url, links_title, links_ori_title = self.get_links_title(url, base_pre_url, links)
                    cur_td_dict['links'].extend(links_url)
                    cur_td_dict['links_title'].extend(links_title)
                    cur_td_dict['links_ori_title'].extend(links_ori_title)

            first_content = False
        if list_add_str != '':
            if td_middle_b == "":
                cur_td_dict['list'].append(list_add_str)
            else:
                cur_td_dict[td_middle_b]['list'].append(list_add_str)
        return cur_td_dict, default_td_head, should_under_first_key, is_table_th_background

    def check_repeat_key(self, cur_key, keys):
        if cur_key == "":
            cur_key = "empty_key"
        new_key = cur_key
        cnt = 2
        while new_key in keys:
            new_key = cur_key + '_' + str(cnt)
            cnt += 1
        return new_key

    def get_table_info_3(self, url, info_table):
        tot_dict = {}
        base_pre_url = 'https://zh.wikipedia.org'
        if info_table.find('tbody', recursive=False) == None:
            return tot_dict
        trs = info_table.find('tbody', recursive=False).find_all('tr', recursive=False)
        cur_first_key = ''
        cur_second_key = ''
        cur_second_key_background_color = False
        cur_third_key = ''
        cur_fourth_key = ''
        first_tr = True

        if info_table.find('caption', recursive=False):
            caption_str = self.str_re("\n".join(list(info_table.find('caption', recursive=False).stripped_strings)))
            links_url, links_title, links_ori_title = self.get_links_title(url, base_pre_url, info_table.find('caption',
                                                                                                              recursive=False).find_all(
                'a'))
            cur_first_key = caption_str
            cur_second_key = ''
            cur_third_key = ''
            cur_fourth_key = ''
            tot_dict[cur_first_key] = {'list': [], 'links': [], 'links_title': [], 'links_ori_title': []}
            tot_dict[cur_first_key]['th_links'] = {'links': links_url, 'links_title': links_title, 'links_ori_title': links_ori_title}

        td_to_third_key = False
        td_to_second_key = False
        for cur_tr in trs:
            th = cur_tr.find('th', recursive=False)
            tds = cur_tr.find_all('td', recursive=False)

            ths = cur_tr.find_all('th', recursive=False)
            if len(ths) > 1:
                cur_first_key = 'table_content'
                cur_second_key = ''
                cur_third_key = ''
                cur_fourth_key = ''
                tot_dict[cur_first_key] = {'list': [], 'links': [], 'links_title': [], 'links_ori_title': []}
                for cur_th in ths:
                    tmp_th_str = self.str_re("\n".join(list(cur_th.stripped_strings)))
                    links_url, links_title, links_ori_title = self.get_links_title(url, base_pre_url,
                                                                                   cur_th.find_all('a'))
                    cur_second_key = tmp_th_str
                    cur_second_key = self.check_repeat_key(cur_second_key, tot_dict[cur_first_key].keys())
                    cur_third_key = ''
                    cur_fourth_key = ''
                    tot_dict[cur_first_key][cur_second_key] = {'list': [], 'links': [], 'links_title': [],
                                                               'links_ori_title': []}
                    tot_dict[cur_first_key][cur_second_key]['th_links'] = {'links': links_url,
                                                                           'links_title': links_title,
                                                                           'links_ori_title': links_ori_title}

            if th != None and tds == [] and len(ths) == 1:
                '''只含th'''
                tmp_th_str, tmp_th_dict, links_url, links_title, links_ori_title = self.get_one_tag_content_br(url,
                                                                                                               base_pre_url,
                                                                                                               th)

                if th.find('table', recursive=False):
                    tmp_dict = self.get_table_info_3(url, th.find('table'))
                    for tmp_dict_key in tmp_dict.keys():
                        new_tmp_dict_key = self.check_repeat_key(tmp_dict_key, tot_dict.keys())
                        tot_dict[new_tmp_dict_key] = tmp_dict[tmp_dict_key]
                    continue

                if cur_first_key == '':
                    cur_first_key = tmp_th_str
                    cur_second_key = ''
                    cur_third_key = ''
                    cur_fourth_key = ''
                    td_to_third_key = False
                    td_to_second_key = False
                    tot_dict[cur_first_key] = {'list': [], 'links': [], 'links_title': [], 'links_ori_title': []}
                    tot_dict[cur_first_key]['th_links'] = {'links': links_url, 'links_title': links_title,
                                                           'links_ori_title': links_ori_title}
                    if tmp_th_dict != {}:
                        tot_dict[cur_first_key] = self.our_merge_dict(tot_dict[cur_first_key], tmp_th_dict)
                else:
                    if ('style' in th.attrs.keys() and 'background' in th.attrs['style']) \
                            or ('class' in th.attrs.keys() and 'navbox-title' in th.attrs['class']) \
                            or ('style' in th.attrs.keys() and re.search("text-align:\s*center",
                                                                         th.attrs['style']) and not re.search(
                        "text-align:\s*left", th.attrs['style'])):
                        cur_second_key_background_color = True
                        cur_second_key = tmp_th_str
                        cur_second_key = self.check_repeat_key(cur_second_key, tot_dict[cur_first_key].keys())
                        cur_third_key = ''
                        cur_fourth_key = ''
                        td_to_third_key = False
                        td_to_second_key = False
                        tot_dict[cur_first_key][cur_second_key] = {'list': [], 'links': [], 'links_title': [],
                                                                   'links_ori_title': []}
                        tot_dict[cur_first_key][cur_second_key]['th_links'] = {'links': links_url,
                                                                               'links_title': links_title,
                                                                               'links_ori_title': links_ori_title}
                        if tmp_th_dict != {}:
                            tot_dict[cur_first_key][cur_second_key] = self.our_merge_dict(
                                tot_dict[cur_first_key][cur_second_key], tmp_th_dict)
                    else:
                        if cur_second_key_background_color:
                            # 北京市、政府
                            cur_third_key = tmp_th_str
                            cur_third_key = self.check_repeat_key(cur_third_key,
                                                                  tot_dict[cur_first_key][cur_second_key].keys())
                            cur_fourth_key = ''
                            td_to_third_key = False
                            tot_dict[cur_first_key][cur_second_key][cur_third_key] = {'list': [], 'links': [],
                                                                                      'links_title': [],
                                                                                      'links_ori_title': []}
                            tot_dict[cur_first_key][cur_second_key][cur_third_key]['th_links'] = {'links': links_url,
                                                                                                  'links_title': links_title,
                                                                                                  'links_ori_title': links_ori_title}
                            if tmp_th_dict != {}:
                                tot_dict[cur_first_key][cur_second_key][cur_third_key] = self.our_merge_dict(
                                    tot_dict[cur_first_key][cur_second_key][cur_third_key], tmp_th_dict)
                        else:
                            cur_second_key = tmp_th_str
                            cur_second_key = self.check_repeat_key(cur_second_key, tot_dict[cur_first_key].keys())
                            cur_third_key = ''
                            cur_fourth_key = ''
                            td_to_third_key = False
                            td_to_second_key = False
                            tot_dict[cur_first_key][cur_second_key] = {'list': [], 'links': [], 'links_title': [],
                                                                       'links_ori_title': []}
                            tot_dict[cur_first_key][cur_second_key]['th_links'] = {'links': links_url,
                                                                                   'links_title': links_title,
                                                                                   'links_ori_title': links_ori_title}
                            if tmp_th_dict != {}:
                                tot_dict[cur_first_key][cur_second_key] = self.our_merge_dict(
                                    tot_dict[cur_first_key][cur_second_key], tmp_th_dict)

            elif th != None and len(ths) == 1:
                '''含th、td'''
                tmp_th_str = self.str_re("\n".join(list(th.stripped_strings)))
                links_url, links_title, links_ori_title = self.get_links_title(url, base_pre_url, th.find_all('a'))
                if tmp_th_str.startswith('•'):
                    if cur_second_key_background_color:
                        if cur_third_key != '':
                            cur_fourth_key = tmp_th_str
                            cur_fourth_key = self.check_repeat_key(cur_fourth_key, tot_dict[cur_first_key][cur_second_key][
                                cur_third_key].keys())
                            tot_dict[cur_first_key][cur_second_key][cur_third_key][cur_fourth_key] = {'list': [],
                                                                                                      'links': [],
                                                                                                      'links_title': [],
                                                                                                      'links_ori_title': []}
                            tot_dict[cur_first_key][cur_second_key][cur_third_key][cur_fourth_key]['th_links'] = {
                                'links': links_url, 'links_title': links_title, 'links_ori_title': links_ori_title}
                        else:
                            cur_third_key = tmp_th_str
                            cur_third_key = self.check_repeat_key(cur_third_key,
                                                                  tot_dict[cur_first_key][cur_second_key].keys())
                            cur_fourth_key = ''
                            td_to_third_key = False
                            tot_dict[cur_first_key][cur_second_key][cur_third_key] = {'list': [], 'links': [],
                                                                                      'links_title': [],
                                                                                      'links_ori_title': []}
                            tot_dict[cur_first_key][cur_second_key][cur_third_key]['th_links'] = {'links': links_url,
                                                                                                  'links_title': links_title,
                                                                                                  'links_ori_title': links_ori_title}
                    else:
                        cur_third_key = tmp_th_str
                        cur_third_key = self.check_repeat_key(cur_third_key,
                                                              tot_dict[cur_first_key][cur_second_key].keys())
                        cur_fourth_key = ''
                        td_to_third_key = False
                        tot_dict[cur_first_key][cur_second_key][cur_third_key] = {'list': [], 'links': [],
                                                                                  'links_title': [],
                                                                                  'links_ori_title': []}
                        tot_dict[cur_first_key][cur_second_key][cur_third_key]['th_links'] = {'links': links_url,
                                                                                              'links_title': links_title,
                                                                                              'links_ori_title': links_ori_title}
                else:
                    if cur_second_key_background_color:
                        cur_third_key = tmp_th_str
                        cur_third_key = self.check_repeat_key(cur_third_key,
                                                              tot_dict[cur_first_key][cur_second_key].keys())
                        cur_fourth_key = ''
                        td_to_third_key = False
                        tot_dict[cur_first_key][cur_second_key][cur_third_key] = {'list': [], 'links': [],
                                                                                  'links_title': [],
                                                                                  'links_ori_title': []}
                        tot_dict[cur_first_key][cur_second_key][cur_third_key]['th_links'] = {'links': links_url,
                                                                                              'links_title': links_title,
                                                                                              'links_ori_title': links_ori_title}
                    else:
                        if cur_first_key == "":
                            cur_first_key = 'table_content'
                            tot_dict[cur_first_key] = {'list': [], 'links': [], 'links_title': [],
                                                       'links_ori_title': []}
                        cur_second_key = tmp_th_str
                        cur_second_key = self.check_repeat_key(cur_second_key, tot_dict[cur_first_key].keys())
                        cur_third_key = ''
                        cur_fourth_key = ''
                        td_to_third_key = False
                        td_to_second_key = False
                        tot_dict[cur_first_key][cur_second_key] = {'list': [], 'links': [], 'links_title': [],
                                                                   'links_ori_title': []}
                        tot_dict[cur_first_key][cur_second_key]['th_links'] = {'links': links_url,
                                                                               'links_title': links_title,
                                                                               'links_ori_title': links_ori_title}

            first_td = True

            for cur_td in tds:
                if cur_first_key == '':
                    if first_tr and 'style' in cur_td.attrs.keys() and 'background' in cur_td.attrs['style']:
                        tmp_th_str = self.str_re("\n".join(list(cur_td.stripped_strings)))
                        links_url, links_title, links_ori_title = self.get_links_title(url, base_pre_url,
                                                                                       cur_td.find_all('a'))
                        cur_first_key = tmp_th_str
                        cur_second_key = ''
                        cur_third_key = ''
                        cur_fourth_key = ''
                        tot_dict[cur_first_key] = {'list': [], 'links': [], 'links_title': [], 'links_ori_title': []}
                        tot_dict[cur_first_key]['th_links'] = {'links': links_url, 'links_title': links_title,
                                                               'links_ori_title': links_ori_title}
                        first_td = False
                        continue
                    else:
                        cur_first_key = 'table_content'
                        cur_second_key = ''
                        cur_third_key = ''
                        cur_fourth_key = ''
                        tot_dict[cur_first_key] = {'list': [], 'links': [], 'links_title': [], 'links_ori_title': []}

                cur_td_contents = cur_td.contents
                new_cur_td_contents = []
                for tmp_content in cur_td_contents:
                    if isinstance(tmp_content, Tag) or str(tmp_content).strip() != '':
                        new_cur_td_contents.append(tmp_content)
                cur_td_contents = new_cur_td_contents
                if len(tds) == 1 and len(cur_td_contents) == 1 and cur_td.find('b') \
                        and 'style' in cur_td.attrs.keys() and 'background' in cur_td.attrs['style'] and re.search(
                    "text-align:\s*center", cur_td.attrs['style']):
                    # 中央军委纪律检查委员会案件审理局-主要领导
                    tmp_th_str = self.str_re("\n".join(list(cur_td.stripped_strings)))
                    links_url, links_title, links_ori_title = self.get_links_title(url, base_pre_url,
                                                                                   cur_td.find_all('a'))
                    cur_second_key = tmp_th_str
                    cur_second_key_background_color = True
                    cur_second_key = self.check_repeat_key(cur_second_key, tot_dict[cur_first_key].keys())
                    cur_third_key = ''
                    cur_fourth_key = ''
                    tot_dict[cur_first_key][cur_second_key] = {'list': [], 'links': [], 'links_title': [],
                                                               'links_ori_title': []}
                    tot_dict[cur_first_key][cur_second_key]['th_links'] = {'links': links_url,
                                                                           'links_title': links_title,
                                                                           'links_ori_title': links_ori_title}
                    first_td = False
                    continue

                if th == None and first_td and len(tds) == 2 and cur_td.find('b', recursive=False):
                    # 09V型核潜艇
                    tmp_th_str = self.str_re("\n".join(list(cur_td.stripped_strings)))
                    links_url, links_title, links_ori_title = self.get_links_title(url, base_pre_url,
                                                                                   cur_td.find_all('a'))
                    if cur_second_key != "":
                        cur_third_key = tmp_th_str
                        cur_third_key = self.check_repeat_key(cur_third_key,
                                                              tot_dict[cur_first_key][cur_second_key].keys())
                        cur_fourth_key = ''
                        tot_dict[cur_first_key][cur_second_key][cur_third_key] = {'list': [], 'links': [],
                                                                                  'links_title': [],
                                                                                  'links_ori_title': []}
                        tot_dict[cur_first_key][cur_second_key][cur_third_key]['th_links'] = {'links': links_url,
                                                                                              'links_title': links_title,
                                                                                              'links_ori_title': links_ori_title}
                        first_td = False
                        td_to_third_key = True
                        continue

                if th == None and first_td and len(tds) == 2 and cur_td.find('span', attrs={"class": "nowrap"},
                                                                             recursive=False):
                    # 北美水獺
                    tmp_th_str = self.str_re("\n".join(list(cur_td.stripped_strings)))
                    links_url, links_title, links_ori_title = self.get_links_title(url, base_pre_url,
                                                                                   cur_td.find_all('a'))
                    if cur_second_key != "":
                        cur_third_key = tmp_th_str
                        cur_third_key = self.check_repeat_key(cur_third_key,
                                                              tot_dict[cur_first_key][cur_second_key].keys())
                        cur_fourth_key = ''
                        tot_dict[cur_first_key][cur_second_key][cur_third_key] = {'list': [], 'links': [],
                                                                                  'links_title': [],
                                                                                  'links_ori_title': []}
                        tot_dict[cur_first_key][cur_second_key][cur_third_key]['th_links'] = {'links': links_url,
                                                                                              'links_title': links_title,
                                                                                              'links_ori_title': links_ori_title}
                        first_td = False
                        td_to_third_key = True
                        continue

                if th == None and first_td and len(tds) == 2 and 'style' in info_table.attrs.keys() and re.search(
                        "background-color:\s*#f0f0f0", info_table.attrs["style"]):
                    # 1,3-丙磺酸内酯
                    tmp_th_str = self.str_re("\n".join(list(cur_td.stripped_strings)))
                    links_url, links_title, links_ori_title = self.get_links_title(url, base_pre_url,
                                                                                   cur_td.find_all('a'))
                    if cur_second_key != "" and not td_to_second_key:
                        cur_third_key = tmp_th_str
                        cur_third_key = self.check_repeat_key(cur_third_key,
                                                              tot_dict[cur_first_key][cur_second_key].keys())
                        cur_fourth_key = ''
                        tot_dict[cur_first_key][cur_second_key][cur_third_key] = {'list': [], 'links': [],
                                                                                  'links_title': [],
                                                                                  'links_ori_title': []}
                        tot_dict[cur_first_key][cur_second_key][cur_third_key]['th_links'] = {'links': links_url,
                                                                                              'links_title': links_title,
                                                                                              'links_ori_title': links_ori_title}
                        first_td = False
                        td_to_second_key = False
                        td_to_third_key = True
                        continue
                    else:
                        cur_second_key = tmp_th_str
                        cur_second_key = self.check_repeat_key(cur_second_key, tot_dict[cur_first_key].keys())
                        cur_third_key = ""
                        cur_fourth_key = ''
                        tot_dict[cur_first_key][cur_second_key] = {'list': [], 'links': [], 'links_title': [],
                                                                   'links_ori_title': []}
                        tot_dict[cur_first_key][cur_second_key]['th_links'] = {'links': links_url,
                                                                               'links_title': links_title,
                                                                               'links_ori_title': links_ori_title}
                        first_td = False
                        td_to_second_key = True
                        td_to_third_key = False
                        continue

                if 'style' in cur_td.attrs.keys() and re.search('text-align:\s*center',
                                                                cur_td.attrs['style']) and re.search(
                    'background-color:\s*#cddeff', cur_td.attrs['style']) and re.search('font-weight:\s*bold',
                                                                                        cur_td.attrs['style']):
                    # 居中粗体样式，另做为二级key。北京市-市象征
                    tmp_th_str = self.str_re("\n".join(list(cur_td.stripped_strings)))
                    links_url, links_title, links_ori_title = self.get_links_title(url, base_pre_url,
                                                                                   cur_td.find_all('a'))
                    cur_second_key = tmp_th_str
                    cur_second_key = self.check_repeat_key(cur_second_key, tot_dict[cur_first_key].keys())
                    cur_third_key = ''
                    cur_fourth_key = ''
                    tot_dict[cur_first_key][cur_second_key] = {'list': [], 'links': [], 'links_title': [],
                                                               'links_ori_title': []}
                    tot_dict[cur_first_key][cur_second_key]['th_links'] = {'links': links_url,
                                                                           'links_title': links_title,
                                                                           'links_ori_title': links_ori_title}
                    cur_second_key_background_color = True
                    first_td = False
                    continue

                if first_td and len(tds) == 2 and 'class' in cur_tr.attrs.keys() and 'mergedrow' in cur_tr.attrs[
                    'class']:
                    tmp_th_str = self.str_re("\n".join(list(cur_td.stripped_strings)))
                    links_url, links_title, links_ori_title = self.get_links_title(url, base_pre_url,
                                                                                   cur_td.find_all('a'))
                    if cur_second_key != "":
                        cur_third_key = tmp_th_str
                        cur_third_key = self.check_repeat_key(cur_third_key, tot_dict[cur_first_key][cur_second_key].keys())
                        cur_fourth_key = ''
                        tot_dict[cur_first_key][cur_second_key][cur_third_key] = {'list': [], 'links': [],
                                                                                  'links_title': [], 'links_ori_title': []}
                        tot_dict[cur_first_key][cur_second_key][cur_third_key]['th_links'] = {'links': links_url,
                                                                                              'links_title': links_title,
                                                                                              'links_ori_title': links_ori_title}
                        first_td = False
                        continue
                    else:
                        cur_second_key = tmp_th_str
                        cur_second_key = self.check_repeat_key(cur_second_key, tot_dict[cur_first_key].keys())
                        cur_third_key = ""
                        cur_fourth_key = ''
                        tot_dict[cur_first_key][cur_second_key] = {'list': [], 'links': [], 'links_title': [],
                                                                   'links_ori_title': []}
                        tot_dict[cur_first_key][cur_second_key]['th_links'] = {'links': links_url,
                                                                               'links_title': links_title,
                                                                               'links_ori_title': links_ori_title}
                        first_td = False
                        td_to_second_key = True
                        td_to_third_key = False
                        continue

                if first_td and len(tds) == 1 and 'class' in cur_tr.attrs.keys() and 'mergedrow' in cur_tr.attrs[
                    'class'] \
                        and cur_td.find('b'):
                    # 浊水溪、支流
                    tmp_th_str = self.str_re("\n".join(list(cur_td.stripped_strings)))
                    links_url, links_title, links_ori_title = self.get_links_title(url, base_pre_url,
                                                                                   cur_td.find_all('a'))
                    cur_second_key = tmp_th_str
                    cur_second_key = self.check_repeat_key(cur_second_key, tot_dict[cur_first_key].keys())
                    cur_third_key = ''
                    cur_fourth_key = ''
                    tot_dict[cur_first_key][cur_second_key] = {'list': [], 'links': [], 'links_title': [],
                                                               'links_ori_title': []}
                    tot_dict[cur_first_key][cur_second_key]['th_links'] = {'links': links_url,
                                                                           'links_title': links_title,
                                                                           'links_ori_title': links_ori_title}
                    first_td = False
                    continue

                cur_td_dict, default_td_head, should_under_first_key, is_table_th_background = self.get_td_contents(url,
                                                                                                                    base_pre_url,
                                                                                                                    cur_td)
                if cur_td_dict == None:
                    first_td = False
                    continue

                cur_td_b = default_td_head
                new_cur_td_b = cur_td_b
                cnt = 2
                if cur_second_key == "" or (should_under_first_key and not cur_second_key_background_color) or (
                        should_under_first_key and is_table_th_background) \
                        or "若非注明，所有数据均出自" in cur_td.get_text():
                    while new_cur_td_b in tot_dict[cur_first_key].keys():
                        new_cur_td_b = cur_td_b + '_' + str(cnt)
                        cnt += 1
                elif cur_third_key == "" or (th == None and cur_second_key_background_color):
                    while new_cur_td_b in tot_dict[cur_first_key][cur_second_key].keys():
                        new_cur_td_b = cur_td_b + '_' + str(cnt)
                        cnt += 1
                elif cur_fourth_key == "":
                    while new_cur_td_b in tot_dict[cur_first_key][cur_second_key][cur_third_key].keys():
                        new_cur_td_b = cur_td_b + '_' + str(cnt)
                        cnt += 1
                else:
                    while new_cur_td_b in tot_dict[cur_first_key][cur_second_key][cur_third_key][cur_fourth_key].keys():
                        new_cur_td_b = cur_td_b + '_' + str(cnt)
                        cnt += 1
                cur_td_b = new_cur_td_b
                if cur_second_key == "" or (should_under_first_key and not cur_second_key_background_color) or (
                        should_under_first_key and is_table_th_background) \
                        or "若非注明，所有数据均出自" in cur_td.get_text():
                    tot_dict[cur_first_key][cur_td_b] = cur_td_dict
                elif cur_third_key == "" or (th == None and cur_second_key_background_color and not td_to_third_key):
                    tot_dict[cur_first_key][cur_second_key][cur_td_b] = cur_td_dict
                elif cur_fourth_key == "":
                    tot_dict[cur_first_key][cur_second_key][cur_third_key][cur_td_b] = cur_td_dict
                else:
                    tot_dict[cur_first_key][cur_second_key][cur_third_key][cur_fourth_key][cur_td_b] = cur_td_dict
                first_td = False
            first_tr = False
        return self.clean_blank_table_info(tot_dict)

    def pipeline_save(self,item):
        save_name=item.title
        save_name=str(save_name).strip()
        save_name = save_name.replace(':', '-')#\ / : * ? " < > |
        save_name = save_name.replace('?', '-')
        save_name = save_name.replace('*', '-')
        save_name = save_name.replace('"', '-')
        save_name = save_name.replace('<', '-')
        save_name = save_name.replace('>', '-')
        save_name = save_name.replace('.', '-')
        save_name = save_name.replace('|', '-')
        save_name = save_name.replace('/', '--')
        save_name = save_name.replace('\\', '--')
        save_dir=self.save_pre_dir+save_name+'/'

        if not os.path.exists(save_dir):
            os.mkdir(save_dir)

        base_csv=save_dir+'base.csv'
        with open(base_csv, 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter=self.sep)
            writer.writerow(["title", "zhwiki_url","enwiki_url"])
            writer.writerow([item.title,item.zhwiki_url,item.enwiki_url])

        page_csv=save_dir+'page.csv'
        with open(page_csv, 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter=self.sep)
            writer.writerow(["page_text"])
            if item.page_text is not None:
                writer.writerow([item.page_text])

        catalog_csv=save_dir+'catalog.csv'
        with open(catalog_csv, 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter=self.sep)
            writer.writerow(["number","text"])
            if item.catalog is not None:
                writer.writerows(item.catalog)

        first_par_csv = save_dir + 'first_par.csv'
        with open(first_par_csv, 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter=self.sep)
            writer.writerow(["par_text"])
            if item.first_par is not None:
                writer.writerow([item.first_par])

        info_csv=save_dir+'infos.json'
        with open(info_csv, 'w', encoding="utf-8", newline='') as fi:
            if item.infos is not None:
                char_infos_json = json.dumps(item.infos, ensure_ascii=False, indent=4)
                fi.write(char_infos_json)


        img_csv=save_dir+'imgs.csv'
        with open(img_csv, 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter=self.sep)
            writer.writerow(["wiki_url",'small_url','alt'])
            if item.imgs is not None:
                self.log.info("get imgs:%d"%(len(item.imgs)))
                writer.writerows(item.imgs)

        thumbs_csv=save_dir+'thumbs.csv'
        with open(thumbs_csv, 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter=self.sep)
            writer.writerow(["head",'img_urls','caption'])
            if item.thumbs is not None:
                self.log.info("get thumbs:%d"%(len(item.thumbs)))
                writer.writerows(item.thumbs)

        if self.is_download_img:
            media_img_dir=save_dir+'media_img'
            if not os.path.exists(media_img_dir):
                os.mkdir(media_img_dir)
            downloads_info=self.download_imgs(item,media_img_dir)
            downloads_info_csv=media_img_dir+'/downloads_info.csv'
            with open(downloads_info_csv, 'w', encoding="utf8", newline='') as fi:
                writer = csv.writer(fi, delimiter=self.sep)
                writer.writerow(["wiki_url",'img_url','save_file'])
                if downloads_info is not None:
                    writer.writerows(downloads_info)

        self.log.info("%s: 保存完毕"%(item.title))


    def download_imgs(self,item,media_img_dir):
        all_imgs=item.imgs
        if all_imgs is None:
            return None
        img_wiki_urls=[]
        for i in all_imgs:
            img_wiki_urls.append(i[0])

        downloads_info=[]
        download_for_ipg_info=[]
        for cur_img_url in img_wiki_urls:
            htmlContent = self.getResponseContent(cur_img_url)
            soup = BeautifulSoup(htmlContent, 'lxml')
            full_media=soup.find('div',attrs={"class":"fullMedia"})
            link=full_media.find('p').find('a')
            href='https:'+link.attrs['href']
            title=link.attrs['title']
            img_file=media_img_dir+'/'+title
            downloads_info.append([cur_img_url,href,img_file])
            download_for_ipg_info.append([title,href,img_file])

        pool = ThreadPool(processes=THREADS)
        pool.map(self.download_for_figure,download_for_ipg_info)
        pool.close()
        pool.join()
            #self.download_for_ipg(title,href,img_file)
        self.log.info("downloading pics finishs.")
        return  downloads_info

    def download_for_figure(self, cur_fig):
        title,url, file=cur_fig[0],cur_fig[1],cur_fig[2]
        cnt = 0
        while cnt <= 10:
            try:
                r = requests.get(url, timeout=(10, 10),headers=HEADER,proxies=PROXIES, stream=True)
                with open(file, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=1024):
                        f.write(chunk)
                # urlretrieve(url, file)
                self.log.info("获取图片%s 成功" % url)
                return
            except:
                cnt += 1
        self.log.error("获取图片%s 失败" % url)
        with open(self.pic_remain_csv, 'a+', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter='\t')
            writer.writerow([title, url, file])
        self.remain_pic_nums += 1









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
                try:
                    if e.code==404:
                        return None
                except Exception as e2:
                    pass
                cnt+=1
                # if cnt>20:
                #     return None
            else:
                self.log.info("返回url:%s 成功, status_code:%d" % (url, int(response.code)))
                return html_text
        self.log.error("返回url:%s 失败" % url)
        return None

    def getExtraLinks(self, url):
        htmlContent = None
        while True:
            htmlContent = requests.get(url).text
            if htmlContent != None:
                break
        soup = BeautifulSoup(htmlContent, 'lxml')
        extralinks = extralinks = soup.find('span', attrs={"class": "mw-headline"}, text=re.compile("外部連結"))
        imdbID = None
        links = []
        if extralinks != None:
            extralinks = extralinks.parent
            ul = extralinks.next_sibling.next_sibling
            while ul.name != 'ul':
                ul = ul.next_sibling.next_sibling
                if ul == None:
                    return imdbID, links
            lis = ul.find_all('li')
            for li in lis:
                url_a_s = li.find_all('a', class_="external")
                for a in url_a_s:
                    cur_url = a.attrs['href']
                    text = li.get_text().strip()
                    links.append([text, cur_url])
                    rst = re.search("/(tt\d+)", cur_url)
                    if rst != None:
                        imdbID = rst.group(1).strip()
        return imdbID, links

    def getLists(self):
        # save_csv = 'event_all.csv'
        # keyworkds=['六四','邓小平','鄧小平','文革','文化大革命']
        # if os.path.exists(save_csv):
        #     df=pd.read_csv(save_csv,sep='\t')
        #     re_data=None
        #     for cur_word in keyworkds:
        #         data=df[df.title.str.contains(cur_word)]
        #         print(data)
        #         if re_data is None:
        #             re_data=data
        #         else:
        #             re_data=pd.concat([re_data,data])
        #     re_data=re_data.drop_duplicates()
        #     print(re_data)
        #     cates_url=re_data[re_data['isleaf']==0]['url'].unique().tolist()
        #     queue_cate=cates_url
        #     viewed_cate=set(cates_url)
        #     next_queue=[]
        #     while queue_cate!=[]:
        #         next_queue = []
        #         for cur_cate in queue_cate:
        #             viewed_cate.add(cur_cate)
        #             tmp=df[df.pre_url.str.contains(cur_cate)]
        #             tmp_cate=tmp[tmp['isleaf']==0]['url'].unique().tolist()
        #             re_data=pd.concat([re_data,tmp])
        #             for i in tmp_cate:
        #                 if i not in viewed_cate:
        #                     next_queue.append(i)
        #         next_queue=list(set(next_queue))
        #         queue_cate=next_queue
        #
        #
        #     print(re_data)
        #     re_data = re_data.drop_duplicates()
        #     print(re_data)
        #     re_data.to_csv('re.csv',sep='\t',encoding='utf8',index=False)
        save_csv = 're.csv'
        df = pd.read_csv(save_csv, sep='\t')
        sub_data=df[df['isleaf']==1]
        sub_data=sub_data[['url','title']]
        print(sub_data)
        sub_data.drop_duplicates()
        print(sub_data)
        new_list=[]
        for cur_url, titles in sub_data.groupby('url'):
            new_list.append([cur_url,titles['title'].values.tolist()[0]])
        print(new_list)
        return new_list

    def get_list_2(self):
        keywords=['杨尚昆','陈锡联', '谭震林', '李维汉', '李德生', '王有才', '张闻天', '李锐', '王铮', '柴玲', '陶铸', '陈毅', '刘晓波',
         '田纪云', '封从德', '习仲勋', '刘刚', '姚依林', '王丹', '王力', '许世友', '李先念', '杨得志', '张春桥', '江平', '陈佩斯',
         '熊焱', '李富春', '周恩来', '周锋锁', '邓林', '张伯笠', '吾尔开希', '崔健', '戴晴', '李录', '北岛', '朱德']
        save_csv = 'event_all.csv'
        if os.path.exists(save_csv):
            df=pd.read_csv(save_csv,sep='\t')
            re_data=None
            for cur_word in keywords:
                data=df[df.title.str.contains(cur_word,na=True)]
                print(data)
                if re_data is None:
                    re_data=data
                else:
                    re_data=pd.concat([re_data,data])
            re_data=re_data.drop_duplicates()
            print(re_data)
        sub_data = re_data[re_data['isleaf'] == 1]
        # save_csv = 're.csv'
        # df = pd.read_csv(save_csv, sep='\t')
        # df=pd.concat([df,sub_data])
        # df.to_csv(save_csv,sep='\t',encoding='utf8',index=False)
        new_list = []
        for cur_url, titles in sub_data.groupby('url'):
            new_list.append([cur_url, titles['title'].values.tolist()[0]])
        print(new_list)
        return new_list

    def get_leaves(self):
        leaves_csv="zhwiki_event_leaves.csv"
        # if os.path.exists(leaves_csv):
        #     df = pd.read_csv(leaves_csv, sep='\t')
        #     urls = df['url'].nunique()
        #     titles = df['title'].nunique()
        #     tot_leaves_list = df.values.tolist()
        #     return tot_leaves_list

        all_csvs_dir='csvs'
        files_list=[]
        for fn in os.listdir(all_csvs_dir):
            files_list.append(os.path.join(all_csvs_dir,fn))

        tot_leaves_df=pd.DataFrame()
        for cur_file in files_list:
            df = pd.read_csv(cur_file, sep='\t')
            sub_data = df[df['isleaf'] == 1].reset_index(drop=True)
            tot_leaves_df = tot_leaves_df.append(sub_data, ignore_index=True)

        tot_leaves_df = tot_leaves_df.drop_duplicates().reset_index(drop=True)
        tot_leaves_df = tot_leaves_df.drop(["pre_url",'isleaf'], axis=1)
        tot_leaves_df = tot_leaves_df.drop_duplicates().reset_index(drop=True)

        tot_leaves_df.to_csv(leaves_csv,sep='\t',index=False)

        urls=tot_leaves_df['url'].nunique()
        titles=tot_leaves_df['title'].nunique()
        tot_leaves_list=tot_leaves_df.values.tolist()
        return tot_leaves_list

    def get_pri_leaves(self):
        leaves_csv="zhwiki_event_pri_leaves.csv"
        if os.path.exists(leaves_csv):
            df = pd.read_csv(leaves_csv, sep='\t')
            urls = df['url'].nunique()
            titles = df['title'].nunique()
            tot_leaves_list = df.values.tolist()
            return tot_leaves_list

        all_csvs_dir='csvs'
        pri_files_name=['一二级链接.csv','政治隐喻.csv','被政府认定为邪教的团体.csv','各恐怖组织成员.csv','各指定者所定恐怖组织.csv','各组织发动的恐怖活动.csv',
                        '邪教题材作品.csv','新疆恐怖主义.csv','中国反叛组织.csv','中国分离主义人物.csv','中国分离主义组织.csv','中国恐怖主义.csv','中国贪污.csv',
                        '中国校园袭击事件.csv','中国伊斯兰教事件.csv','中国右翼政治.csv','中国政变.csv','中国政治案件.csv','中国政治丑闻.csv','中国政治迫害.csv',
                        '中国政治争议.csv','中华民国分离主义.csv','中华人民共和国被禁影视作品.csv','中华人民共和国分离主义.csv',
                        '中华人民共和国恐怖活动.csv','中华人民共和国事故.csv','中华人民共和国水灾.csv','中华人民共和国屠杀事件.csv','中华人民共和国宗教事件.csv',
                        '新兴宗教.csv','中国罢工事件.csv','中国佛教事件.csv','中国各朝代政治事件.csv','中国各省政治事件.csv']
        files_list=[]
        for fn in pri_files_name:
            files_list.append(os.path.join(all_csvs_dir,fn))

        tot_leaves_df=pd.DataFrame()
        for cur_file in files_list:
            df = pd.read_csv(cur_file, sep='\t')
            sub_data = df[df['isleaf'] == 1].reset_index(drop=True)
            tot_leaves_df = tot_leaves_df.append(sub_data, ignore_index=True)

        tot_leaves_df = tot_leaves_df.drop_duplicates().reset_index(drop=True)
        tot_leaves_df = tot_leaves_df.drop(["pre_url",'isleaf'], axis=1)
        tot_leaves_df = tot_leaves_df.drop_duplicates().reset_index(drop=True)

        tot_leaves_df.to_csv(leaves_csv,sep='\t',index=False)

        urls=tot_leaves_df['url'].nunique()
        titles=tot_leaves_df['title'].nunique()
        tot_leaves_list=tot_leaves_df.values.tolist()
        return tot_leaves_list


    def get_leaves_title(self):
        title_file='d:/hwz/code/KG_nodes_0825/title.txt'
        list=[]
        with open(title_file,'r',encoding="utf8") as fi:
            lines=fi.readlines()
            for cur_line in lines:
                if cur_line.strip():
                    list.append(["https://zh.wikipedia.org/wiki/"+quote(cur_line.strip()),cur_line])
        return list



if __name__ == "__main__":
    zhwiki = znWiki('KG_nodes_09_18.log','start_epoch_KG_nodes_09_18.txt','d:/hwz/code/KG_nodes_0825/data/',True,False,leaves_from='KG_nodes_0825')#"local_csvs
    #zhwiki = znWiki('tot_leaves_09_18.log', 'start_epoch_tot_leaves_09_18.txt', 'd:/cjy/wiki/data/', True, False, leaves_from='local_csvs')
