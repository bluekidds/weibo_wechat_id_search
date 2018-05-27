
# coding: utf-8

import requests
from bs4 import BeautifulSoup
import re
import threading
from queue import Queue
import time
import urllib
import json
import http.cookiejar as HC
import os

KEYWORD_FILE = 'keyword.txt'                # 關鍵字檔案
OUTPUT_FILE = 'output.csv'                  # 輸出檔案
WEIBO_COOKIE_FILE = 'weibo_cookie.txt'      # 微博 Cookie 檔案
WEIBO_COOKIE = {}                           # 微博 Cookie 物件
TIEBA_DOMAIN = 'https://tieba.baidu.com'    # 貼吧 Domain
FETCHER_WORKERS = 5                         # FETCHER 線程數量
FETCHER_QUEUE = Queue()                     # FETCHER 列隊
WEIBO_FETCHER_WORKERS = 1                   # 微博 FETCHER 線程數量
WEIBO_FETCHER_QUEUE = Queue()               # 微博 FETCHER 列隊
WEIBO_DELAY = 10                            # 微博頁面等待時間
PARSER_WORKERS = 20                         # PARSER 線程數量
PARSER_QUEUE = Queue()                      # PARSER 列隊
KEYWORD = set()                             # 關鍵字列表
REPORT_INTERVAL = 5                         # [LOG] 列隊數量顯示間隔(s)
LOG_FETCHER_REMAINING = True                # [LOG] FETCHER 列隊數量顯示
LOG_PARSER_REMAINING = True                 # [LOG] PARSER 列隊數量顯示
LOG_ARTICLE_ADDED = False                   # [LOG] 文章加入顯示
LOG_FETCHER_START = True                    # [LOG] FETCHER 開始執行顯示
LOG_PARSER_START = False                    # [LOG] PARSER 開始執行顯示
LOG_FETCHER_END = True                      # [LOG] FETCHER 結束執行顯示
LOG_PARSER_END = True                       # [LOG] PARSER 結束執行顯示
LOG_FETCHER_PAGE = True                     # [LOG] FETCHER 單一關鍵字頁數顯示
WORD_LIST_PATH = 'words/'                   # 常見字路徑
WORD_LIST = [file for root, dirs, files in os.walk(WORD_LIST_PATH) for file in files if file.endswith('txt')] # 常見單字過濾列表

# 讀入關鍵字列表至 KEYWORD
input_file = open(KEYWORD_FILE, 'r')
for line in input_file:
    KEYWORD.add(line.strip())
    
# 讀入過濾字串
WORDS_FILTER = set()
for filename in WORD_LIST:
    print('[FILETR] Loading Dictionary: ' + filename)
    try:
        filename = os.path.join(os.getcwd(), WORD_LIST_PATH + filename)
        WORDS_FILTER |= set(line.strip().lower() for line in open(filename))
    except Exception as e:
        print(str(e))
        pass

# 讀入微博 Cookie
f = open(WEIBO_COOKIE_FILE,'r')
for line in f.read().split(';'):
    name,value=line.strip().split('=',1)
    WEIBO_COOKIE[name] = value

# 抓取單頁貼吧搜尋結果，回傳文章 Div 列表
def fetch_search_result(keyword='關鍵字', pn=1):
    r = requests.get('https://tieba.baidu.com/f/search/res?ie=utf-8&qw=' + str(keyword) + '&pn=' + str(pn))
    if r.status_code == 200:
        soup = BeautifulSoup(r.text, 'html.parser')
        divs = soup.find_all('div', 's_post')
        result = []
        for d in divs:
            if d['class'] == ['s_post']:
                result.append(d)
        return result
    else:
        return []

# 抓取單頁貼吧搜尋結果，回傳貼吧文章物件列表
def parse_search_result(keyword='關鍵字', pn=1):
    r = fetch_search_result(keyword, pn)
    result = []
    for p in r:
        data = {}
        try:
            if not p.find('a', 'bluelink')['href'].startswith('http'):
                data['type'] = 'Tieba'
                data['url'] = TIEBA_DOMAIN + p.find('a', 'bluelink')['href'].split('?',maxsplit=1)[0]
                data['title'] = p.find('a', 'bluelink').text
                data['summary'] = p.find('div', 'p_content').text.strip()
                result.append(data)
        except:
            print('[PARSER][Search][' + keyword + '] Unknown Error')
    return result

# 抓取單頁貼吧結果，回傳文章 Div 列表
def fetch_teiba_result(keyword='貼吧名稱', pn=0):
    r = requests.get('https://tieba.baidu.com/f?ie=utf-8&kw=' + str(keyword) + '&pn=' + str(pn))
    if r.status_code == 200:
        soup = BeautifulSoup(r.text, 'html.parser')
        return soup.find_all('li', ' j_thread_list clearfix')
    else:
        return []

# 抓取單頁貼吧結果，回傳貼吧文章物件列表
def parse_teiba_result(keyword='貼吧名稱', pn=0):
    r = fetch_teiba_result(keyword, pn)
    result = []
    for p in r:
        data = {}
        try:
            if not p.find('a', 'j_th_tit')['href'].startswith('http'):
                data['type'] = 'Tieba'
                data['url'] = TIEBA_DOMAIN + p.find('a', 'j_th_tit')['href'].split('?',maxsplit=1)[0]
                data['title'] = p.find('a', 'j_th_tit').text
                data['summary'] = p.find('div', 'threadlist_abs threadlist_abs_onlyline ').text.strip()
                result.append(data)
        except:
            print('[PARSER][Tieba][' + keyword + '] Unknown Error')
    return result

# 傳入微博 Script Tag 回傳真實 HTML 結果
def weibo_real_html(content='({})'):
    json_re = r'.*\(({.*})\)'
    matches = re.match(json_re, content)
    json_content = matches.group(1)
    json_obj = json.loads(json_content)
    soup = BeautifulSoup(json_obj['html'], 'html.parser')
    return soup

# 抓取微博搜尋結果，回傳文章Div列表
def fetch_weibo_result(keyword='關鍵字', pn=1):
    r = requests.get('http://s.weibo.com/weibo/' + urllib.parse.quote(urllib.parse.quote(keyword)) + '&page=' + str(pn), cookies=WEIBO_COOKIE)
    if "$CONFIG['islogin'] = '0';" in r.text:
        print('need login')
        return 'need login'
    elif '"pid":"pl_common_sassfilter"' in r.text:
        print('verification code')
        return 'verification code'
    else:
        soup = BeautifulSoup(r.text, 'html.parser')
        scripts = soup.find_all('script')
        for script in scripts:
            if '{"pid":"pl_weibo_direct"' in script.text:
                soup = weibo_real_html(script.text)
                return soup.find_all('div', 'WB_cardwrap S_bg2 clearfix')

# 抓取微博搜尋結果，回傳文章物件列表
def parse_weibo_result(keyword='關鍵字', pn=1):
    r = fetch_weibo_result(keyword, pn)
    result = []
    for p in r:
        data = {}
        try:
            url = p.find('ul', 'feed_action_info feed_action_row4').find_all('li')[1].find('a')['action-data']
            url_re = r'.*url=(\/\/.*?)&.*'
            matches = re.match(url_re, url)
            url = 'http:' + matches.group(1)
            
            summary = p.find('div', 'content clearfix').find('p').text
            summary = re.sub(r'^\s+', '', summary)
            summary = re.sub(r'\s+...展开全文c$', '', summary)
            summary = re.sub(r'\s+\u200b', '', summary)
            
            data['type'] = 'Weibo'
            data['url'] =  url
            data['title'] = 'null'
            data['summary'] = summary.strip()
            result.append(data)
        except:
            print('[PARSER][Weibo][' + keyword + '] Unknown Error')
    return result

# 取得該關鍵字搜尋結果所有的文章並加入 PARSER 列隊中
def all_search_result(keyword='關鍵字'):
    r = requests.get('https://tieba.baidu.com/f/search/res?ie=utf-8&qw=' + str(keyword))
    if r.status_code == 200:
        if '抱歉，没有找到与' in r.text:
            print('[FETCHER][Search][' + keyword + '] No result')
            return 'no result'
        
        soup = BeautifulSoup(r.text, 'html.parser')
        pagelink = soup.find_all('a', 'last')
        page_re = r'.*pn=(\d+)'
        try:
            matches = re.match(page_re, pagelink[-1]['href'])
            total_pn = int(matches.group(1))
        except:
            total_pn = 0
        
        for i in range(1, total_pn + 1):
            if LOG_FETCHER_PAGE:
                print('[FETCHER][Search][' + keyword + '] Fetching pn = ' + str(i) + ', total_pn = ' + str(total_pn))
            for article in parse_search_result(keyword, i):
                article['keyword'] = keyword
                PARSER_QUEUE.put(article)
                if LOG_ARTICLE_ADDED:
                    print('[PARSER_QUEUE][' + keyword + '] Job Added From Baidu Search where pn = ' + str(i) + ' : ' + article['url'])

# 取得該關鍵字貼吧所有的文章並加入 PARSER 列隊中
def all_teiba_result(keyword='貼吧名稱'):
    r = requests.get('https://tieba.baidu.com/f?ie=utf-8&kw=' + str(keyword))
    if r.status_code == 200:
        if '抱歉，根据相关法律法规和政策，本吧暂不开放。' in r.text:
            print('[FETCHER][Tieba][' + keyword + '] Law related')
            return 'illegal'
        soup = BeautifulSoup(r.text, 'html.parser')
        pagelink = soup.find_all('a', 'last')
        page_re = r'.*pn=(\d+)'
        try:
            matches = re.match(page_re, pagelink[-1]['href'])
            total_pn = int(matches.group(1))
        except:
            total_pn = 0
        
        for i in range(0, total_pn + 100, 50):
            if LOG_FETCHER_PAGE:
                print('[FETCHER][Tieba][' + keyword + '] Fetching pn = ' + str(i) + ', total_pn = ' + str(total_pn))
            try:
                for article in parse_teiba_result(keyword, i):
                    article['keyword'] = keyword
                    PARSER_QUEUE.put(article)
                    if LOG_ARTICLE_ADDED:
                        print('[PARSER_QUEUE][' + keyword + '] Job Added From Teiba Directly where pn = ' + str(i) + ' : ' + article['url'])
            except:
                pass

# 取得該關鍵字微博搜尋結果中所有文章並加入 PARSER 列隊中
def all_weibo_result(keyword='關鍵字'):
    r = requests.get('http://s.weibo.com/weibo/' + urllib.parse.quote(urllib.parse.quote(keyword)), cookies = WEIBO_COOKIE)
    if r.status_code == 200:
        if "$CONFIG['islogin'] = '0';" in r.text:
            print('need login')
            return 'need login'
        elif '"pid":"pl_common_sassfilter"' in r.text:
            print('verification code')
            return 'verification code'
        else:
            print('login no need')
            try:
                soup = BeautifulSoup(r.text, 'html.parser')
                scripts = soup.find_all('script')
                for script in scripts:
                    if '{"pid":"pl_weibo_direct"' in script.text:
                        soup = weibo_real_html(script.text)
                        pagelink = soup.find('div', 'layer_menu_list W_scroll').find_all('a')
                        page_re = r'.*&page=(\d+)'
                        try:
                            matches = re.match(page_re, pagelink[-1]['href'])
                            total_pn = int(matches.group(1))
                        except:
                            total_pn = 0
        
                for i in range(1, total_pn + 1):
                    if LOG_FETCHER_PAGE:
                        print('[FETCHER][Weibo][' + keyword + '] Fetching pn = ' + str(i) + ', total_pn = ' + str(total_pn))
                    try:
                        for article in parse_weibo_result(keyword, i):
                            article['keyword'] = keyword
                            PARSER_QUEUE.put(article)
                            if LOG_ARTICLE_ADDED:
                                print('[PARSER_QUEUE][' + keyword + '] Job Added From Baidu Search where pn = ' + str(i) + ' : ' + article['url'])
                    except:
                        pass
                    time.sleep(WEIBO_DELAY)
            except:
                pass

def check_weibo_cookies():
    return False
    r = requests.get('http://s.weibo.com/weibo/' + urllib.parse.quote(urllib.parse.quote('Cookie測試')), cookies = WEIBO_COOKIE)
    if r.status_code == 200:
        if "$CONFIG['islogin'] = '0';" in r.text:
            print('need login, not enable weibo')
            return False
        elif '"pid":"pl_common_sassfilter"' in r.text:
            print('verification code, not enable weibo')
            return False
        #elif 'location.replace' in r.text:
        #    print('refresh taken')
        #    soup = BeautifulSoup(r.text, 'html.parser')
        #    scripts = soup.find_all('script')
        #    for script in scripts:
        #        if 'location.replace' in script.text:
        #            url_re = r'.*location\.replace\(\"(.*)\"\).*'
        #            try:
        #                matches = re.match(url_re, script.text.strip())
        #                url = matches.group(1)
        #                print(url)
        #                r = requests.get(url, cookies = WEIBO_COOKIE)
        #                return True
        #            except:
        #                return False
        else:
            return True

# 可能的微信帳號過濾器，傳入文章內容，傳出帳號列表
def possible_wechat_filter(content='', pre_out=set()):
    #print(content)
    improve_re = r'((群|wechat|[微VvＶ][信XxＸ]?)[：:]?\ ?.{6,20})'
    phone_re = r'((13[0-9])|(14[5|7])|(15([0-3]|[5-9]))|(18[0,5-9]))\d{8}'
    wechat_re = r'[a-zA-Z]{1}[-_a-zA-Z0-9]{5,19}'
    wechat_exclude_phone_re = r'^[微VvＶ][信XxＸ]?[：:]?\ ?\d{11}$'
    #qq_re = r'[1-9]\d{9,11}'
    output = pre_out

    try:
        matches = re.finditer(phone_re, content)
        for num, match in enumerate(matches):
            output.add(match.group().lower())

        improve_matches = re.finditer(improve_re, content)
        for improve_num, improve_match in enumerate(improve_matches):
            improved_content = improve_match.group()

            matches = re.finditer(wechat_re, improved_content)
            for num, match in enumerate(matches):
                if not wechat_exclude_phone_re.match(match.group().lower()):
                    output.add(match.group().lower())

            #matches = re.finditer(qq_re, improved_content)
            #for num, match in enumerate(matches):
            #    output.add(match.group().lower())

            output -= WORDS_FILTER    
            for account in output:
                if len(set(account)) == 1:
                    output.remove(account)
    except:
        pass

    return output

# 貼吧文章讀取器，傳入文章網址，自動讀入該文章多頁內容，回傳帳號列表
def tieba_article_parser(url):
    assert url != ''
    r = requests.get(url)
    if r.status_code == 200:
        soup = BeautifulSoup(r.text, 'html.parser')
        pagelink = soup.find('li', 'l_pager pager_theme_4 pb_list_pager').find_all('a')
        if pagelink != list():
            page_re = r'.*pn=(\d+)'
            matches = re.match(page_re, pagelink[-1]['href'])
            total_page = int(matches.group(1))
        else:
            total_page = 1

        accounts = set()
        for page in range(1, total_page + 1):
            if page != 1:
                r = requests.get(url + '?pn=' + str(page))
                soup = BeautifulSoup(r.text, 'html.parser')
            for i in soup.find_all('div', id=re.compile("^post_content")):
                accounts = possible_wechat_filter(i.text, accounts)
        return list(accounts)
    else:
        return []

def weibo_article_parser(url):
    assert url != ''
    r = requests.get(url, cookies=WEIBO_COOKIE)
    if r.status_code == 200:
        soup = BeautifulSoup(r.text, 'html.parser')
        scripts = soup.find_all('script')
        for script in scripts:
            if '{"ns":"pl.content.weiboDetail.index"' in script.text:
                soup = weibo_real_html(script.text)
                content = soup.find('div', 'WB_text W_f14').text
        accounts = set()
        accounts = possible_wechat_filter(content, accounts)
        return list(accounts)
    else:
        return []

# CSV 逗號及雙引號處理器
def csvhandlerstr(str):
    if ',' in str:
        if '"' in str:
            str = str.replace('"','""')
        str = '"' + str + '"'
    return str

# 輸出格式
def output_format(article={'url': 'http://tieba.baidu.com/p/id', 'title': '標題', 'summary': '摘要', 'keyword': '關鍵字'}, accounts=[]):
    for account in accounts:
        with open(OUTPUT_FILE, 'a') as f:
            f.write(csvhandlerstr(account) + ', ' + csvhandlerstr(article['keyword']) + ', ' + csvhandlerstr(article['url']) + ', ' + csvhandlerstr(article['title']) + ', ' + csvhandlerstr(article['summary']) + "\n")

# 帳號列表抓取worker
def parser():
    while True:
        if not PARSER_QUEUE.empty():
            article = PARSER_QUEUE.get()
            if LOG_PARSER_START:
                print('[PARSER_WORKER][' + article['type'] + '][' + article['keyword']  + '] Job Start: url = ' + article['url'])
            try:
                if article['type'] == 'Tieba':
                    accounts = tieba_article_parser(article['url'])
                elif article['type'] == 'Weibo':
                    accounts = weibo_article_parser(article['url'])
                else:
                    print('[PARSER_WORKER][' + article['type'] + '][' + article['keyword']  + '] Job Error: Unknown type where url = ' + article['url'])
                output_format(article, accounts)
            except:
                pass
            if LOG_PARSER_END:
                print('[PARSER_WORKER][' + article['type'] + '][' + article['keyword']  + '] Job End: url = ' + article['url'] + ', accnum = ' + str(len(accounts)))
            PARSER_QUEUE.task_done()

# 啟動上面的
for w in range(PARSER_WORKERS):
    t = threading.Thread(target=parser, name='parser-%s' % w)
    t.daemon = True
    t.start()

# 文章列表抓取worker
def fetcher():
    while True:
        if not FETCHER_QUEUE.empty():
            req = FETCHER_QUEUE.get()
            if LOG_FETCHER_START:
                print('[FETCHER_WORKER][' + req['type'] + '][' + req['keyword']  + '] Job Start')
            try:
                if req['type'] == 'Search':
                    all_search_result(req['keyword'])
                elif req['type'] == 'Tieba':
                    all_teiba_result(req['keyword'])
            except:
                pass
            if LOG_FETCHER_END:
                print('[FETCHER_WORKER][' + req['type'] + '][' + req['keyword']  + '] Job End')
            FETCHER_QUEUE.task_done()

# 啟動上面的
for w in range(FETCHER_WORKERS):
    t = threading.Thread(target=fetcher, name='fetcher-%s' % w)
    t.daemon = True
    t.start()

# 微博文章列表抓取worker
def weibo_fetcher():
    while True:
        if not WEIBO_FETCHER_QUEUE.empty():
            req = WEIBO_FETCHER_QUEUE.get()
            if LOG_FETCHER_START:
                print('[FETCHER_WORKER][' + req['type'] + '][' + req['keyword']  + '] Job Start')
            if req['type'] == 'Weibo':
                all_weibo_result(req['keyword'])
            if LOG_FETCHER_END:
                print('[FETCHER_WORKER][' + req['type'] + '][' + req['keyword']  + '] Job End')
            WEIBO_FETCHER_QUEUE.task_done()

# 啟動上面的，啟動前先檢查cookie過期沒
if check_weibo_cookies():
    for w in range(WEIBO_FETCHER_WORKERS):
        t = threading.Thread(target=weibo_fetcher, name='weibo-fetcher-%s' % w)
        t.daemon = True
        t.start()

# 讀入關鍵字並放入 FETCHER 的列隊裡
for key in KEYWORD:
    req = {}
    req['type'] = 'Search'
    req['keyword'] = key
    FETCHER_QUEUE.put(req)

    req = {}
    req['type'] = 'Tieba'
    req['keyword'] = key
    FETCHER_QUEUE.put(req)

    req = {}
    req['type'] = 'Weibo'
    req['keyword'] = key
    WEIBO_FETCHER_QUEUE.put(req)

# 定時回報列隊剩餘數量
while True:
    time.sleep(REPORT_INTERVAL)
    if LOG_FETCHER_REMAINING:
        print('[FETCHER_QUEUE] Job remaining: ' + str(FETCHER_QUEUE.qsize()))
    if LOG_PARSER_REMAINING:
        print('[PARSER_QUEUE] Job remaining: ' + str(PARSER_QUEUE.qsize()))

