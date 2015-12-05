#coding=utf-8
'''
@since: 2015-6-24
@author: Hearen
@contact: LHearen@126.com
'''
import urllib
import re
import os
import mmap
import time
import datetime
import threading
import multiprocessing
from multiprocessing import Process
from Queue import Queue
from bs4 import BeautifulSoup
from multiprocessing import cpu_count
import subprocess

class PostParser():
    def __init__(self, url):
        self.__url = url # the url of the post - the very beginning page
        
        self.__SubDir = url[7:].split('/thread')[0].replace('.', '-')
        #./data/__SubDir/__PostId.txt used for different clubs
        #used by store and __initStartIndex
        #initialized in __initUrlQueue
        self.__UrlQueue = multiprocessing.Manager().Queue()##store all the urls to be parsed - to be parsed, not all maybe
        # - initialized in __initUrlQueue
        self.__PostId = 0 #the unique id of the post parsed from the url 
        #- initialized in __initUrlQueue
        self.__PageAmount = 1 #the total amount of the pages belonging to the post
        #- initialized in __initUrlQueue
        self.__LastPageIndex = 1 #indexing the last page parsed last time
        #- initialized in __initStartIndex called in __initUrlQueue
        self.__LastFloorIndex = 0 #indexing the last floor parsed last time
        #- initialized in __initStartIndex called in __initUrlQueue
        
        self.__Title = ''#the title and type and the like info will be stored
        #- initialized in __pageParser - only the first page will initialize this value
        self.__PostTime = ''#the post time will be the same with the post time of floor 1
        #- initialized in __pageParser - only the first page will initialize this value
        
        self.__MaxFloorIndex = 1 #the maximum of floor
        #- set in pageParser - dynamically changed via the floor
        
        #this factor can be useless when it comes to updated version
        #self.__BrowserCountString = '' #a number string of browser total amount - 99,232
        #- initialized in __pageParser - only floor 1 will initialize this value
        
        self.__PageDic = {} #store all the page lists
        #each page list stores all the post dics in its page
        #the dics are used to store the info of each post
    
    def __initStartIndex(self):
        """
        should be invoked after the self.__PostId initialized in initUrlQueue
        via the postID to find the file storing the post data
        to get the last page index and floor number parsed already
        """
        filename = './data/%s/%s'%(self.__SubDir, str(self.__PostId) + '.txt')
        try:#handle the unexisting case - not create a file here 
            #- just wait the store method to do it
            f = open(filename, 'r')
            tmpString = ''
            for i in range(4):
                tmpString = f.readline()
            tmpString = tmpString.replace(',', '')
            tmpSS = re.findall(r'\d+', tmpString)
            if len(tmpSS) < 2:#the post wasn't parsed normally
                return
            self.__LastPageIndex = int(tmpSS[0])
            self.__LastFloorIndex = int(tmpSS[1])
            #print(tmpString)
        except Exception as e:
            pass#will not handle the file not found exception, just use the default value

    def __getPageAmount(self, pagesTag):
        '''
        handle different cases where last tag may be absent
        and as a result, you have to manually find the maximum page index
        invoked by __initUrlQueue
        '''
        if pagesTag is None:
            return 1
        lastTag = pagesTag.find('a', class_='last')
        if lastTag is not None:
            pageAmountString = lastTag.string
            return int(re.findall(r'\d+', str(pageAmountString))[0])
        else:
            pageIndex = 0
            for aTag in pagesTag.findAll('a'):
                if str(aTag.string.encode('utf-8')).isdigit() and int(str(aTag.string)) > pageIndex:
                    pageIndex = int(aTag.string)
            return pageIndex

    def __initUrlQueue(self):
        '''
        initialize the url queue for multi-thread parsers to parse in parallel
        and at the same time get the post unique id
        '''
        #try another two times when failed to get the page the first time
        for i in range(3):
            html = urllib.urlopen(self.__url).read().decode('gbk')
            if isinstance(html, int):
                time.sleep(3)
            else:
                break;
        #if at last, still get nothing, return None
        if isinstance(html, int):
            return False
        
        #successfully withdraw the page content
        html = html.encode('utf-8')
        soup = BeautifulSoup(str(html))
        
        #get the total amount of pages of this post
        #=======================================================================
        # pageAmountString = soup.find('div', class_='pages').find('a', class_='last').string
        # self.__PageAmount = int(re.findall(r'\d+', str(pageAmountString))[0])
        # print(self.__PageAmount)
        #=======================================================================
        self.__PostId = int(re.findall(r'\d+', self.__url)[0])
        
        self.__initStartIndex()
        
        self.__PageAmount = self.__getPageAmount(soup.find('div', class_='pages'))
        if self.__PageAmount == 1:
            self.__UrlQueue.put(self.__url)
            return True
        
        #get the index page url format
        pagesHtml = soup.find('div', class_='pages')
        indexPattern = re.compile(r'href="(.+?extra=).*?">')
        indexFormatString = re.findall(indexPattern, str(pagesHtml))[0]
        
        #retrieve the unique post ID from indexFormatString
        
        
        #to form a complete url as follows
        #http://club.eladies.sina.com.cn/viewthread.php?tid=5801720&amp;extra=&page=51
        rooturl = self.__url.split('/thread')[0] + '/'
        for i in range(self.__LastPageIndex, self.__PageAmount + 1):#[)
            pageUrl = rooturl + indexFormatString + '&page=' + str(i)
            #print(pageUrl)
            self.__UrlQueue.put(pageUrl)
        return True


    def __store(self): 
        '''
        using PostId as the unique filename
        store all the data in PageDic 
        - which means this can only called after parsing all the pages
        '''
        filePath = './data/%s'%self.__SubDir
        filename = filePath + '/' + str(self.__PostId) + '.txt'
        if not os.path.exists(filePath):
            os.makedirs(filePath)
        f = None
        try:
            f = open(filename, 'a')#clear the data after opening the file
            if os.path.getsize(filename) == 0:
                headerString = str('ID:%d\n标题：%s\n%s\n页面数:总楼数\t%d:%d\nURL:%s\n'%\
                (self.__PostId, self.__Title, self.__PostTime, self.__PageAmount, \
                 self.__MaxFloorIndex, self.__url))
                f.write(headerString)
            else:#update the LastPageIndex and LastFloorIndex
                with open(filename, "r+b") as f1:
                    mm = mmap.mmap(f1.fileno(), 0)
                    headerString = mm.readline()
                    headerString += mm.readline()
                    headerString += mm.readline()
                    replaceString0 = mm.readline()
                    #mm[len(headerString):len(replaceString0)] = ''
                    replaceString1 = str('页面数:总楼数\t%d:%d\r\n'%\
                    (self.__PageAmount, self.__MaxFloorIndex))
                    mm[len(headerString):len(headerString) + len(replaceString1)] = replaceString1
                    mm.close()
            for pageIndex in range(self.__LastPageIndex, self.__PageAmount + 1):
                dicList = self.__PageDic.get(pageIndex)
                if dicList is not None and len(dicList) > 0:
                    for dic in dicList:
                        f.write('********************************\n')
                        for k, v in dic.iteritems():
                            f.write(k + ':' + str(v) + '\n')
            f.close()
        except Exception as e:
            self.__log("PostParser.__store %s"%e)
        print('PageParser.__store storing post %s successfully'%self.__PostId)


    def __parseContent(self, contentTag):
        """
        used in pageParser
        parse all the related elements in the content including strings and images
        """
        content = ''
        try:
            for child in contentTag.descendants:
                if child.name != 'img':
                    if child.name is None:
                        if '批注：' not in child.encode('utf-8'):
                            content += child.encode('utf-8').strip()
                else:
                    content += '[' + child['src'].encode('utf-8') + ']'
        except Exception as e:
            self.__log('PostParser._parseContent: %s'%e)
            return None
        return content
    
    def __log(self, e):
        filePath = './data/%s'%self.__SubDir
        filename = filePath + '/' + 'log.txt'
        if not os.path.exists(filePath):
            os.makedirs(filePath)
        f = open(filename, 'a')
        f.write('ID: %d Time: %s Error: %s Url: %s\n'%\
                (self.__PostId, datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),\
                  e, self.__url))
        f.close()
    
    def __pageParser(self):
        '''
        using the url stored in UrlQueue to parse the page and store it in PageDic
        '''
        print('Start parsing the page...')
        #thread = threading.current_thread()
        #print(thread.getName())
        #print(Process.name(self))
        #print(self.__UrlQueue.empty())
        while not self.__UrlQueue.empty():#self.__UrlQueue
            url = self.__UrlQueue.get()
            
            if self.__PageAmount != 1:
                indexPattern = re.compile(r'page=(\d+)')
                index = int(re.findall(indexPattern, url)[0])
            else:
                index = 1#the very begining page may be different
                #when there is only one page
            
            #retrieve the html of the given url
            #if the first try failed, try for another two times
            html = None
            for i in range(3):
                html = urllib.urlopen(url).read().decode('gbk').encode('utf-8')
                if isinstance(html, int):
                    time.sleep(3)
                else:
                    break;
                    
            #if all hits failed, just run another url - this may not be a good choice
            #once the one of the page is not withdrawn correctly, 
            #all the rest should be ignored - TODO
            dicList = []
            dic = {}
            soup = BeautifulSoup(str(html))
            
            if index == 1:#the first page will handle extra stuff
                self.__Title = soup.title.string.encode('utf-8')
            floors = soup.findAll('div', class_='mainbox')
            try:
                for floor in floors:
                    floorSoup = BeautifulSoup(str(floor))
                    personName = '被禁言用户'
                    personUrl = '被禁言用户, 空间不可访问！'
                    personID = 0
                    nameTag = floorSoup.find('div', class_='myInfo_up').find('a', class_='f14')
                    if nameTag is not None:
                        personName = floorSoup.find('div', class_='myInfo_up').find('a', class_='f14').string.encode('utf-8') #nickname of the post man
                        personUrl = floorSoup.find('div', class_='myInfo_up').find('a', class_='f14')['href'].encode('utf-8') #get the user url where he or she can be visisted
                        personID = personUrl.split('uid=')[1] #the unique user ID parsed from user url
                    personLevel = floorSoup.find('div', class_='myInfo_up').find('span', class_='authortitle').string.encode('utf-8') #get the level of the user
                    postTime = floorSoup.find('div', class_='myInfo_up').find('font', color='#c5c5c5').string.encode('utf-8') #the time of the post or answer
                    tmpStr = floorSoup.find('div', class_='myInfo_dw').contents[0].encode('utf-8') #发帖 1001    精华：7   注册时间：2007-8-2
                    tmpStrArray = re.findall(r'\d+', tmpStr)
                    postCount = int(tmpStrArray[0].encode('utf-8'))
                    essenceCount = int(tmpStrArray[1].encode('utf-8'))
                    registerTime = '-'.join(tmpStrArray[2:]).encode('utf-8')
                    #get the floor number
                    rowString = ''
                    for s in floorSoup.find('div', class_='myStair').strings:
                        rowString += s.encode('utf-8')
                    rowString = rowString.strip()
                    row = int(re.findall(r'\d+', rowString)[0])
                    
                    #get the content of the floor
                    
                    contentTag = floorSoup.find('div', class_='mybbs_cont')
                    content = self.__parseContent(contentTag)
                    
                    
                    dic['昵称'] = personName
                    dic['空间地址'] = personUrl
                    dic['用户等级'] = personLevel
                    dic['用户ID'] = personID
                    dic['发帖数'] = postCount
                    dic['精华数'] = essenceCount
                    dic['注册时间'] = registerTime
                    dic['内容'] = content
                    dic['发帖时间'] = postTime
                    dic['楼数'] = row
                    if row == 1: #only the first row will contain the following elements
                        #self.__BrowserCountString = str(floorSoup.find('div', class_='mybbs_cont').find('span').findAll('font')[0].string.encode('utf-8')).strip() #get the browser times
                        #this can only be used in the first page of the post - not convenient for further use
                        #self.__MaxFloorIndex = str(floorSoup.find('div', class_='mybbs_cont').find('span').findAll('font')[1].string.encode('utf-8')).strip() #get the answer times
                        self.__PostTime = postTime#the post time of the first floor is also the post time of the post
                    if row > self.__LastFloorIndex:
                        dicList.append(dic)
                    if row > self.__MaxFloorIndex:
                        self.__MaxFloorIndex = row
                    #print('Floor :%d'%row)
                    dic = {}#renew a dictionary
                self.__PageDic[index] = dicList
            except Exception as e:
                self.__log('PostParser.__pageParser %s'%e)
            #print('Done parsing page %d'%index)
            #print(datetime.datetime.now())
        #print('Done parsing the post')

    def parse(self):
        print(os.getpid())
        self.__initUrlQueue()
        self.__pageParser()#11:01:26 - 11:06:14 110 - 5:48 - using just one process
        self.__store()
    
if __name__ == '__main__':
    #url = 'http://club.eladies.sina.com.cn/thread-5801720-1-1.html'
    print(datetime.datetime.now())
    startTime = datetime.datetime.now()
    url = 'http://club.mil.news.sina.com.cn/thread-720300-1-1.html' #0:02:34.348000 one process one thread
    parser = PostParser(url)
    parser.parse()
    print('Done!!!')
    print(datetime.datetime.now())
    endTime = datetime.datetime.now()
    print(endTime - startTime)
          