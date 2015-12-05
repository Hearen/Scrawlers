#coding=utf-8
'''
Author: LHearen
E-mail: LHearen@126.com
Time  :	2015-12-05 10:47
Description: Used to collect and parse the posts;
'''
import urllib
import sys
import re
import os
import mmap
import time
import datetime
import spynner
import pyquery
import threading
import multiprocessing
from multiprocessing import Process
from Queue import Queue
from bs4 import BeautifulSoup
from multiprocessing import cpu_count
import subprocess


class PostParser():
    def __init__(self, clubUrl, url):
        self.__url = url # the url of the post - the very beginning page

        self.__SubDir = clubUrl[7:].replace('.', '-').replace('/', '-')
        #./data/__SubDir/__PostId.txt used for different clubs
        #used by store and __initStartIndex
        #initialized in __initUrlQueue
        self.__UrlQueue = multiprocessing.Manager().Queue()##store all the urls to be parsed - to be parsed, not all maybe
        # - initialized in __initUrlQueue
        self.__PostId = '' #the unique id of the post parsed from the url
        #- initialized in __initUrlQueue
        self.__PageAmount = 1 #the total amount of the pages belonging to the post
        #- initialized in __initUrlQueue
        self.__LastPageIndex = 1 #indexing the last page parsed last time
        #- initialized in __initStartIndex called in __initUrlQueue
        self.__LastFloorIndex = -1 #indexing the last floor parsed last time
        #- initialized in __initStartIndex called in __initUrlQueue
        #the first floor in the first page will be labelled as 0 so the __LastFloorIndex = -1 as an original value
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
        and at the same time get the post unique id and pageAmount
        '''
        #try another two times when failed to get the page the first time
        for i in range(3):
            browser = spynner.Browser()
            browser.create_webview()
            browser.set_html_parser(pyquery.PyQuery)
            browser.load(self.__url, 10)

            try:
                browser.wait_load(10)
            except Exception as e:
                pass

            html = browser.html.encode('utf-8')
            browser.close()
            if isinstance(html, int):
                time.sleep(3)
            else:
                break;
        #if at last, still get nothing, return None
        if isinstance(html, int):
            return False

        soup = BeautifulSoup(str(html))

        #retrieve the unique post ID from url
        self.__PostId = self.__url.split('/thread/')[1]

        self.__initStartIndex()
        pageTag = soup.find('div', id='toppage_thread').find('div', class_='pages')
        if pageTag is None:
            self.__PageAmount = 1
        else:
            aTags = pageTag.findAll('a')
            for aTag in aTags:
                pageString = aTag.string.encode('utf-8').strip()
                if pageString.isdigit() and int(pageString) > self.__PageAmount:
                    self.__PageAmount = int(pageString)
        for i in range(1, self.__PageAmount + 1):
            self.__UrlQueue.put(self.__url + '/p' + str(i))
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
                headerString = str('ID:%s\n标题：%s\n%s\n页面数:总楼数\t%d:%d\nURL:%s\n'%\
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
                        for k, v in dic.iteritems():#one post including comments
                            f.write(k + ':' + str(v) + '\n')
            f.close()
        except Exception as e:
            self.__log("PostParser.__store %s"%e)
        #print('PageParser.__store storing post %s successfully'%self.__PostId)


    def __parseContent(self, contentTag):
        """
        used in pageParser
        parse all the related elements in the content including strings and images
        """
        content = ''
        if contentTag is None:
            return content
        try:
            for child in contentTag.descendants:
                if child.name != 'img':
                    if child.name is None:
                        if '回复：' in child.encode('utf-8'):
                            content += '\r\n回复：' + child.encode('utf-8').strip()
                        else:
                            content += child.encode('utf-8').strip()
                else:
                    if 'src="' in child.encode('utf-8'):
                        content += '[http://bbs.iyaxin.com/' + child['src'].encode('utf-8') + ']'
                    elif 'file="' in child.encode('utf-8'):
                        content += '[http://bbs.iyaxin.com/' + child['file'].encode('utf-8') + ']'
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
        f.write('ID: %s Time: %s Error: %s Url: %s\n'%\
                (self.__PostId, datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),\
                  e, self.__url))
        f.close()

    def __getActualAmount(self, countString):
        '''
        get the number instead of number with special units like ��
        '''
        count = 0
        if '��' not in countString:
            count = int(countString)
        else:
            count = int(re.findall(r'\d+', countString)[0]) * 10000
        return count

    def __pageParser(self):
        '''
        using the url stored in UrlQueue to parse the page and store it in PageDic
        '''
        #print('Start parsing the page:%s at: %s'%(self.__url, str(datetime.datetime.now())))

        while not self.__UrlQueue.empty():#self.__UrlQueue
            url = self.__UrlQueue.get()

            index = int(re.findall(r'/p(\d+)', url)[0])

            #retrieve the html of the given url
            #if the first try failed, try for another two times
            browser = spynner.Browser()
            browser.create_webview()
            browser.set_html_parser(pyquery.PyQuery)
            browser.load(url, 10)

            try:
                browser.wait_load(10)
            except Exception as e:
                pass

            html = browser.html.encode('utf-8')
            browser.close()
            dicList = []
            dic = {}
            soup = BeautifulSoup(str(html))

            if index == 1:#the first page will handle extra stuff
                self.__Title = soup.title.string.encode('utf-8')
            floors = soup.findAll('table', class_='viewpost')
            row = -1
            try:
                for floor in floors:
                    floorSoup = BeautifulSoup(str(floor))
                    personName = floorSoup.find('a', class_='username').string.encode('utf-8')
                    personUrl = floorSoup.find('a', class_='username')['href']
                    personHeadPortrait = floorSoup.find('img', class_='userhead')['src']
                    userInfoTag = floorSoup.find('ul', class_='userinfomation')
                    tmpTag = userInfoTag.findAll('li')[0]
                    tmpString = self.__parseContent(tmpTag)
                    tmpSS = tmpString.split(':')
                    personGender = tmpSS[1].split('\n')[0].strip()
                    personPosition = ''
                    if '\n' in tmpSS[2]:
                        personPosition = tmpSS[2].split('\n')[0].strip()
                    else:
                        personPosition = tmpTag.find('span').string.encode('utf-8')
                    registerTime = tmpSS[3]
                    tmpTag = userInfoTag.findAll('li')[1]
                    countString = self.__parseContent(tmpTag)
                    countSS = re.findall(r'\d+', countString)
                    personLevelString = floorSoup.find('span', class_='level').find('i')['title'].encode('utf-8')
                    personLevel = re.findall(r'\d+', personLevelString)[0]
                    postCount = int(countSS[0].strip())
                    essenceCount = int(countSS[1].strip())
                    reputationCount = int(countSS[2].strip())
                    creditCount = int(countSS[3].strip())
                    postTimeTag = floorSoup.find('div', class_='grey')
                    postTimeString = self.__parseContent(postTimeTag)
                    postTime = re.findall(r'\d+-\d+-\d+\s*\d+:\d+:\d+', postTimeString)[0]
                    contentTag = floorSoup.find('div', class_='wrap')
                    content = self.__parseContent(contentTag)
                    rowTag = floorSoup.find('p', class_='louc')
                    rowString = self.__parseContent(rowTag)
                    userType = 'PC用户'
                    if '[' in rowString:
                        userType = '移动用户'
                        rowString = rowString.split(']')[1]
                    if len(re.findall(r'\d+', rowString)) > 0:
                        rowString = re.findall(r'\d+', rowString)[0]
                        row = int(rowString)
                    else:
                        row += 1#increase by 1 when the row is not in ordinal form

                    #get the content of the floor


                    dic['楼数'] = row
                    dic['用户名'] = personName
                    dic['用户类型'] = userType
                    dic['空间地址'] = personUrl
                    dic['用户头像'] = personHeadPortrait
                    dic['等级'] = personLevel
                    dic['性别'] = personGender
                    dic['所在地'] = personPosition
                    dic['注册时间'] = registerTime
                    dic['发帖数'] = postCount
                    dic['精华数'] = essenceCount
                    dic['积分'] = creditCount
                    dic['声望'] = reputationCount
                    dic['内容'] = content
                    dic['发帖时间'] = postTime

                    if row == 0:
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
        #print(os.getpid())
        self.__initUrlQueue()
        self.__pageParser()#11:01:26 - 11:06:14 110 - 5:48 - using just one process
        self.__store()
        print('Done parsing page: %s'%url)

if __name__ == '__main__':
    #url = 'http://club.eladies.sina.com.cn/thread-5801720-1-1.html'
    #startTime = datetime.datetime.now()
    clubUrl = 'http://women.club.sohu.com/zz482/threads'
    url = 'http://women.club.sohu.com/zz482/thread/39uzzrkqos1'
    parser = PostParser(clubUrl, url)
    parser.parse()
    print('Done parsing page: %s!!!'%url)
    #print(datetime.datetime.now())
    #endTime = datetime.datetime.now()
    #print(endTime - startTime)
