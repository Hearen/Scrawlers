#coding=utf-8
'''
Author: LHearen
E-mail: LHearen@126.com
Time  :	2015-12-05 10:47
Description: Used to collect and parse the posts;
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
        sumString = pagesTag.find('label').find('span').string.encode('utf-8')
        intString = re.findall(r'\d+', str(sumString))[0]
        return int(intString)

    def __initUrlQueue(self):
        '''
        initialize the url queue for multi-thread parsers to parse in parallel
        and at the same time get the post unique id
        '''
        #try another two times when failed to get the page the first time
        for i in range(3):
            html = urllib.urlopen(self.__url).read().decode('gbk', 'ignore').encode('utf-8')
            if isinstance(html, int):
                time.sleep(3)
            else:
                break;
        #if at last, still get nothing, return None
        if isinstance(html, int):
            return False

        soup = BeautifulSoup(str(html))

        #get the total amount of pages of this post
        #=======================================================================
        # pageAmountString = soup.find('div', class_='pages').find('a', class_='last').string
        # self.__PageAmount = int(re.findall(r'\d+', str(pageAmountString))[0])
        # print(self.__PageAmount)
        #=======================================================================
        self.__PostId = int(re.findall(r'-(\d+)-1-\d+', self.__url)[0])
        #-PostId-postPageIndex-clubPageIndex
        #we get the postId in the first page of a post

        self.__initStartIndex()

        self.__PageAmount = self.__getPageAmount(soup.find('div', class_='pg'))


        #to form a complete url as follows
        #http://www.mala.cn/thread-12118779-1-1.html
        #http://www.mala.cn/thread-12118779-2-1.html
        urlSS = self.__url.split('.html')[0].split('-')
        baseUrl = urlSS[0] + '-' + urlSS[1]
        for i in range(self.__LastPageIndex, self.__PageAmount + 1):#[)
            pageUrl = baseUrl + '-' + str(i) + '-' + urlSS[3] + '.html'
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
                headerString = str('ID:%d\n标题：%s\n发帖时间：%s\n页面数:总楼数\t%d:%d\nURL:%s\n'%\
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
                if dicList is not None and len(dicList) > 0:#post list in a page
                    for dic in dicList:
                        f.write('********************************\n')
                        for k, v in dic.iteritems():#one post including comments
                            if k != '评论':
                                f.write(k + ':' + str(v) + '\n')
                            else:#comments
                                f.write('评论:\n')
                                for commentDic in v:#dicList
                                    f.write('\t--------------------\n')
                                    for k1, v1 in commentDic.iteritems():#comment dic
                                        f.write('\t' + k1 + ':' + str(v1) + '\n')
            f.close()
        except Exception as e:
            self.__log("PostParser.__store %s"%e)
        #print('Storing post %s successfully'%self.__PostId)


    def __parseContent(self, contentTag):
        """
        used in pageParser
        parse all the related elements in the content including strings and images
        """
        content = ''
        if contentTag is None:
            return None
        try:
            for child in contentTag.descendants:
                if child.name != 'img':
                    if child.name is None:
                        content += child.encode('utf-8').strip()
                elif 'zoomfile="http' in str(child):#str(child).find('zoomfile') != -1
                    content += '[' + child['zoomfile'].encode('utf-8') + ']'
                elif 'file="http:' in str(child):
                    content += '[' + child['file'].encode('utf-8') + ']'
                elif 'src="http' in str(child):
                    content += '[http://www.mala.cn/' + child['src'].encode('utf-8') + ']'
        except Exception as e:
            self.__log('PostParser._parseContent'%e)
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
        #print('Start parsing the post ...')
        #thread = threading.current_thread()
        #print(thread.getName())
        #print(Process.name(self))
        #print(self.__UrlQueue.empty())
        while not self.__UrlQueue.empty():#self.__UrlQueue
            url = self.__UrlQueue.get()
            index = int(re.findall(r'-\d+-(\d+)-\d+', url)[0])

            #retrieve the html of the given url
            #if the first try failed, try for another two times
            html = None
            for i in range(3):
                html = urllib.urlopen(url).read().decode('gbk', 'ignore').encode('utf-8')
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
            floors = soup.findAll('div', attrs={'id':re.compile(r"post_\d+")})
            try:
                for floor in floors:
                    floorSoup = BeautifulSoup(str(floor))
                    personName = ''
                    personNameTag = floorSoup.find('a', class_='xw1')
                    if personNameTag is not None:
                        personName = personNameTag.string.encode('utf-8') #nickname of the post man
                    elif None is not floorSoup.find('div', class_='pls favatar').find('a', href='javascript:;'):
                        personName = floorSoup.find('div', class_='pls favatar').find('a', href='javascript:;').find('em').string.encode('utf-8')

                    genderDivTag = floorSoup.find('div', class_='pi').find('img')
                    personGender = '性别保密哦^_^'
                    if genderDivTag is not None:
                        personGender = genderDivTag['title'].encode('utf-8')

                    personUrlTag = floorSoup.find('a', class_='xw1')
                    personUrl = ''
                    personID = 0
                    if personUrlTag is not None:
                        personUrl = personUrlTag['href'].encode('utf-8') #get the user url where he or she can be visisted
                        personID = int(personUrl.split('uid=')[1]) #the unique user ID parsed from user url
                    postTimeString = floorSoup.find('em', attrs={'id':re.compile(r'authorposton\d+')}).string.encode('utf-8') #the time of the post or answer
                    postTime = re.findall(r'\d+-\d+-\d+ \d+:\d+', postTimeString)[0]
                    #get the floor number
                    rowString = floorSoup.findAll('td', class_='plc')[0].find('em').string.encode('utf-8')
                    row = 1
                    if rowString.isdigit():
                        row = int(rowString)

                    #get the content of the floor

                    contentTag = floorSoup.find('td', attrs={'id':re.compile(r'postmessage_\d+')})
                    content = self.__parseContent(contentTag)
                    if content is None:
                        content = ''

                    #Unsolved div class='cm' just disappeared sometimes
                    commentTags0 = floorSoup.find('div', class_='cm')#.findAll('div', class_='pstl xs1 cl')
                    commentList = []
                    if commentTags0 is not None:
                        commentTags = floorSoup.find('div', class_='cm').findAll('div', class_='pstl xs1 cl')
                        if len(commentTags):
                            for commentTag in commentTags:
                                commentDic = {}
                                cPersonName = ''
                                cPersonTag = commentTag.find('div', class_='psta vm').find('a', class_='xi2 xw1')
                                cPersonUrl = ''
                                cPersonId = 0
                                if cPersonTag is not None:
                                    cPersonName = cPersonTag.string.encode('utf-8').replace('\n', '')
                                    cPersonUrl = cPersonTag['href'].encode('utf-8')
                                    cPersonId = int(cPersonUrl.split('uid=')[1].strip())
                                else:
                                    cNameTmpTag = commentTag.find('div', class_='psta vm')
                                    for s in cNameTmpTag.strings:
                                        cPersonName += s.encode('utf-8').replace('\n', '')
                                    cPersonName.strip()
                                commentContent = ''
                                commentTime = ''
                                for s in commentTag.find('div', class_='psti').strings:
                                    s = s.encode('utf-8')
                                    if re.findall(r'\d+-\d+-\d+\s\d+:\d+', s):
                                        commentTime = re.findall(r'\d+-\d+-\d+\s\d+:\d+', s)[0]
                                        continue
                                    if re.match('回复', s):
                                        continue
                                    commentContent += s.replace('\n', '')
                                commentContent = commentContent.strip()

                                commentDic['用户名'] = cPersonName
                                commentDic['空间地址'] = cPersonUrl
                                commentDic['用户ID'] = cPersonId
                                commentDic['回复时间'] = commentTime
                                commentDic['内容'] = commentContent
                                commentList.append(commentDic)
                                #print()


                    dic['用户性别'] = personGender
                    dic['昵称'] = personName
                    dic['空间地址'] = personUrl
                    dic['用户ID'] = personID
                    dic['内容'] = content
                    dic['发帖时间'] = postTime
                    dic['楼数'] = row
                    if len(commentList):
                        dic['评论'] = commentList
                    if row == 1: #only the first row will contain the following elements
                        #self.__BrowserCountString = str(floorSoup.find('div', class_='mybbs_cont').find('span').findAll('font')[0].string.encode('utf-8')).strip() #get the browser times
                        #this can only be used in the first page of the post - not convenient for further use
                        #self.__MaxFloorIndex = str(floorSoup.find('div', class_='mybbs_cont').find('span').findAll('font')[1].string.encode('utf-8')).strip() #get the answer times
                        self.__PostTime = postTime#the post time of the first floor is also the post time of the post
                    if row > self.__LastFloorIndex:
                        dicList.append(dic)
                        #print('Length of post list: %d in page: %d'%(len(dicList), index))
                    if row > self.__MaxFloorIndex:
                        self.__MaxFloorIndex = row
                    #print('Done parsing floor :%d'%row)
                    dic = {}#renew a dictionary
            except Exception as e:
                self.__log('PostParser.__pageParser %s'%e)
            self.__PageDic[index] = dicList#collect the current page
            #print('Length of self.__PageDic: %d'%len(self.__PageDic))
            print('Done parsing page %d:%s'%(index,datetime.datetime.now()))
            #print(datetime.datetime.now())
        #print('Done parsing the post')

    def parse(self):
        #print('PID: %d'%os.getpid())
        self.__initUrlQueue()
        self.__pageParser()#11:01:26 - 11:06:14 110 - 5:48 - using just one process
        self.__store()

if __name__ == '__main__':
    #url = 'http://club.eladies.sina.com.cn/thread-5801720-1-1.html'
    #print(datetime.datetime.now())
    #startTime = datetime.datetime.now()
    #url = 'http://www.mala.cn/thread-12118779-1-1.html'
    url = 'http://www.mala.cn/thread-12118779-1-1.html'
    parser = PostParser(url)
    parser.parse()
    print('Done parsing post URL:%s'%url)
    #print(datetime.datetime.now())
    #endTime = datetime.datetime.now()
    #print('Post parsing cost: %s'%str(endTime - startTime))

