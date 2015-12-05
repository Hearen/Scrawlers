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
    def __init__(self, clubUrl, url):
        self.__url = url # the url of the post - the very beginning page

        self.__SubDir = clubUrl[7:].split('.html')[0].replace('.', '-').replace('/', '-')
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
        and at the same time get the post unique id and pageAmount
        '''
        #try another two times when failed to get the page the first time
        for i in range(3):
            html = urllib.urlopen(self.__url).read()
            if isinstance(html, int):
                time.sleep(3)
            else:
                break;
        #if at last, still get nothing, return None
        if isinstance(html, int):
            return False

        soup = BeautifulSoup(str(html))

        #retrieve the unique post ID from url
        self.__PostId = int(re.findall(r'\d+', self.__url)[0])

        self.__initStartIndex()

        self.__PageAmount = self.__getPageAmount(soup.find('div', class_='pg'))
        if self.__PageAmount < 1:
            self.__PageAmount = 1
            self.__UrlQueue.put(self.__url)
            return True
        ssTmp = self.__url.split('.html')[0].split('-')
        baseUrl = ssTmp[0] + '-' + ssTmp[1] + '-';
        for i in range(1, self.__PageAmount + 1):
            self.__UrlQueue.put(baseUrl + str(i) + '-' + ssTmp[3] + '.html')
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
                        for k, v in dic.iteritems():#one post including comments
                            if k != '评论':
                                f.write(k + ':' + str(v) + '\n')
                        if dic.has_key('评论'):#comments '评论' in dic.keys()
                            f.write('评论:\n')
                            for commentDic in dic['评论']:#dicList
                                f.write('\t--------------------\n')
                                for k1, v1 in commentDic.iteritems():#comment dic
                                    f.write('\t' + k1 + ':' + str(v1) + '\n')
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
                        content += child.encode('utf-8').strip()
                else:
                    if 'src="' in str(child):
                        content += '[http://bbs.iyaxin.com/' + child['src'].encode('utf-8') + ']'
                    elif 'file="' in str(child):
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
        f.write('ID: %d Time: %s Error: %s Url: %s\n'%\
                (self.__PostId, datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),\
                  e, self.__url))
        f.close()

    def __getActualAmount(self, countString):
        '''
        get the number instead of number with special units like 万
        '''
        count = 0
        if '万' not in countString:
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

            index = int(re.findall(r'\d+-(\d+)-\d+', url)[0])

            #retrieve the html of the given url
            #if the first try failed, try for another two times
            html = None
            for i in range(3):
                html = urllib.urlopen(url).read()
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
            floors = soup.findAll('div', id=re.compile(r'post_\d+'))
            row = 0
            try:
                for floor in floors:
                    floorSoup = BeautifulSoup(str(floor))
                    personName = ''
                    personUrl = ''
                    personID = 0
                    personHeadPortrait = ''
                    personLevel = '管理员'
                    topicCount = 0
                    postCount = 0
                    creditCount = 0
                    specialtyTag = floorSoup.find('div', class_='pi')

                    if specialtyTag is not None \
                         and '亚心网民' in self.__parseContent(specialtyTag):
                        personName = '亚心网民'
                        personLevel = '亚心网民'
                    else:
                        personName = floorSoup.find('div', class_='authi').find('a').string.encode('utf-8')
                        personUrl = floorSoup.find('div', class_='authi').find('a')['href'].encode('utf-8')
                        personID = int(re.findall(r'\d+', personUrl)[0])
                        headTag = floorSoup.find('a', class_='avtm')
                        if headTag is not None:
                            personHeadPortrait = headTag.find('img')['src'].encode('utf-8')
                        personLevelTag = floorSoup.find('font', color='a9c705')
                        if personLevelTag is not None:
                            personLevelString = personLevelTag.string.encode('utf-8')
                            personLevel = re.findall(r'\d+', personLevelString)[0]
                            personLevel += '级'
                        topicCount = int(floorSoup.find('div', class_='tns xg2').findAll('p')[0].find('a').string.encode('utf-8'))
                        postCountString = floorSoup.find('div', class_='tns xg2').findAll('p')[1].find('a').string.encode('utf-8')
                        postCount = self.__getActualAmount(postCountString)

                        creditCountString = floorSoup.find('div', class_='tns xg2').findAll('p')[2].find('a').string.encode('utf-8')
                        creditCount = self.__getActualAmount(creditCountString)
                    postTimeString = floorSoup.find('td', class_='plc').find('div', class_='authi').find('em').string.encode('utf-8')
                    postTime = re.findall(r'\d+-\d+-\d+\s\d+:\d+', postTimeString)[0]
                    rowString = floorSoup.find('td', class_='plc').find('div', class_='pi').find('em').string.encode('utf-8')
                    if rowString.isdigit():
                        row = int(rowString)
                    else:
                        row += 1#increase by 1 when the row is not formal

                    #get the content of the floor

                    contentTag = floorSoup.find('td', class_='t_f')
                    content = self.__parseContent(contentTag)
                    commentList = []
                    commentTmpTag = floorSoup.find('div', class_='cm')
                    if commentTmpTag is not None:
                        commentTags = commentTmpTag.findAll('div', class_='pstl xs1 cl')


                        for commentTag in commentTags:
                            commentDic = {}
                            commentSoup = BeautifulSoup(str(commentTag))
                            cPersonUrl = commentSoup.find('div', class_='psta vm').find('a')['href'].encode('utf-8')
                            cPersonId = int(re.findall(r'\d+', cPersonUrl)[0])
                            cPersonHeadPortrait = commentSoup.find('div', class_='psta vm').find('img')['src'].encode('utf-8')
                            cPersonName = commentSoup.find('a', class_='xi2 xw1').string.encode('utf-8')
                            cContentTag = commentSoup.find('div', class_='psti')
                            postTimeString = cContentTag.find('span', class_="xg1").string.encode('utf-8')
                            postTime = re.findall(r'\d+-\d+-\d+\s\d+:\d+', postTimeString)[0]
                            cComment = self.__parseContent(cContentTag)
                            commentDic['用户昵称'] = cPersonName
                            commentDic['用户ID'] = cPersonId
                            commentDic['头像地址'] = cPersonHeadPortrait
                            commentDic['空间地址'] = cPersonUrl
                            commentDic['内容'] = cComment
                            commentDic['回复时间'] = postTime
                            commentList.append(commentDic)

                    dic['昵称'] = personName
                    dic['空间地址'] = personUrl
                    dic['用户头像'] = personHeadPortrait
                    dic['用户等级'] = personLevel
                    dic['积分'] = creditCount
                    dic['用户ID'] = personID
                    dic['发起主题数'] = topicCount
                    dic['发帖数'] = postCount
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
    clubUrl = 'http://bbs.iyaxin.com/forum-91-1.html'
    url = 'http://bbs.iyaxin.com/thread-1003506-1-1.html'
    parser = PostParser(clubUrl, url)
    parser.parse()
    print('Done parsing page: %s!!!'%url)
    #print(datetime.datetime.now())
    #endTime = datetime.datetime.now()
    #print(endTime - startTime)
