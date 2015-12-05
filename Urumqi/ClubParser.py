#-*- coding:utf-8 -*-
'''
@since: 2015-6-29
@author: Hearen
@contact: LHearen@126.com
'''
"""
"""
import urllib
import time
import datetime
import sys
import re
import os
import threading
import multiprocessing
from Queue import Queue
from multiprocessing import Process
from bs4 import BeautifulSoup
from PageParser import PostParser

MaxProcessNum = 5

reload(sys)

sysEncoding = sys.getdefaultencoding()

class ClubParser():
    """
    multi-thread in this case will just decrease the speed of downloading
    - as a result there will be a mainthread to control and a child thread to download
    """
    def __init__(self, url, previousLastTimeStamp):
        self.__url = url #the root url of a club
        self.__PageUrlQueue = multiprocessing.Manager().Queue()
        self.__UrlList = []
        self.__LastTimeStamp0 = previousLastTimeStamp #the last modified timestamp stored in a file
        self.__LastTimeStamp = 0 #used to store the current last modified timestamp
        self.__TimeStampFilename = './data/' + self.__url[7:].replace(r'/', '-').replace('.html', '').replace('.', '-') + '.txt'

    #return the next page url or False
    
    def __log(self, filename, s):
        f = open(filename, 'a')
        f.write(s)
        f.close()
    
    
    def __parsePage(self, pageUrl, isFirstPage):
        """
        using the url of the club - stored in self.__url 
        to retrive all the posts urls belonging to the club
        isFirstPage - used to identify the first post url - to record the last time stamp so far
        return - can be None or url of next page
        """        
        print('Start fetching post urls in page %s' % pageUrl)
        html = 0
        #withdraw the page of the given url for three times - in case of failure the first time
        for i in range(3):
            try:
                html = urllib.urlopen(pageUrl).read().decode('utf-8').encode('utf-8')
                if not isinstance(html, int):
                    break
            except Exception as e:
                print('ClubParser.__parsePage retrieving failed url:%s'%pageUrl)

        #open url fail or not
        if isinstance(html, int):
            print('ClubParser.__parsePage retrieving failed url:%s'%pageUrl)
            return False

        #print 'Retrieving successfully'
        soup = BeautifulSoup(html)
        #cannot easily get the floors as the others
        #get the essential data we need - the last post time and url
        byTags = soup.findAll('td', class_='by')
        aTags = soup.findAll('a',  class_= 's xst')
        timestamp = 0
        urlIndex = 3 #two for label and one for the post starter in byTags
            #parse each post in block to get title and post url
        for aTag in aTags:
            try:
                url = aTag['href'].encode('utf-8')
                #title = aTag.string.encode('utf-8')
                lastPostTimeString = byTags[urlIndex].find('em').find('a').string.encode('utf-8')
                urlIndex += 2
                timeArray = time.strptime(lastPostTimeString, "%Y-%m-%d %H:%M")
                #self.__log('c.html', title + lastPostTimeString + '\n')
                timestamp = int(time.mktime(timeArray))
                if(timestamp >= self.__LastTimeStamp0):
                    self.__UrlList.append(url)
                elif not isFirstPage:#only the first page will contain random posts - without time sequence
                    return False
                if isFirstPage and timestamp > self.__LastTimeStamp:#make sure the timestamp is the max
                    self.__LastTimeStamp = timestamp
                #time sequence is not strictly followed in some club just in the first page
            except Exception as e:#there may be some format exception, which is okay, continue
                continue
        #print('Page parsed successfully URL:%s'%pageUrl)
        return True
        
    def __savePostUrls(self):
        '''
        make sure the urls are fetched
        '''
        filePath = './data'
        if not os.path.exists(filePath):
            os.makedirs(filePath)
        try:
            f = open('./data/postUrl-%s.txt'%self.__url[7:].replace(r'/', '-').replace('.html', '').replace('.', '-'), 'a')
            for url in self.__UrlList:
                f.write(url+'\n')
            f.close()
        except IOError as e:
            print('ClubParser.__savePostUrls saving posts failed: %s'%e)
    
    def __saveLastTimeStamp(self):
        """
        save the last modified timestamp and time to a file named by the club url 
        the first line of the file
        update the last modified time
        """
        filePath = './data'
        if not os.path.exists(filePath):
            os.makedirs(filePath)
        try:
            f = open(self.__TimeStampFilename, 'a')
            f.truncate(0)
            f.write(str(self.__LastTimeStamp) + '\r\n')
            f.write(time.ctime(self.__LastTimeStamp))
            f.close()
        except IOError as e:
            print('ClubParser.__saveLastTimeStamp failed: %s'%e)

    def __initPageUrlQueue(self):
        '''
        via the first page of the club to find out all the page urls 
        in the club and store them in queue for latter use
        '''
        print('Start getting all page urls for club %s'%self.__url) 
        html = 0
        #withdraw the page of the given url for three times - in case of failure the first time
        for i in range(3):
            try:
                html = urllib.urlopen(self.__url).read()#.encode('utf-8')#return int when failing to open the url
                if not isinstance(html, int):
                    break
            except Exception as e:
                print('ClubParser.__initPageUrlQueue retrieving failed url:[%s]' % self.__url)

        #open url fail or not
        if isinstance(html, int):
            print('ClubParser.__initPageUrlQueue retrieving failed url:[%s]' % self.__url)
            return None

        #print 'Retrieving successfully'
        soup = BeautifulSoup(html)
        pageAmountString = soup.find('div', class_='pg').find('a', class_='last').string.encode('utf-8')
        pageAmount = int(re.findall(r'\d+', pageAmountString)[0])
        
        ssTmp = self.__url.split('.html')[0].split('-')
        baseUrl = ssTmp[0] + '-' + ssTmp[1] + '-';
        for i in range(1, 2):
            self.__PageUrlQueue.put(baseUrl + str(i) + '.html')
        print('Done initializing all page urls for club %s!!'%self.__url)
        


    def __initLastTimeStamp0(self):
        """
        via the filename to init the timestamp - the previous last reply timestamp
        at the top line of the file - each club will have a file
        if there is not such time stamp just use the value given in constructor
        """
        try:
            f = open(self.__TimeStampFilename, 'r')
            timeStampString = f.readline().strip()
            f.close()
        except Exception:#if there is no such file, just return without any change
            return
        if timeStampString.isdigit():
            self.__LastTimeStamp0 = int(timeStampString)
            print(time.ctime(self.__LastTimeStamp0))

    def __parseClub(self):
        """
        get all posts urls in a club
        url - the root url of a  club
        initDeadline - the default timestamp when there is no file existed
        """
        print('Start parsing club: %s'%self.__url)
        #thread = threading.current_thread()
        #print(thread.getName())
        
        self.__initLastTimeStamp0()
        
        self.__initPageUrlQueue()
        
        print('Posts after %s will be parsed'%str(time.ctime(self.__LastTimeStamp0)))
        
        #in downlist.parsePage, all the url in a page is stored in urlList
        #using next_page to traverse the nextpage and store the other urls of post
        isFirstPage = True
        while not self.__PageUrlQueue.empty() and \
            self.__parsePage(self.__url, isFirstPage):
            isFirstPage = False
            if len(self.__UrlList) > 50:
                break
            
        #update the last modifed time of the current club
        self.__saveLastTimeStamp()
        
        print("%d of posts to be parsed in club:%s"%(len(self.__UrlList), self.__url))
        processes = []
        '''
        while len(self.__UrlList):
            listLen=len(self.__UrlList)
            if MaxProcessNum > listLen:
                processNum = listLen 
            else:
                processNum = MaxProcessNum
            for i in range(processNum):
                url = self.__UrlList.pop()
                postParser = PostParser(self.__url, url)
                subProcess = Process(target=postParser.parse)
                processes.append(subProcess)
                subProcess.start()
                subProcess.join()
        '''
            
        #using unique url to parse the corresponding page
        for url in self.__UrlList:
            postParser = PostParser(self.__url, url)
            subProcess = Process(target=postParser.parse)
            processes.append(subProcess)
            subProcess.start()
            #postParser.parse()
            print('Start parsing post in Url:%s' % url)
        
        for subProcess in processes:
            subProcess.join() 
        
        print('Done parsing all posts of club url: %s'%self.__url)
        return True 


    def parse(self):
        self.__parseClub()
        self.__savePostUrls()
        #childThread = threading.Thread(target=self.__parseClub)
        #childThread.start()
        #childThread.join()
        
if __name__ == '__main__':
    reload(sys)
    print(sys.getdefaultencoding())
    #1. 生活 - 新疆买房
    #2. 声音 - 关注民生
    #3. 爱好 - 情感文学
    #4. 热点 - 饮食男女
    #5. 热点 - 乌鲁木齐24小时
    clubUrls = [
               'http://bbs.iyaxin.com/forum-219-1.html',
               'http://bbs.iyaxin.com/forum-91-1.html',
               'http://bbs.iyaxin.com/forum-177-1.html',
               'http://bbs.iyaxin.com/forum-196-1.html',
               'http://bbs.iyaxin.com/forum-867-1.html'
               ]
    print('Start working: %s'%datetime.datetime.now())
    startTime = datetime.datetime.now()
    '''
    for clubUrl in clubUrls:
        clubParser = ClubParser(clubUrl, 1414639980)
        clubParser.parse()
    
    '''
    #handle each club
    subProcesses = []
    for clubUrl in clubUrls:
            clubParser = ClubParser(clubUrl, 1414639980)
            subProcess = Process(target=clubParser.parse)
            subProcess.start()
            subProcesses.append(subProcess)
            
    for subProcess in subProcesses:
        subProcess.join()
    
    print('Finished at: %s'%datetime.datetime.now())
    endTime = datetime.datetime.now()
    print('Parse all clubs successfully! Time cost: %s'%str(endTime - startTime))
    