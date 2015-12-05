#coding:utf-8
'''
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
from bs4 import BeautifulSoup
from multiprocessing import Process
from PostParser import PostParser

MaxProcessNum = 5

class ClubParser():
    """
    multi-thread in this case will just decrease the speed of downloading
    - as a result there will be a mainthread to control and a child thread to download
    """
    def __init__(self, url, previousLastTimeStamp):
        self.__url = url #the root url of a club
        self.__PageAmount = 1
        self.__UrlList = []
        self.__LastTimeStamp0 = previousLastTimeStamp #the last modified timestamp stored in a file
        self.__LastTimeStamp = 0 #used to store the current last modified timestamp
        self.__TimeStampFilename = './data/' + self.__url[7:].replace(r'/', '-').replace('.html', '').replace('.', '-') + '.txt'

    #return the next page url or False
    
    def __parsePage(self, pageUrl, isFirstPage):
        """
        using the url of the club - stored in self.__url 
        to retrive all the posts urls belonging to the club
        isFirstPage - used to identify the first post url - to record the last time stamp so far
        return - can be None or url of next page
        """        
        #print 'Retrieving from %s' % pageUrl

        #withdraw the page of the given url for three times - in case of failure the first time
        for i in range(3):
            try:
                html = urllib.urlopen(pageUrl).read()
                html = html.decode('gbk', 'ignore').encode('utf-8')#.encode('utf-8')#.encode('utf-8')#return int when failing to open the url
                if not isinstance(html, int):
                    break
            except Exception as e:
                print('Unable to download page [trying for:%d][%s]' % (html, pageUrl))

        #open url fail or not
        if isinstance(html, int):
            return False
        soup = BeautifulSoup(html)
        #print(soup.prettify())
        #get amount of pages in the first page
        if isFirstPage:
            try:
                pageAmountString = soup.find('div', class_='pg').find('label').find('span').string.encode('utf-8')
                self.__PageAmount = int(re.findall(r'\d+', pageAmountString)[0])
            except Exception as e:
                print('Soup parse error in ClubParser.__parsePage %s'%e)
                return False
        tbodies = soup.find_all('tbody', attrs={'id':re.compile(r'.*?thread.*?')})
        #parse each post in block to get title and post url
        timestamp = 0#used to identify the first post in the current page
        for body in tbodies:
            try:
                postSoup = BeautifulSoup(str(body)) #parse each block to get the info of each post
                lastPostTimeString = postSoup.findAll('td', class_='by')[1].find('em').find('a').string.encode('utf-8')
                postUrl = postSoup.find('th').find('a', class_='s xst')['href'].encode('utf-8')
                #only root_url and subUrl both can identify the post url
                
                timeArray = time.strptime(lastPostTimeString, "%Y-%m-%d %H:%M")
                
                #only the first page and the first timestamp 
                #can be used as the last modified time
                #record the last modified timestamp of the club
                if((isFirstPage) and (timestamp == 0)):
                    timestamp = int(time.mktime(timeArray))
                    self.__LastTimeStamp = timestamp
                timestamp = int(time.mktime(timeArray))
                #===============================================================
                #collect the newer posts according to the timestamp
                #the list is sorted in time order
                #so once the timestamp is less than the deadline,
                #you can just ignore all the rest
                #===============================================================
                if(timestamp >= self.__LastTimeStamp0):
                    self.__UrlList.append(postUrl)
                elif not isFirstPage:#only the first page will contain random posts - without time sequence
                    return False
                if isFirstPage and timestamp > self.__LastTimeStamp:#make sure the timestamp is the max
                    self.__LastTimeStamp = timestamp
                #time sequence is not strictly followed in some club just in the first page
            except Exception as e:#there may be some format exception, which is okay, continue
                print('ClubParser.__parsePage bodies failed %s'%e)
                continue
        #print('Page parsed successfully URL:%s'%pageUrl)
        return True
        
    def __savePostUrls(self):
        '''
        make sure the urls are fetched
        '''
        dirPath = './data'
        if not os.path.exists(dirPath):
            os.makedirs(dirPath)
        filePath = './data/postUrl-%s.txt'%self.__url[7:].replace(r'/', '-').replace('.html', '').replace('.', '-')
        try:
            f = open(filePath, 'a')
            for url in self.__UrlList:
                f.write(url+'\n')
            f.close()
        except IOError as e:
            print('write_list IOException:%s'%e)
    
    def __saveLastTimeStamp(self):
        """
        save the last modified timestamp and time to a file named by the club url 
        the first line of the file
        update the last modified time
        """
        dirPath = './data'
        if not os.path.exists(dirPath):
            os.makedirs(dirPath)
        try:
            f = open(self.__TimeStampFilename, 'a')
            f.truncate(0)
            f.write(str(self.__LastTimeStamp) + '\r\n')
            f.write(time.ctime(self.__LastTimeStamp))
            f.close()
        except IOError as e:
            print('ClubParser.__saveLastTimeStamp :%s'%e)


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
            #print(time.ctime(self.__LastTimeStamp0))

    def __parseClub(self):
        """
        get all posts urls in a club
        url - the root url of a  club
        initDeadline - the default timestamp when there is no file existed
        """
        #print('Start parsing club: %s'%self.__url)
        #thread = threading.current_thread()
        #print(thread.getName())
        
        self.__initLastTimeStamp0()
        
        #print(time.ctime(self.__LastTimeStamp0))
        
        #in downlist.parsePage, all the url in a page is stored in urlList
        #using next_page to traverse the nextpage and store the other urls of post
        self.__parsePage(self.__url, True)
        urlSS = self.__url.split('.html')[0].split('-')
        baseUrl = urlSS[0] + '-' + urlSS[1]
        for i in range(2, self.__PageAmount + 1):
            nextPage = baseUrl + '-' + str(i) + '.html'
            #print("Next page:%s" % nextPage)#can be used to debug
            if not self.__parsePage(nextPage, False):
                break;
            if len(self.__UrlList) > 300:
                break
        
        #update the last modifed time of the current club
        self.__saveLastTimeStamp()
        '''
        print("Length of List:%d", len(self.__UrlList))
        while len(self.__UrlList):
            url = self.__UrlList.pop()
            postParser = PostParser(url)
            postParser.parse()
        '''
        processes = []
        while len(self.__UrlList):
            listLen=len(self.__UrlList)
            if MaxProcessNum > listLen:
                processNum = listLen 
            else:
                processNum = MaxProcessNum
            for i in range(processNum):
                url = self.__UrlList.pop()
                postParser = PostParser(url)
                subProcess = Process(target=postParser.parse)
                processes.append(subProcess)
                subProcess.start()
                subProcess.join()
        
        print('Done parsing all posts of club url : %s' % self.__url)
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
    #1. 麻辣生活论坛 - 医院论坛；
    #2. 麻辣生活论坛 - 财富沙龙；
    #3. 麻辣生活论坛 - 亲子乐园；
    #4. 麻辣生活论坛 - 情感空间；  
    #5. 麻辣生活论坛 - 消费维权
    clubUrls = [
               'http://www.mala.cn/forum-57-1.html',
               'http://www.mala.cn/forum-85-1.html',
               'http://www.mala.cn/forum-264-1.html',
               'http://www.mala.cn/forum-10-1.html',
               'http://www.mala.cn/forum-838-1.html'
               ]
    print(datetime.datetime.now())
    startTime = datetime.datetime.now()
    '''
    #one process - easy to test
    for clubUrl in clubUrls:
        clubParser = ClubParser(clubUrl, 1414639980)#Thu Oct 30 11:33:00 2014
        clubParser.parse()
    '''
    #handle each club
    subProcesses = []
    for clubUrl in clubUrls:
            clubParser = ClubParser(clubUrl, 1414639980)#Thu Oct 30 11:33:00 2014
            subProcess = Process(target=clubParser.parse)
            subProcess.start()
            subProcesses.append(subProcess)
            
    for subProcess in subProcesses:
        subProcess.join()
    
    #print(datetime.datetime.now())
    endTime = datetime.datetime.now()
    print(endTime - startTime)
    print('Parse all clubs successfully!')
    
