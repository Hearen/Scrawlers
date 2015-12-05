#-*- coding:utf-8 -*-
'''
@since: 2015-6-24
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
import threading
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
        print 'Retrieving from %s' % pageUrl
        html = 0
        #withdraw the page of the given url for three times - in case of failure the first time
        for i in range(3):
            try:
                html = urllib.urlopen(pageUrl).read()
                html = html.decode('gbk').encode('utf-8')#.encode('utf-8')#return int when failing to open the url
                if not isinstance(html, int):
                    break
            except Exception as e:
                print('ClubParser.__parsePage retrieving failed url:[%s]' % pageUrl)

        #open url fail or not
        if isinstance(html, int):
            print('ClubParser.__parsePage retrieving failed url:[%s]' % pageUrl)
            return None

        #print 'Retrieving successfully'
        soup = BeautifulSoup(html)
        #print(soup.prettify())
        #get the url of next page
        try:
            next_page = soup.find('div', class_="pages").find('a', class_="next")['href']
            next_page = pageUrl.split('/forum')[0] + '/' + next_page 
    
            root_url = pageUrl.split('/forum')[0] + '/';
            
            tbodies = soup.find_all('tbody')
        except Exception as e:
            print('Soup parse error in ClubParser.__parsePage %s'%e)
            return None

        #parse each post in block to get title and post url
        timestamp = 0#used to identify the first post in the current page
        for body in tbodies:
            try:
                postSoup = BeautifulSoup(str(body)) #parse each block to get the info of each post
                lastPostTimeString = postSoup.find('a', href=re.compile('.*?lastpost.*?')).string
                subUrl = postSoup.find('span').find('a', target="_blank")['href']
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
                    self.__UrlList.append(str(root_url + subUrl).encode('utf-8'))
                elif not isFirstPage:#only the first page will contain random posts - without time sequence
                    return None
                if isFirstPage and timestamp > self.__LastTimeStamp:#make sure the timestamp is the max
                    self.__LastTimeStamp = timestamp
                #time sequence is not strictly followed in some club just in the first page
            except Exception as e:#there may be some format exception, which is okay, continue
                continue
        #print('Page parsed successfully URL:%s'%pageUrl)
        return next_page
        
    def __savePostUrls(self):
        '''
        make sure the urls are fetched
        '''
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
        try:
            f = open(self.__TimeStampFilename, 'a')
            f.truncate(0)
            f.write(str(self.__LastTimeStamp) + '\r\n')
            f.write(time.ctime(self.__LastTimeStamp))
            f.close()
        except IOError as e:
            print('ClubParser.__saveLastTimeStamp failed: %s'%e)


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
        
        print(time.ctime(self.__LastTimeStamp0))
        
        #in downlist.parsePage, all the url in a page is stored in urlList
        #using next_page to traverse the nextpage and store the other urls of post
        next_page = self.__parsePage(self.__url, True)
    
        print("Next page:%s" % next_page)#can be used to debug
        while True:
            if next_page is None:
                break
            next_page = self.__parsePage(next_page, False)#till there is no next - the end of the club
            print("Next page:%s" % next_page)#can be used to debug
            if len(self.__UrlList) > 50:
                break
        #update the last modifed time of the current club
        self.__saveLastTimeStamp()
        
        print("Length of List:%d", len(self.__UrlList))
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
                postParser = PostParser(url)
                subProcess = Process(target=postParser.parse)
                processes.append(subProcess)
                subProcess.start()
                subProcess.join()
        '''
            
        #using unique url to parse the corresponding page
        for url in self.__UrlList:
            postParser = PostParser(url)
            subProcess = Process(target=postParser.parse)
            processes.append(subProcess)
            subProcess.start()
            #postParser.parse()
            print('Post parsed Url:%s' % url)
        
        for subProcess in processes:
            subProcess.join() 
        
        print('Done retrieving all posts of club url : %s' % self.__url)
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
    #1. 女性论坛 - 美容护肤；
    #2. 女性论坛 - 窈窕身姿；
    #3. 军事论坛 - 三军论坛 - 空军版；
    #4. 军事论坛 - 军事历史；  
    clubUrls = [
               'http://club.eladies.sina.com.cn/forum-2-1.html',
               'http://club.eladies.sina.com.cn/forum-7-1.html',
               'http://club.mil.news.sina.com.cn/forum-6-1.html',
               'http://club.mil.news.sina.com.cn/forum-9-1.html'
               ]
    print('Start working: %s'%datetime.datetime.now())
    startTime = datetime.datetime.now()
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
    print('Parse all clubs successfully! Time cose: %s'%str(endTime - startTime))
    