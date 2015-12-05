#-*- coding:utf-8 -*-

"""
@since 2015-6-30
@author: Hearen
@contact: LHearen@126.com
"""
import urllib
import time
import datetime
import sys
import re
import os
import threading
import spynner
import pyquery
import multiprocessing
from multiprocessing import Process
from bs4 import BeautifulSoup
from PostParser import PostParser

MaxProcessNum = 5

class ClubParser():
    """
    multi-thread in this case will just decrease the speed of downloading
    - as a result there will be a mainthread to control and a child thread to download
    """
    def __init__(self, url, previousLastTimeStamp):
        self.__url = url #the root url of a club
        self.__PostUrlList = []
        self.__PageUrlQueue = multiprocessing.Manager().Queue()#used to store page urls of the club
        
    def __initPageUrls(self):
        
        for i in range(3):
            try:
                browser = spynner.Browser()
                browser.create_webview()
                browser.set_html_parser(pyquery.PyQuery)
                browser.load(self.__url, 20)
                 
                try:
                    browser.wait_load(10)
                except:
                    pass
                 
                html = browser.html.encode('utf-8')
                browser.close()
                if not isinstance(html, int):
                    break
            except Exception as e:
                print('ClubParser.__initPageUrls failed retrieving in Url: %s' % self.__url)
        if isinstance(html, int):
            print('ClubParser.__initPageUrls failed retrieving in Url: %s' % self.__url)
            return None
        soup = BeautifulSoup(html)
        f = open('tmp.txt', 'a')
        f.truncate(0)
        f.write(html)
        f.close()
        pageAmount = 1000
        if soup.find('div', class_='pages') is not None:
            aTags = soup.find('div', class_='pages').findAll('a')
            for aTag in aTags:
                tmpString = aTag.string.encode('utf-8')
                if tmpString.isdigit() and int(tmpString) > pageAmount:
                    pageAmount = int(tmpString)
        #http://women.club.sohu.com/zz0894/threads/p1?type=all&order=rtime
        for i in range(1, pageAmount + 1):
            url = self.__url + '/p' + str(i) + '?type=all&order=rtime'
            self.__PageUrlQueue.put(url)
        
    #return the next page url or False
    def __parsePage(self, pageUrl):
        """
        using the url of the club - stored in self.__url 
        to retrive all the posts urls belonging to the club
        isFirstPage - used to identify the first post url - to record the last time stamp so far
        return - can be None or url of next page
        """        

        #withdraw the page of the given url for three times - in case of failure the first time
        for i in range(3):
            try:
                browser = spynner.Browser()
                browser.create_webview()
                browser.set_html_parser(pyquery.PyQuery)
                browser.load(self.__url, 10)
                 
                try:
                    browser.wait_load(10)
                except:
                    pass
                 
                html = browser.html.encode('utf-8')
                browser.close()
                if not isinstance(html, int):
                    break
            except Exception as e:
                print('ClubParser.__parsePage failed retrieving in Url: %s' % pageUrl)

        #open url fail or not
        if isinstance(html, int):
            print('ClubParser.__parsePage failed retrieving in Url: %s' % pageUrl)
            return None

        soup = BeautifulSoup(html)
        #print(soup.prettify())
        #get the url of next page
        try:
            tbodies = soup.find('table', class_='postlist').find('tbody').findAll('tr')
        except Exception as e:
            print('Soup parse error in ClubParser.__parsePage %s'%e)
            return None

        for body in tbodies:
            try:
                postSoup = BeautifulSoup(str(body)) #parse each block to get the info of each post
                aTag = postSoup.find('td', class_='posttitle').find('a')
                if aTag is None:
                    continue
                url = aTag['href']
                if 'http:' not in url:
                    url = 'http://' + self.__url[7:].split('/')[0] + url
                '''
                lastPostTag = postSoup.find('span', class_='new_format')
                if lastPostTag is None:
                    continue
                lastPostTimeString = lastPostTag.string.encode('utf-8')
                '''
                self.__PostUrlList.append(url)
            except Exception as e:#there may be some format exception, which is okay, continue
                continue
        print('Page parsed successfully URL:%s'%pageUrl)
        return True
        
    def __savePostUrls(self):
        '''
        make sure the urls are fetched
        '''
        filePath = './data'
        if not os.path.exists(filePath):
            os.mkdir(filePath)
        try:
            f = open('./data/postUrl-%s.txt'%self.__url[7:].replace(r'/', '-').replace('.', '-'), 'a')
            for url in self.__PostUrlList:
                f.write(url+'\n')
            f.close()
        except IOError as e:
            print('write_list IOException:%s'%e)
    

    def __parseClub(self):
        """
        get all posts urls in a club
        url - the root url of a  club
        initDeadline - the default timestamp when there is no file existed
        """
        
        while not self.__PageUrlQueue.empty() and \
            self.__parsePage(self.__PageUrlQueue.get()):
            if len(self.__PostUrlList) > 50:
                break
        
        print("Length of List:%d", len(self.__PostUrlList))
        
        processes = []
        while len(self.__PostUrlList):
            listLen=len(self.__PostUrlList)
            if MaxProcessNum > listLen:
                processNum = listLen 
            else:
                processNum = MaxProcessNum
            for i in range(processNum):
                url = self.__PostUrlList.pop()
                postParser = PostParser(self.__url, url)
                subProcess = Process(target=postParser.parse)
                processes.append(subProcess)
                subProcess.start()
                subProcess.join()
            
        print('Done retrieving all posts of club url : %s' % self.__url)
        return True 


    def parse(self):
        print('Start parsing club: %s'%self.__url)
        self.__initPageUrls()
        self.__parseClub()
        self.__savePostUrls()
        #childThread = threading.Thread(target=self.__parseClub)
        #childThread.start()
        #childThread.join()
        
if __name__ == '__main__':
    reload(sys)
    print(sys.getdefaultencoding())
    #1. 情感天地 -> 情感阵营 -> 情感杂谈
    #2. 情感天地 -> 情感阵营 -> 情感宣泄室
    #3. 情感天地 -> 情感阵营 -> 围城故事
    #4. 情感天地 -> 情感阵营 -> 夕阳有约
    #5. 情感天地 -> 情感阵营 -> 灌水驿站
    clubUrls = [
               'http://women.club.sohu.com/zz0894/threads',
               'http://women.club.sohu.com/andun/threads',
               'http://women.club.sohu.com/marriage/threads',
               'http://women.club.sohu.com/xyyx/threads',
               'http://women.club.sohu.com/zz482/threads'
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
    