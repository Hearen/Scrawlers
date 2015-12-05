#coding: utf-8 #############################################################
# File Name: main.py
# Author: mylonly
# mail: mylonly@gmail.com
# Created Time: Wed 11 Jun 2014 08:22:12 PM CST
#########################################################################
#!/usr/bin/python

import re
import urllib.request
import threading
import time
from queue import Queue
from html.parser import HTMLParser

#各图集入口链接
htmlDoorList = []
#包含图片的Hmtl链接
htmlUrlList = []
#图片Url链接Queue
imageUrlList = Queue(0)
#捕获图片数量
imageGetCount = 0
#已下载图片数量
imageDownloadCount = 0
#每个图集的起始地址，用于判断终止
nextHtmlUrl = ''
#本地保存路径
localSavePath = './'

#this cannot select different images while the whole mechanism is wrong - stroing the different urls will make it work
#如果你想下你需要的分辨率的，请修改replace_str,有如下分辨率可供选择1920x1200，1980x1920,1680x1050,1600x900,1440x900,1366x768,1280x1024,1024x768,1280x800
replace_str = '1920x1080'

replaced_str = '960x600'

#内页分析处理类 collect all the images belonging to the same collection
class ImageHtmlParser(HTMLParser):
	def __init__(self):
		self.nextUrl = ''
		HTMLParser.__init__(self)
	def handle_starttag(self,tag,attrs):
		global imageUrlList
		if(tag == 'img' and len(attrs) > 2 ):
			if(attrs[0] == ('id','bigImg')):#the idendification of the image
				url = attrs[1][1]
				url = url.replace(replaced_str,replace_str)
				imageUrlList.put(url)
				global imageGetCount
				imageGetCount = imageGetCount + 1
				print(url)
		elif(tag == 'a' and len(attrs) == 4):
			if(attrs[0] == ('id','pageNext') and attrs[1] == ('class','next')):
				global nextHtmlUrl	
				nextHtmlUrl = attrs[2][1];

#首页分析类
class IndexHtmlParser(HTMLParser):
	def __init__(self):
		self.urlList = []
		self.index = 0
		self.nextUrl = ''
		self.tagList = ['li','a']
		self.classList = ['photo-list-padding','pic']
		HTMLParser.__init__(self)
	def handle_starttag(self,tag,attrs):
		if(tag == self.tagList[self.index]):#to handle this specific website, there are too many optimized methods
			for attr in attrs:
				if (attr[1] == self.classList[self.index]):
					if(self.index == 0):
						#第一层找到了
						self.index = 1
					else:
						#第二层找到了
						self.index = 0#go out after storing the url of a inside li
						print(attrs[1][1])
						self.urlList.append(attrs[1][1])#store the urls inside li, just another collection
						break
		elif(tag == 'a'):#find all the urls in the next page - about 31 pages
			for attr in attrs:
				if (attr[0] == 'id' and attr[1] == 'pageNext'):
					self.nextUrl = attrs[1][1]
					print('nextUrl:',self.nextUrl)
					break

#首页Hmtl解析器
indexParser = IndexHtmlParser()
#内页Html解析器
imageParser = ImageHtmlParser()

#根据首页得到所有入口链接
print('开始扫描首页...')
host = 'http://desk.zol.com.cn'
indexUrl = '/meinv/'
while (indexUrl != ''):
	print('正在抓取网页:',host+indexUrl)
	try:
		con = urllib.request.urlopen(host+indexUrl).read()
		indexParser.feed(str(con))#storing all the scrawleable urls from the current page and all the next pages;
		count += 1
		if (indexUrl == indexParser.nextUrl):#in the current situation, there is no need to check this condition - no pageNext any more
			break
		else:
			indexUrl = indexParser.nextUrl#traverse all the next pages with urls
	except urllib.request.URLError as e:
		print(e.reason)

print('首页扫描完成，所有图集链接已获得：')
htmlDoorList = indexParser.urlList

#根据入口链接得到所有图片的url
class getImageUrl(threading.Thread):
	def __init__(self):
		threading.Thread.__init__(self)
	def run(self):
		for door in htmlDoorList:
			print('开始获取图片地址,入口地址为:',door)
			global nextHtmlUrl
			nextHtmlUrl = ''
			while(door != ''):
				print('开始从网页%s获取图片...'% (host+door))
				if(nextHtmlUrl != ''):
					con = urllib.request.urlopen(host+nextHtmlUrl).read()
				else:
					con = urllib.request.urlopen(host+door).read()
				try:
					imageParser.feed(str(con))
					print('下一个页面地址为:',nextHtmlUrl)
					if(door == nextHtmlUrl):
						break
				except urllib.request.URLError as e:
					print(e.reason)
		print('所有图片地址均已获得:',imageUrlList)

class getImage(threading.Thread):
	def __init__(self):
		threading.Thread.__init__(self)
	def run(self):
		global imageUrlList
		print('开始下载图片...')
		while(True):
			print('目前捕获图片数量:',imageGetCount)
			print('已下载图片数量:',imageDownloadCount)
			image = imageUrlList.get()
			print('下载文件路径:',image)
			try:
				cont = urllib.request.urlopen(image).read()
				patter = '[0-9]*\.jpg';
				match = re.search(patter,image);
				if match:
					print('正在下载文件：',match.group())
					filename = localSavePath+match.group()
					f = open(filename,'wb')
					f.write(cont)
					f.close()
					global imageDownloadCount
					imageDownloadCount = imageDownloadCount + 1
				else:
					print('no match')
				if(imageUrlList.empty()):
					break
			except urllib.error.URLError as e:
				print(e.reason)
		print('文件全部下载完成...')

get = getImageUrl()
get.start()
print('获取图片链接线程启动:')

# time.sleep(2)

download = getImage()
download.start()
print('下载图片链接线程启动:')