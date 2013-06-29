import pymongo
from pymongo import *

from bs4 import BeautifulSoup

import requests
import uuid
from uuid import *

from threading import Thread
#from time import sleep

import ThreadPool
from ThreadPool import *

import tldextract

from urlparse import *

'''
fk_index_coll = Connection('localhost',27018)['fk_data']['index']

sitemap_file = "flipkart_sitemap.xml"
f = open(sitemap_file,'r')
site_map = f.read()
smap_soup = BeautifulSoup(site_map)
'''

def threaded_function(root_domain,url):
    print 'thread, downloading',url,root_domain
    data = requests.get(url)
    site_data_soup = BeautifulSoup(data.content)
    a_tags = site_data_soup.find_all('a')
    for a_tag in a_tags:
        if 'href' in a_tag.attrs and a_tag.name == "a" and a_tag['href'] != '#':
            parse_result = urlparse(a_tag['href'])
            if parse_result.scheme == 'http':
                href =  a_tag['href']
                if tldextract.extract(href).domain != root_domain:
                    print 'discarding',href
                else:
                    print 'OK',href



    '''
    fk_data_soup = BeautifulSoup(data.content)
    book_data = {'title':'','keywords':'','description':'','og_title':'','og_image':'','url':'','smap_url':arg}

    titles = fk_data_soup.find_all('title')
    if len(titles) > 0:
        book_data['title'] = titles[0].contents[0]

    meta_kw = fk_data_soup.find_all('meta',attrs={"name" : "Keywords"})

    if len(meta_kw) > 0:
        book_data['keywords'] = meta_kw[0]['content']

    meta_descr = fk_data_soup.find_all('meta',attrs={"name" : "Description"})

    if len(meta_descr) > 0:
        book_data['description'] = meta_descr[0]['content']

    meta_ogtitle = fk_data_soup.find_all('meta',attrs={"name" : "og_title"})

    if len(meta_ogtitle) > 0:
        book_data['og_title'] = meta_ogtitle[0]['content']
        

    meta_ogimage = fk_data_soup.find_all('meta',attrs={"name" : "og_image"})

    if len(meta_ogimage) > 0:
        book_data['og_image'] = meta_ogimage[0]['content']


    meta_ogurl = fk_data_soup.find_all('meta',attrs={"name" : "og_url"})

    if len(meta_ogurl) > 0:
        book_data['og_url'] = meta_ogurl[0]['content']

    print book_data
    book_data['_id'] = str(uuid4().int)
    fk_index_coll.insert(book_data)
    '''

'''
threads  = []
tp = ThreadPool(max_workers=120)

for elem in  smap_soup.find_all('loc'):
    if fk_index_coll.find_one({"smap_url":elem.contents[0]}) is not None:
        print 'already in db',elem.contents[0]
        continue
    tp.add_job(threaded_function,[elem.contents[0]])
'''

class Scrapper:
    def __init__(self,st_data):
        self.store_data = st_data
        self.url = ''
        self.domain = ''

    def scrap(self,url):
        if url is not None and len(url) > 0:
            url_info = tldextract.extract(url)
            self.domain = url_info.domain
            print '[INFO] Scrapper::scrap, domain',self.domain
            tp = ThreadPool(max_workers=120)
            tp.add_job(threaded_function,[self.domain,url])            
        else:
            print '[ALARM] Scrapper:scrap, invalid url'
