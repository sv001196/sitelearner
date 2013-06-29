import json
import requests
from xlrd import open_workbook
import sys
import simplejson

import requests
from requests import *

import os
from django.utils.encoding import smart_str, smart_unicode

import pprint

from uuid import uuid4
import pymongo
from pymongo import *

from bs4 import BeautifulSoup
import json
import requests
from xlrd import open_workbook
import sys
import simplejson

import requests
from requests import *

import tldextract

import urllib2

from bs4 import BeautifulSoup

import random

import scrapper
from scrapper import *

#traverse the schema, get the key table names [category, sub-category and item] and their properties [table fields]

class Importer:

    def __init__(self,file_name,schema_file_name):
        print '++ Importer::__init__'
        self.file_name = file_name
        self.schema_file_name = schema_file_name
        self.store_name = ''
        self.url = ''
        self.image_url = ''
        self.schema = ''
        self.soup = None

        self.col_mapping = {}
        self.fields = {}
        self.colid_name_map = {}
        self.values = {}
        self.filters = []
        self.store_doc = None
        self.store_id = None
        self.store_data = []

        self.category_coll = None
        self.subcategory_coll = None
        self.tt_db = None
        self.stores_coll = None
        self.item_coll = None        
        self.unique_key = ""
        print '-- Importer::__init__'

    def addCategory(self,category_name):
        cat_id = uuid4().int
        self.category_coll.insert({"_id":str(cat_id),
                                   "name":category_name,
                                   "subcategories":[]
                                   })
        return cat_id

    def addSubCategory(self,subcategory_name,category_id):
        subcat_id = uuid4().int

        cat_doc = self.category_coll.find_one({"_id":str(category_id)})
        if cat_doc is not None and 'subcategories' in cat_doc:
            cat_doc['subcategories'].append(str(subcat_id))
            self.category_coll.update({"_id":str(cat_doc["_id"])},cat_doc,True)

            self.subcategory_coll.insert({"_id":str(subcat_id),
                                          "name":subcategory_name,
                                          "category_id":str(category_id)
                          })
        return subcat_id

    def addFilters(self):
        if len(self.filters) > 0:
            self.store_doc["filters"] = self.filters
            if 'url' in self.store_doc:
                existing_store_filter = self.stores_coll.find_one({"url":self.store_doc['url']})
                if existing_store_filter is not None:
                    self.stores_coll.update({"_id":existing_store_filter["_id"]},self.store_doc,True)
                else:
                    self.stores_coll.update({"_id":self.store_doc["_id"]},self.store_doc,True)
            else:
                self.stores_coll.update({"_id":self.store_doc["_id"]},self.store_doc,True)
    
    def categoryId(self,category_name):
        print 'categoryId',self.category_coll
        cat_doc = self.category_coll.find_one({"name":category_name})
        if cat_doc is None:
            cat_id = self.addCategory(category_name)
            return cat_id
        return cat_doc['_id']

    def subcategoryId(self,subcategory_name,category_id):
        subcat_doc = self.subcategory_coll.find_one({"name":subcategory_name})
        if subcat_doc is None:
            subcat_id = self.addSubCategory(subcategory_name,category_id)
            return subcat_id
        return subcat_doc['_id']

    def ttcategoryId(self,category_name):
        cat_doc = Connection()['teritree']['categories'].find_one({"category":category_name})
        if cat_doc is not None:
            return cat_doc['_id']

        return None

    def ttsubcategoryId(self,category_name):
        cat_doc = Connection()['teritree']['subcategories'].find_one({"subcategory":category_name})
        if cat_doc is not None:
            return cat_doc['_id']

        return None


    def getPossibleValues(self,tag):
        #print '++ getPossibleValues'
        children = tag.findChildren()
        #print  'getPossibleValues, children len',tag.name,children
        for child in children:
            if child.name == "values":
                values = child.findChildren()
                for value in values:
                    #print 'value',value.name,'of',child.parent["column_name"]
                    if child.parent["column_name"] in self.values:
                        self.values[value.parent.parent["column_name"]][value['name']] = value.name
                    else:
                        self.values[value.parent.parent["column_name"]]= {value['name']:value.name}

        #print '-- getPossibleValues'
        

    def populateColumnMapping(self):
        #print '++ populateColumnMapping'
        columns = self.soup.find_all(column_name=True)

        for column in columns:

            

            if len(column["column_name"]) > 0:
                all_unique_keys = self.soup.find_all("",{"unique":"1"})
                if len(all_unique_keys) > 0:
                    
                    self.unique_key = all_unique_keys[0].name
                    print 'UNIQUE',self.unique_key
            
                self.col_mapping[column["column_name"]] = None
                self.fields[column["column_name"]] = column.name
                self.getPossibleValues(column)            
                #print 'calling filter',column.name
                    
                #print 'after getting poss values of',column.name,self.values

        #print '-- populateColumnMapping',self.fields

    def readColumnIds(self):
        print '++ readColumnIds',self.file_name
        wb =  open_workbook(self.file_name)    
        #print '[INFO] readColumnIds, number of work sheets',wb.nsheets
        cat_sheet = wb.sheet_by_name('catalogue')
        
        if cat_sheet.nrows > 0 and cat_sheet.ncols:
            for colidx in range(cat_sheet.ncols):
                cell = cat_sheet.cell(0,colidx).value
                if cell in self.fields:
                    self.col_mapping[cell] = colidx
                    self.colid_name_map[colidx] = cell

        #print 'col map readColumnIds',self.col_mapping
        #print 'col id name map readColumnIds',self.colid_name_map
        #print '-- readColumnIds'

    
    def process_data(self):
        print '++ process_data',self.values
        print '[INFO] process_data',self.filters

        wb =  open_workbook(self.file_name)    
        print '[INFO] process_data, number of work sheets',wb.nsheets
        cat_sheet = wb.sheet_by_name('catalogue')
        for rowidx in range(cat_sheet.nrows):
            row_data = {}
            for colidx in range(cat_sheet.ncols):
                cell = cat_sheet.cell(rowidx,colidx).value
                if rowidx != 0:
                    if colidx in self.colid_name_map:
                        #colid_name_map[colidx]
                        if self.colid_name_map[colidx] in self.values:
                            if cell in self.values[self.colid_name_map[colidx]]:
                                row_data[self.fields[self.colid_name_map[colidx]]] = self.values[self.colid_name_map[colidx]][cell]
                            else:
                                row_data[self.fields[self.colid_name_map[colidx]]] = cell
                        else:
                            row_data[self.fields[self.colid_name_map[colidx]]] = cell
                        
            if rowidx != 0:
                self.store_data.append(row_data)            

        print '[INFO] process_data, unqiue keys',self.unique_key

        self.store_data = sorted(self.store_data,key=lambda x: x['url'], reverse=True)
        print '-- process_data'
    
    def traverse_schema(self):
        print '++ traverse_schema'
        f = open(self.schema_file_name,'r')
        self.schema = f.read()
        self.soup = BeautifulSoup(self.schema)
        self.populateColumnMapping()
        self.readColumnIds()
        self.process_data()
        f.close()

        #print 'found category', self.soup.find_all('category')
        #print 'found subcategory', self.soup.find_all('subcategory')
        #print 'found item', soup.find_all('item')
        print '-- traverse_schema'



print 'number of arguments',len(sys.argv)
if len(sys.argv) > 1:
    for i in range(1,len(sys.argv)):
        if 'schema:' in sys.argv[i]:
            schema_file_name = sys.argv[i].lstrip("schema:")       
        if 'file:' in sys.argv[i]:
            file_name = sys.argv[i].lstrip("file:")

    print file_name,schema_file_name

    isOk = True

    if len(file_name) == 0:
        print 'filename option is missing'
        isOk = False


    imp = Importer(file_name,schema_file_name)
    if isOk is True:
        imp.traverse_schema()
        if len(imp.store_data) > 0:
            Scrapper(imp.store_data).scrap('http://www.basicslife.com')
            
else:
    print 'supported options are file, schema'
