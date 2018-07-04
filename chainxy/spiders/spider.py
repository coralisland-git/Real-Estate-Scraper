# from __future__ import unicode_literals
import scrapy

import json

import csv

import os

import scrapy

from scrapy.spiders import Spider

from scrapy.http import FormRequest

from scrapy.http import Request

from chainxy.items import ChainItem

from selenium import webdriver

from pyvirtualdisplay import Display

import random

from lxml import etree

import time

import MySQLdb

import sys

import pdb


class spider(scrapy.Spider):

	name = 'spider'

	domain = 'https://www.agentcloud.com'

	proxy_list = []

	choice = ''

	def __init__(self):

		# self.driver = webdriver.Chrome("./chromedriver")

		script_dir = os.path.dirname(__file__)

		file_path = script_dir + '/data/proxy list.txt'

		with open(file_path, 'rb') as text:

			content = text.readlines()

		for proxy in content :

			proxy = proxy.replace('\n', '')

			proxy = 'http://' + proxy

			self.proxy_list.append(proxy)


		self.headers = [

			"folio", "pin", "owner", "owner_addr", "owner_city", "owner_state", "owner_zip", "site_addr", "site_city", "site_state", "site_zip", "bed", "bath", "square_footage", "zestimate", "active", "icomps", "redfin", "trulia", "movoto", "realtor"

		]

		db = MySQLdb.connect(host="localhost", user="root", passwd="root", db="zillow_db")       

		self.cur = db.cursor()

	def start_requests(self):

		yield scrapy.Request(self.domain, callback=self.parse_case,  meta={'proxy' : random.choice(self.proxy_list) }, dont_filter=True)

		# yield scrapy.Request(url=self.domain, callback=self.parse_case)

		# url = "https://parser-external.geo.moveaws.com/suggest?input=858+TIMBER+POND+DR&limit=10&client_id=rdcV8"

		# yield scrapy.Request(url, callback=self.parse_realtor,  method="get", dont_filter=True)


	def parse_case(self, response):

		print("""
					
		--------- Options ---------

			0 : Csv

			1 : Zillow

			2 : Icomps

			3 : Redfin

			4 : Trulia

			5 : Movoto

			6 : Realtor
		---------------------------
					""")

		self.choice = raw_input(' Select : ')

		try:

			self.choice = int(self.choice)

		except:

			self.choice = -1


		if self.choice == 0:

			yield scrapy.Request(self.domain, callback=self.parse_csv, meta={'proxy' : random.choice(self.proxy_list) }, dont_filter=True)

		elif self.choice == 1:

			sql = "select * from parcel_estimate where zestimate is NULL or zestimate='0' or zestimate='' or bed='' or bed is NULL or bath='' or bath is NULL or square_footage='' or square_footage is NULL"

			self.cur.execute(sql)

			rows = self.cur.fetchall()

			for row in rows:

				item = ChainItem()

				for ind in range(0, len(row)-1):

					item[self.headers[ind].title()] = row[ind+1]

				url  = 'https://www.zillow.com/homes/'+item['Site_Addr'].replace(' ', '+')+',-'+item['Site_City'].replace(' ', '-')+',-'+item['Site_State']+'-'+item['Site_Zip']+'_rb/'

				yield scrapy.Request(url= url, callback=self.parse_zillow, meta={'item' : item, 'url' : url, 'proxy' : random.choice(self.proxy_list) }, dont_filter=True) 

		elif self.choice == 2:

			sql = "select * from parcel_estimate where icomps is NULL or icomps='0' or icomps=''"

			self.cur.execute(sql)

			rows = self.cur.fetchall()

			for row in rows:

				item = ChainItem()

				for ind in range(0, len(row)-1):

					item[self.headers[ind].title()] = row[ind+1]

				url = 'https://icomps.com/'

				headers = {

					'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',

					'Accept-Encoding':'gzip, deflate, br',

					'Content-Type':'multipart/form-data; boundary=----WebKitFormBoundaryEA6WIdnFYAG0Pxrw',

					'Upgrade-Insecure-Requests':'1'

				}

				formdata = {

					'address' : item['Site_Addr'] + ', ' + item['Site_City'] + ', ' + item['Site_State']

				}

				yield scrapy.FormRequest(url, callback=self.parse_icomps, formdata=formdata , method='POST', meta={ 'item' : item, 'proxy' : random.choice(self.proxy_list)}, dont_filter=True)

		elif self.choice == 3:

			sql = "select * from parcel_estimate where redfin is NULL or redfin='0' or redfin=''"

			self.cur.execute(sql)

			rows = self.cur.fetchall()

			for row in rows:

				item = ChainItem()

				for ind in range(0, len(row)-1):

					item[self.headers[ind].title()] = row[ind+1]

				url = 'https://www.redfin.com/stingray/do/location-autocomplete?location='+item['Site_Addr']+'&start=0&count=10&v=2&market=social&al=1&iss=false&ooa=true'

				yield scrapy.Request(url, callback=self.parse_redfin, meta={ 'item' : item , 'proxy' : random.choice(self.proxy_list) }, dont_filter=True)

		elif self.choice == 4:

			sql = "select * from parcel_estimate where trulia is NULL or trulia='0' or trulia=''"

			self.cur.execute(sql)

			rows = self.cur.fetchall()

			for row in rows:

				item = ChainItem()

				for ind in range(0, len(row)-1):

					item[self.headers[ind].title()] = row[ind+1]

				address = item['Site_Addr'] + ' ' + item['Site_City'] + ', ' + item['Site_State'] + ' ' + item['Site_Zip']

				
				header = {

					'accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',

					'accept-encoding':'gzip, deflate, br',

					'upgrade-insecure-requests':'1',

					'cookie':'tlftmusr=180606p9vyel0d0efj4vsgy66dq8w324; fvstts=20180605; _ga=GA1.2.1424644448.1528261873; s_fid=5202564B7E9D6A5D-10386B4355C92852; s_vi=[CS]v1|2D8BB67D0503044C-40001182000012FA[CE]; G_ENABLED_IDPS=google; QSI_S_ZN_aVrRbuAaSuA7FBz=v:0:0; __gads=ID=d6ea1fa12003c8a5:T=1528262252:S=ALNI_MY2dDo6YpnTbwqcV1PdO7rg6uipgQ; PHPSESSID=8opt7oaom461gl8esvkte9jt12; csrft=AQWUWAGkdRPH41%2B9XCbW3RWnt3Uq6KeGcwsaU2ZSVtY%3D; _sdsat_landing_page=https://www.trulia.com/p/fl/brandon/706-bloomingfield-dr-brandon-fl-33511--2035460054|1528712466756; _sdsat_session_count=2; _gid=GA1.2.391308738.1528712467; s_campaign=Direct%20Traffic; s_cc=true; fontsLoaded=1; _sdsat_traffic_source=https://www.trulia.com/p/fl/brandon/706-bloomingfield-dr-brandon-fl-33511--2035460054; _px=fRFBUvo7T6KGiiRvTFWH6Bu8RDTZpn2zawcTEfmaIwYFoCM/0h+5JZLY0j6RIIbcT4EZwZ6PZ52HfdxxfqZO/g==:1000:L0LAYNO73oaP8Py1Tv2xXApUBu2FjfUYQxl2nYSpWrNXL8M77qEHDHXWunEePY6hsZ8nLAu5f5xswS+Ns6nId6Les3fnrsJsSldrpAVQyrmEW7rEAEsJrLWwv8XUp4xLtmFV/ytGV5tkNBu9/NWIR3/KVj72kFvk3LxeXRPrWD/fVP2Wuq3mzsWmMxmeP/7eNJ+pvqLaJHmX1fp68o+PvbFJrSCCtU9+8rwT3JwQvzloo0/Oq3CPXxfrGiC1SaBb+nXQVA8bwh99wO/uWOUAtg==; tabc=%7B%22996%22%3A%22a%22%2C%22991%22%3A%22b%22%2C%22992%22%3A%22control%22%2C%221010%22%3A%22control%22%2C%22999%22%3A%22b%22%2C%22884%22%3A%22b%22%7D; s_sq=%5B%5BB%5D%5D; promptdata=%7B%22c%22%3A%7B%22pg-pdp%22%3A19%7D%2C%22pd%22%3A%5B%5D%2C%22pts%22%3Anull%7D; _sdsat_lt_pages_viewed=29; _sdsat_pages_viewed=18; _gat=1; trul_visitTimer=1528717766163_1528718620838; SERVERID=webfe338|Wx5lI'
				}

				url = "https://www.trulia.com/submit_search/?ac_entered_query=%s&bedFilter=&locationId&locationType=address&maxPrice=&search=%s&tst=h" %(address, address)

				yield scrapy.Request(url, callback=self.parse_trulia, method="get", meta={ 'item' : item, 'proxy' : random.choice(self.proxy_list) }, dont_filter=True)

		elif self.choice == 5:

			sql = "select * from parcel_estimate where movoto is NULL or movoto='0' or movoto=''"

			self.cur.execute(sql)

			rows = self.cur.fetchall()

			for row in rows:

				item = ChainItem()

				for ind in range(0, len(row)-1):

					item[self.headers[ind].title()] = row[ind+1]

				address = item['Site_Addr'].replace(' ', '-').lower()+'-'+item['Site_City'].replace(' ', '-').lower()+'-'+item['Site_State'].lower()
				
				url = "https://www.movoto.com/address-%s/" %address

				yield scrapy.Request(url, callback=self.parse_movoto, meta={ 'item' : item, 'proxy' : random.choice(self.proxy_list) }, dont_filter=True)

		elif self.choice == 6:

			sql = "select * from parcel_estimate where realtor is NULL or realtor='0' or realtor=''"

			self.cur.execute(sql)

			rows = self.cur.fetchall()

			for row in rows:

				item = ChainItem()

				for ind in range(0, len(row)-1):

					item[self.headers[ind].title()] = row[ind+1]

				address = item['Site_Addr'].replace(' ', '+').upper()

				url = "https://parser-external.geo.moveaws.com/suggest?input=%s&limit=10&client_id=rdcV8" %address

				yield scrapy.Request(url, callback=self.parse_realtor, meta={ 'item' : item, 'proxy' : random.choice(self.proxy_list), 'url' : url  }, dont_filter=True)


		else:

			print(' ~~~~ Warning! : invalid format ~~~')


	def parse_csv(self, response):

		count = 0

		script_dir = os.path.dirname(__file__)

		file_path = script_dir + '/data/HC_ALL_SFR.csv'

		with open(file_path, 'rb') as csvfile:

			spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')

			for row in spamreader:

				if count >= 1:

					item = ChainItem()

					for ind in range(0, len(row)):

						item[self.headers[ind].title()] = row[ind].replace('"', '')

					sql = "select * from parcel_estimate where folio=%s" %item['Folio']

					self.cur.execute(sql)

					rows = self.cur.fetchall()

					if len(rows) == 0:
				
						yield item

				count += 1


	def parse_zillow(self, response):

		if 'captcha' in response.url.lower():

			yield scrapy.Request(url=response.meta['url'], callback=self.parse_zillow, meta={'item' : response.meta['item'], 'url' : response.meta['url'], 'proxy' : random.choice(self.proxy_list)}, dont_filter=True) 

		else :

			try:

				item = response.meta['item']

				detail = self.eliminate_space(response.xpath('//header[@class="zsg-content-header addr"]/h3//span[@class="addr_bbs"]//text()').extract())

				item['Bed'] = detail[0].lower().replace('bed', '').replace('s', '').strip()

				item['Bath'] = float(detail[1].lower().replace('bath', '').replace('s', '').strip())

				item['Square_Footage'] = float(detail[2].lower().replace('sqft','').replace('s', '').strip().replace(',',''))

				try:

					market = self.validate(''.join(response.xpath('//div[contains(@class, "home-summary-row")]')[0].xpath('.//text()').extract()))

					if 'sale' in market.lower() or 'sold' in market.lower():

						item['Active'] = 'Yes'

					else : 

						item['Active'] = 'No'	

					if item['Active'] == 'Yes':

						item['Zestimate'] = self.validate(''.join(response.xpath('//div[contains(@class, "home-summary-row")]')[2].xpath('./span[2]//text()').extract())).replace(',', '').replace('$', '')

					else :

						item['Zestimate'] = self.validate(''.join(response.xpath('//div[contains(@class, "home-summary-row")]')[1].xpath('./span[2]//text()').extract())).replace(',','').replace('$', '')

				except :

					item['Active'] = ''

					item['Zestimate'] = ''

				yield item

			except :

				pass


	def parse_icomps(self, response):		

		if 'comparables' in response.body.lower() : 

			print('warning : Icomps.com went down. please try again later.')

		else:

			item = response.meta['item']

			try:

				item['Icomps'] = self.validate(''.join(response.xpath('//p[contains(@class, "green value")]//text()').extract()).split(':')[1].strip().replace('$', '').replace(',',''))

			except :

				item['Icomps'] = ''

			yield item


	def parse_redfin(self, response):

		item = response.meta['item']

		data = response.body.split('&&')

		if len(data) > 1:

			data = data[1]

		try:

			data = json.loads(data)

			matched_addr = data['payload']['exactMatch']['name']

			url = 'https://www.redfin.com'+data['payload']['exactMatch']['url']

			if item['Site_Addr'].lower() == matched_addr.lower():

				yield scrapy.Request(url=url, callback=self.parse_redfin_detail, meta={ 'item' : item, 'proxy' : random.choice(self.proxy_list) }, dont_filter=True)

		except :

			pass


	def parse_redfin_detail(self, response):

		item = response.meta['item']

		try:

			item['Redfin'] = self.validate(''.join(response.xpath('//div[@class="info-block avm"]//div[@class="statsValue"]//text()').extract()).replace('$', '').replace(',',''))

		except :

			item['Redfin'] = ''

		yield item


	def parse_trulia(self, response):

		try:

			item = response.meta['item']

			item['Trulia'] = self.validate(response.xpath('//div[contains(@class, "ptxsXxsVisible ptlSmlVisible")]//span[@data-role="price"]//text()').extract_first()).replace('$', '').replace(',','')

		except :

			item['Trulia'] = ''

		yield item


	def parse_movoto(self, response):

		movoto = self.validate(response.xpath('//div[@class="dpp-price"][1]//span//text()').extract_first()).replace('$', '').replace(',','')

		try:

			item = response.meta['item']

			item['Movoto'] = self.validate(response.xpath('//div[@class="dpp-price"][1]//span//text()').extract_first()).replace('$', '').replace(',','')

		except :

			item['Movoto'] = ''

		yield item


	def parse_realtor(self, response):

		item = response.meta['item']

		try:

			mpr_id = json.loads(response.body)['autocomplete'][0]['mpr_id']

			url = 'https://www.realtor.com/realestateandhomes-detail/M%s' %mpr_id

			yield scrapy.Request(url=url, callback=self.parse_realtor_detail, meta={ 'item' : item,  'url' : url, 'proxy' : random.choice(self.proxy_list) }, dont_filter=True)

		except :

			pass


	def parse_realtor_detail(self, response):

		if response.url == 'https://www.realtor.com/miscellaneous/userblocked':

			yield scrapy.Request(url=response.meta['url'], callback=self.parse_realtor_detail, meta={'item' : response.meta['item'], 'url' : response.meta['url'], 'proxy' : random.choice(self.proxy_list)}, dont_filter=True) 

		else:

			try:

				item = response.meta['item']

				item['Realtor'] = self.validate(response.xpath('//div[@class="ldp-header-price"]//span[@itemprop="price"]//text()').extract_first()).replace('$', '').replace(',','')

			except:

				item['Realtor'] = ''

			yield item


	def validate(self, item):

		try:

			return item.replace('\n', '').replace('\t','').replace('\r', '').strip()

		except:

			pass
			

	def eliminate_space(self, items):

		tmp = []

		for item in items:

			if self.validate(item) != '':

				tmp.append(self.validate(item))

		return tmp