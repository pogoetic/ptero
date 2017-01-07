
import requests, boto3, sqlite3, json, ConfigParser, uuid, time, datetime
from dateutil.parser import parse
from dateutil.relativedelta import *
from dateutil import tz

#Keys
config = ConfigParser.RawConfigParser(allow_no_value=True)
config.read('keys.cfg')
iatakey = config.get("API", "iatakey")
geocodekey = config.get("API", "geocodekey")

conn = sqlite3.connect('pterodb')
data_resetflag = False

def data_reset(reset=False):
	if reset == True:
		c = conn.cursor()
		table = ['cities']
		for t in table:
			if table_exists(t) == True:
				print 'Dropping Table {}'.format(t)
				c.execute('Drop Table {}'.format(t))
		c.execute('VACUUM;')
		conn.commit()
		print 'Data Reset Success'

def table_exists(tablename):
	c = conn.cursor()
	command = 'SELECT count(*) FROM sqlite_master WHERE type=\'table\' AND name=\'{}\';'.format(tablename)
	c.execute(command)
	row = c.fetchone()
	if row[0] != 0:
		return True
	else:
		return False

def iata_city_refresh(apikey=iatakey, force=False):
	headers = {'content-type': 'application/json'}
	url = ' https://iatacodes.org/api/v6/cities?api_key={}'.format(apikey)
	c = conn.cursor()
	c.execute('Select count(*) from cities')
	numrows = c.fetchone()
	if numrows[0] != 0:
		c.execute('Select max(created) from cities')
		maxdate = c.fetchone()
		maxdate = parse(maxdate[0])
		if force == True:
			comparedate = maxdate+relativedelta(weeks=-1) #or (months=+1)
		else: 
			comparedate = maxdate+relativedelta(weeks=+1) #or (months=+1)
		today = datetime.datetime.utcnow()	
		if today > comparedate:
			r = requests.post(url, headers=headers) #API Call and refill table data
			if r.status_code == 200:
				print str(r.status_code) +' - Success!'
				response = r.json()
				c = conn.cursor()
				#Make this an update new instead of truncate/insert
				c.execute('Delete From cities;')
				c.execute('VACUUM;')
				for i in response['response']:
					c.execute('Insert Into cities(code, name, country_code) values(?,?,?)',(i['code'],i['name'],i['country_code']))
				conn.commit()
			else: 
				print str(r.status_code) + ' - ERROR!'
		else:
			print 'IATA cities -> its not time for an update yet'
	else: 
		r = requests.post(url, headers=headers) #API Call and refill table data
		if r.status_code == 200:
			print str(r.status_code) +' - Success!'
			response = r.json()
			c = conn.cursor()
			for i in response['response']:
				c.execute('Insert Into cities(code, name, country_code) values(?,?,?)',(i['code'],i['name'],i['country_code']))
			conn.commit()
		else: 
			print str(r.status_code) + ' - ERROR!'
	return None

############################################################################
#DB Setup
data_reset(reset=data_resetflag)

c = conn.cursor()
if table_exists('cities') == False:
	command = 'Create Table IF NOT EXISTS cities(code varchar(3) PRIMARY KEY, name varchar(100), country_code varchar(2), lat Decimal(9,6), long Decimal(9,6), created DATETIME DEFAULT (DATETIME(\'now\')))'
	c.execute(command)
	conn.commit()
	print 'Table cities Created!'

############################################################################


iata_city_refresh(force=False)
#Now Add Google API for Lat/Long of each City
#9368 cities but 2500 per day limit and 50 per second limit. Will need to save existing lat/long so as not to requery each week. 

c = conn.cursor()
c.execute("Select * from cities where lat IS NULL")
rows = c.fetchall()

#for r in rows:
	# print r
# exit()


headers = {'content-type': 'application/json'}
i = 1
for r in rows:
	if i<=2300: #2450
		code = r[0]
		city = r[1].encode("utf8")
		city = city.translate(None, "'")
		country = r[2]
		url = 'https://maps.googleapis.com/maps/api/geocode/json?address={}&components=country:{}&key={}'.format(city,country,geocodekey)
		print code +' - '+url
		r = requests.post(url, headers=headers)
		if r.status_code == 200:
			response = r.json()
			lat = response['results'][0]['geometry']['location']['lat']
			lng = response['results'][0]['geometry']['location']['lng']
			c.execute("Update cities Set lat={},long={} Where name = '{}' AND country_code = '{}'".format(lat,lng,city,country))
			conn.commit()
		else: 
			print str(r.status_code) + ' - ERROR!'

		time.sleep(0.1) #no more than 10 requests per second
		i=i+1
	else:
		break 

print '{} rows Done!'.format(i-1)


conn.close
