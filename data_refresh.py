
import requests, boto3, sqlite3, json, ConfigParser, uuid
from datetime import *
from dateutil.relativedelta import *
from dateutil.parser import *
from dateutil.tz import *

#Keys
config = ConfigParser.RawConfigParser(allow_no_value=True)
config.read('keys.cfg')
iatakey = config.get("API", "iatakey")

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

def iata_city_refresh(apikey=iatakey):
	c = conn.cursor()
	c.execute('Select count(*) from cities')
	numrows = c.fetchone()
	if numrows[0] != 0:
		c.execute('Select max(created) from cities')
		maxdate = c.fetchone()
		maxdate = parse(maxdate[0])
		comparedate = maxdate+relativedelta(weeks=+1) #or (months=+1)
		today = datetime.utcnow()	
		if today > comparedate:
			#API Call and refill table data
			headers = {'content-type': 'application/json'}
			url = ' https://iatacodes.org/api/v6/cities?api_key={}'.format(apikey)
			r = requests.post(url, headers=headers)
			if r.status_code == 200:
				print str(r.status_code) +' - Success!'
				response = r.json()
				c = conn.cursor()
				for i in response['response']:
					c.execute('Insert Into cities(code, name, country_code) values(?,?,?)',(i['code'],i['name'],i['country_code']))
				conn.commit()
			else: 
				print str(r.status_code) + ' - ERROR!'
		else:
			print 'IATA cities -> its not time for an update yet'
	else: 
		#API Call and refill table data
		headers = {'content-type': 'application/json'}
		url = ' https://iatacodes.org/api/v6/cities?api_key={}'.format(apikey)
		r = requests.post(url, headers=headers)
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
	command = 'Create Table IF NOT EXISTS cities(code varchar(3) PRIMARY KEY, name varchar(100), country_code varchar(2), created DATETIME DEFAULT (DATETIME(\'now\')))'
	c.execute(command)
	conn.commit()
	print 'Table cities Created!'

############################################################################


iata_city_refresh()

