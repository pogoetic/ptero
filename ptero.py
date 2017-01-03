
import requests, boto3, sqlite3, json, ConfigParser

#Keys
config = ConfigParser.RawConfigParser(allow_no_value=True)
config.read('keys.cfg')
qpxkey = config.get("API", "qpxkey")
skyscannerkey = config.get("API", "skyscannerkey")
seskey = config.get("API", "seskey")

conn = sqlite3.connect('pterodb')
data_resetflag = False

def table_exists(tablename):
	c = conn.cursor()
	command = 'SELECT count(*) FROM sqlite_master WHERE type=\'table\' AND name=\'{}\';'.format(tablename)
	c.execute(command)
	row = c.fetchone()
	if row[0] != 0:
		return True
	else:
		return False

def data_reset(reset=False):
	if reset == True:
		c = conn.cursor()
		if table_exists('apilimit') == True:
			print 'Dropping Table apilimit'
			c.execute('Drop Table apilimit')
		if table_exists('apihistory') == True:
			print 'Dropping Table apihistory'
			c.execute('Drop Table apihistory') 
		if table_exists('qbxresponse') == True:
			print 'Dropping Table qbxresponse'
			c.execute('Drop Table qbxresponse') 
		c.execute('VACUUM;')
		conn.commit()
		print 'Data Reset Success'

def update_api_history(apiID,numcalls,reset=False):
	c = conn.cursor()	
	if reset == True:
		c.execute('Delete from apihistory;')
		c.execute('VACUUM;')
		conn.commit()
		return False

	command = 'Select count(*) from apihistory where apiID = {} and date = date(\'now\',\'localtime\')'.format(apiID)
	c.execute(command)
	row = c.fetchone()
	if row[0] == 0:
		command = 'Insert Into apihistory(apiID,date,numcalls) Values({},date(\'now\',\'localtime\'),{})'.format(apiID,numcalls)
		c.execute(command)
		conn.commit()
		return True
	else:
		command = 'Select numcalls from apihistory where apiID = {} and date = date(\'now\',\'localtime\')'.format(apiID)
		c.execute(command)
		row = c.fetchone()
		command = 'Update apihistory Set numcalls = {} where apiID = {} and date = date(\'now\',\'localtime\')'.format(row[0]+numcalls,apiID)
		c.execute(command)
		conn.commit()
		return True

def api_limit_reached(apiID):
	c = conn.cursor()
	c.execute('Select numcalls from apihistory where apiID={} and date = date(\'now\',\'localtime\')'.format(apiID))
	numcalls = c.fetchone()
	c.execute('Select dailylimit from apilimit where apiID={}'.format(apiID))
	dailylimit = c.fetchone()
	if numcalls >= dailylimit:
		return True
	else: 
		return False

def update_qbx_response(rawresponse):
	c = conn.cursor()	
	#command = "Insert Into qbxresponse(rawresponse) values(\'{}\');".format(rawresponse)
	c.execute("Insert Into qbxresponse(rawresponse) values(?)",(rawresponse,))
	conn.commit()
	command = 'Select * from qbxresponse order by queryid desc LIMIT 1'
	c.execute(command)
	row = c.fetchone()
	return row[0]

############################################################################
#DB Setup
data_reset(reset=data_resetflag)

c = conn.cursor()
if table_exists('apilimit') == False:
	#c.execute('Drop Table apilimit')
	c.execute('Create Table IF NOT EXISTS apilimit(apiID int,apicode varchar(20),apiname varchar(100),dailylimit int)')
	command = 'Insert Into apilimit(apiID, apicode, apiname, dailylimit) Values({},\'{}\',\'{}\',{})'.format('1','QPX','Google QPX Express API','50')
	c.execute(command)
	conn.commit()

if table_exists('apihistory') == False:
	#c.execute('Drop Table apihistory') 
	c.execute('Create Table IF NOT EXISTS apihistory(apiID int, date date, numcalls int)')
	c.execute('CREATE UNIQUE INDEX {ix} on {tn}({cn},{cn2})'.format(ix='idx1', tn='apihistory', cn='apiID', cn2='date'))
	conn.commit()

#c.execute('Drop Table qbxresponse') 
#conn.commit()

if table_exists('qbxresponse') == False:
	#c.execute('Drop Table apihistory') 
	c.execute('Create Table IF NOT EXISTS qbxresponse(queryid INTEGER PRIMARY KEY, rawresponse BLOB, created datetime DEFAULT CURRENT_TIMESTAMP)')
	conn.commit()

############################################################################
#User Settings

#QPX Requests 
def QPXSearch(jsonquery,apikey=qpxkey):
	if api_limit_reached(apiID=1) == False:
		headers = {'content-type': 'application/json'}
		url = 'https://www.googleapis.com/qpxExpress/v1/trips/search?key={}'.format(apikey)
		r = requests.post(url, headers=headers, data=jsonquery)
		if r.status_code == 200:
			print str(r.status_code) +' - Success!'
			update_api_history(apiID=1,numcalls=1)
			update_qbx_response(r.text)
			return r.json()
		else: 
			print str(r.status_code) + ' - Failure!'
			return None
	else:
		print 'API Limit Reached!'
		return None

jsonbody = {
  "request": {
    "passengers": {
      "adultCount": "2",
      "childCount": "1",
      "infantInLapCount": "1",
      "infantInSeatCount": "1",
      "seniorCount": "1"
    },
    "saleCountry": "US",
    "solutions": 2,
    "slice": [
      {
        "origin": "PHL",
        "destination": "LAX",
        "date": "2017-03-01",
        #"maxStops": integer,
        #"maxConnectionDuration": integer,
        "preferredCabin": "Coach", #COACH, PREMIUM_COACH, BUSINESS, and FIRST
        #"permittedDepartureTime": {
        #  "earliestTime": string,
        #  "latestTime": string
        #},
        #"permittedCarrier": [
        #  string
        #],
        #"alliance": string, #ONEWORLD, SKYTEAM, and STAR. Do not use this field with permittedCarrier 
        "prohibitedCarrier": [
          "KC","NK"
        ],
        "preferredCabin": "COACH"
      }
    ],
    #"maxPrice": string,
    "saleCountry": "US"
    #"ticketingCountry": string,
    #"refundable": boolean,
    #"solutions": "1"
  }
}

'''
jsonbody = {
 "request": {
  "passengers": {
   "adultCount": 2
  },
  "saleCountry": "US",
  "solutions": 1,
  "slice": [
   {
    "date": "2017-03-01",
    "origin": "PHL",
    "destination": "LAX",
    "prohibitedCarrier": [
     "KC"
    ],
    "preferredCabin": "COACH"
   }
  ]
 }
}
'''

print '\n'
print json.dumps(jsonbody)
parsedjson = json.loads(json.dumps(jsonbody))
#print parsedjson
#print parsedjson['request']['slice'][0]['origin']

############################################################################
#QPX Calls
#response = QPXSearch(json.dumps(jsonbody))

print '\n'
c.execute('Select * from apihistory where date = date(\'now\',\'localtime\')')
rows = c.fetchall()
print rows
print '\n'

#c.execute('Select * from qbxresponse where substr(created,0,11) = date(\'now\',\'localtime\')')
#rows = c.fetchall()
#for row in rows:
#	print '\n'+ str(row) +'\n'

#Parse QBX Response
	#Grab the actual response in the future rather than querying DB
c.execute('Select rawresponse from qbxresponse where substr(created,0,11) = date(\'now\',\'localtime\') order by created desc')
row = c.fetchone()
r = json.loads(row[0])
print 'requestId: ' + r['trips']['requestId'] +'\n'

x=0
for to in r['trips']['tripOption']:
	print 'tripOption {}: '.format(x)
	print 'price: ' + to['saleTotal'][3:]
	print 'currency: ' + to['saleTotal'][:3]

	print 'tripOptionId: ' + to['id']
	print 'total flight duration: ' + str(to['slice'][0]['duration'])

	print 'segmentId: ' + to['slice'][0]['segment'][0]['id']
	print 'segment carrier: ' + str(to['slice'][0]['segment'][0]['flight']['carrier'])
	print 'segment flight number: ' + str(to['slice'][0]['segment'][0]['flight']['number'])
	print 'cabin: ' + to['slice'][0]['segment'][0]['cabin']
	print 'bookingCode: ' + to['slice'][0]['segment'][0]['bookingCode']
	print 'bookingCodeCount: ' + str(to['slice'][0]['segment'][0]['bookingCodeCount'])
	print 'marriedSegmentGroup: ' + to['slice'][0]['segment'][0]['marriedSegmentGroup']

	for l in to['slice'][0]['segment'][0]['leg']:
		print 'legId: ' + l['id']
		print 'aircraft: ' + l['aircraft']
		print 'arrivaltime: ' + l['arrivalTime']
		print 'departuretime: ' + l['departureTime']
		print 'origin: ' + l['origin']
		print 'destination: ' + l['destination']
		print 'duration: ' + str(l['duration'])
		print 'mileage: ' + str(l['mileage'])

	print 'fareId: ' + to['pricing'][0]['fare'][0]['id']
	print 'fare basisCode: ' + to['pricing'][0]['fare'][0]['basisCode']
	for p in to['pricing']:
		try:
			print 'adultCount: ' + str(p['passengers']['adultCount'])
		except:
			pass
		try:
			print 'seniorCount: ' + str(p['passengers']['seniorCount'])
		except:
			pass
		try:
			print 'childCount: ' + str(p['passengers']['childCount'])
		except:
			pass
		try:
			print 'infantInSeatCount: ' + str(p['passengers']['infantInSeatCount'])
		except:
			pass
		try:
			print 'infantInLapCount: ' + str(p['passengers']['infantInLapCount'])
		except:
			pass
		#print 'baseFareTotal: ' + p['baseFareTotal'][3:]
		#print 'saleTaxTotal: ' + p['saleTaxTotal'][3:]
		#print 'saleTotal: ' + p['saleTotal'][3:]
	print 'latestTicketingTime: ' +to['pricing'][0]['latestTicketingTime']

	print '\n'
	x=x+1


'''
#Parsing JSON from Response directly
x = json.loads(response)
print type(x)
print x['kind']
'''


conn.close




