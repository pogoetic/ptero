
import requests, boto3, sqlite3, json

#Keys

conn = sqlite3.connect('pterodb')
dataresetflag = False

def TableExists(tablename):
	c = conn.cursor()
	command = 'SELECT count(*) FROM sqlite_master WHERE type=\'table\' AND name=\'{}\';'.format(tablename)
	c.execute(command)
	row = c.fetchone()
	if row[0] != 0:
		return True
	else:
		return False

def DataReset(reset=False):
	if reset == True:
		c = conn.cursor()
		if TableExists('apilimit') == True:
			print 'Dropping Table apilimit'
			c.execute('Drop Table apilimit')
		if TableExists('apihistory') == True:
			print 'Dropping Table apihistory'
			c.execute('Drop Table apihistory') 
		if TableExists('qbxresponse') == True:
			print 'Dropping Table qbxresponse'
			c.execute('Drop Table qbxresponse') 
		c.execute('VACUUM;')
		conn.commit()
		print 'Data Reset Success'

def UpdateAPIHistory(apiID,numcalls,reset=False):
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

def APILimitReached(apiID):
	c = conn.cursor()
	c.execute('Select numcalls from apihistory where apiID={} and date = date(\'now\',\'localtime\')'.format(apiID))
	numcalls = c.fetchone()
	c.execute('Select dailylimit from apilimit where apiID={}'.format(apiID))
	dailylimit = c.fetchone()
	if numcalls >= dailylimit:
		return True
	else: 
		return False

def UpdateQBXResponse(rawresponse):
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
DataReset(reset=dataresetflag)

c = conn.cursor()
if TableExists('apilimit') == False:
	#c.execute('Drop Table apilimit')
	c.execute('Create Table IF NOT EXISTS apilimit(apiID int,apicode varchar(20),apiname varchar(100),dailylimit int)')
	command = 'Insert Into apilimit(apiID, apicode, apiname, dailylimit) Values({},\'{}\',\'{}\',{})'.format('1','QPX','Google QPX Express API','50')
	c.execute(command)
	conn.commit()

if TableExists('apihistory') == False:
	#c.execute('Drop Table apihistory') 
	c.execute('Create Table IF NOT EXISTS apihistory(apiID int, date date, numcalls int)')
	c.execute('CREATE UNIQUE INDEX {ix} on {tn}({cn},{cn2})'.format(ix='idx1', tn='apihistory', cn='apiID', cn2='date'))
	conn.commit()

#c.execute('Drop Table qbxresponse') 
#conn.commit()

if TableExists('qbxresponse') == False:
	#c.execute('Drop Table apihistory') 
	c.execute('Create Table IF NOT EXISTS qbxresponse(queryid INTEGER PRIMARY KEY, rawresponse BLOB, created datetime DEFAULT CURRENT_TIMESTAMP)')
	conn.commit()

############################################################################
#User Settings

#QPX Requests 
def QPXSearch(jsonquery,apikey=qpxkey):
	if APILimitReached(apiID=1) == False:
		headers = {'content-type': 'application/json'}
		url = 'https://www.googleapis.com/qpxExpress/v1/trips/search?key={}'.format(apikey)
		r = requests.post(url, headers=headers, data=jsonquery)
		if r.status_code == 200:
			print str(r.status_code) +' - Success!'
			UpdateAPIHistory(apiID=1,numcalls=1)
			UpdateQBXResponse(r.text)
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
      "childCount": "0",
      "infantInLapCount": "0",
      "infantInSeatCount": "0",
      "seniorCount": "0"
    },
    "slice": [
      {
        "origin": "PHL",
        "destination": "SFO",
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
          "KC"
        ]
      }
    ],
    #"maxPrice": string,
    "saleCountry": "US",
    #"ticketingCountry": string,
    #"refundable": boolean,
    "solutions": "1"
  }
}

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


print json.dumps(jsonbody)
parsedjson = json.loads(json.dumps(jsonbody))
#print parsedjson
#print parsedjson['request']['slice'][0]['origin']

############################################################################
#QPX Calls
#response = QPXSearch(json.dumps(jsonbody))


c.execute('Select * from apihistory where date = date(\'now\',\'localtime\')')
rows = c.fetchall()
print rows

#c.execute('Select * from qbxresponse where substr(created,0,11) = date(\'now\',\'localtime\')')
#rows = c.fetchall()
#for row in rows:
#	print '\n'+ str(row) +'\n'

#Parse QBX Response
	#Grab the actual response in the future rather than querying DB
c.execute('Select rawresponse from qbxresponse where substr(created,0,11) = date(\'now\',\'localtime\')')
row = c.fetchone()
r = json.loads(row[0])
print r['kind']

'''
#Parsing JSON from Response directly
x = json.loads(response)
print type(x)
print x['kind']
'''


conn.close




