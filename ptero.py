
import requests, sqlite3, json, ConfigParser, uuid

#Keys
config = ConfigParser.RawConfigParser(allow_no_value=True)
config.read('keys.cfg')
qpxkey = config.get("API", "qpxkey")
skyscannerkey = config.get("API", "skyscannerkey")
iatakey = config.get("API", "iatakey")
geocodekey = config.get("API", "geocodekey")

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
        table = ['apilimit','apihistory','qbxresponse','useraccount']
        for t in table:
            if table_exists(t) == True:
                print 'Dropping Table {}'.format(t)
                c.execute('Drop Table {}'.format(t))
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

    command = 'Select count(*) from apihistory where apiID = {} and date = date(\'now\')'.format(apiID)
    c.execute(command)
    row = c.fetchone()
    if row[0] == 0:
        command = 'Insert Into apihistory(apiID,date,numcalls) Values({},date(\'now\'),{})'.format(apiID,numcalls)
        c.execute(command)
        conn.commit()
        return True
    else:
        command = 'Select numcalls from apihistory where apiID = {} and date = date(\'now\')'.format(apiID)
        c.execute(command)
        row = c.fetchone()
        command = 'Update apihistory Set numcalls = {} where apiID = {} and date = date(\'now\')'.format(row[0]+numcalls,apiID)
        c.execute(command)
        conn.commit()
        return True

def api_limit_reached(apiID):
    c = conn.cursor()
    c.execute('Select numcalls from apihistory where apiID={} and date = date(\'now\')'.format(apiID))
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

def create_user_account(emailaddress):
    c = conn.cursor()   
    try:
        newuuid=uuid.uuid4()
        c.execute('Insert Into useraccount(useraccountid, emailaddress) Values(\'{}\',\'{}\')'.format(str(newuuid),emailaddress))
        c.execute('Select * from useraccount Order by created desc LIMIT 1')
        row = c.fetchone()
        conn.commit()
        return row[0]
    except:
        return None

def create_user_route(useraccountid,o_or_d,cityID,airportID):
    if o_or_d == 'o':
        table = 'userorigin'
        col = 'origincityid'
    elif o_or_d == 'd':
        table = 'userdestination'
        col = 'destinationcityid'
    else: 
        return None

    try:
        c = conn.cursor()   
        c.execute('Insert Into {}(useraccountid, {}, airportID) Values(\'{}\',{},{})'.format(table,col,useraccountid,cityID,airportID))
        conn.commit()
        c.execute('Select origininstanceID from {} where useraccountid = \'{}\' Order by created desc LIMIT 1'.format(table, useraccountid))
        row = c.fetchone()
        return row[0]

    except sqlite3.Error as er:
        #print 'er:', er.message
        return 'er:', er.message

def nearby_airports(citycode,distance=150,primaryairports=1,apikey=iatakey):
    #IATA Nearby Lookup
    c = conn.cursor()
    airports = []
    c.execute('Select cityID, lat, long from cities where code = \'{}\''.format(citycode))
    row = c.fetchone()
    cityid = row[0]
    lat = row[1]
    lng = row[2] 
    headers = {'content-type': 'application/json'}
    url = 'http://iatacodes.org/api/v6/nearby?lat={}&lng={}&distance={}&api_key={}'.format(lat,lng,distance,apikey)
    r = requests.post(url, headers=headers)
    if r.status_code == 200:
        response = r.json()
        for r in response['response']:
            #We filter on notairport IS NULL to ignore records that are not airports
            c.execute('Select * from airports where notairport IS NULL and [primary] = {} and code = \'{}\''.format(primaryairports,r['code']))
            row = c.fetchone()
            if row != None:
                airports.append(row[0])
        return cityid, airports

    else: 
        print str(r.status_code) + ' - ERROR!'
        return None


def qpx_search(jsonquery,apikey=qpxkey):
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

if __name__ == '__main__':

    ############################################################################
    #DB Setup
    data_reset(reset=data_resetflag)

    c = conn.cursor()
    if table_exists('apilimit') == False:
        #c.execute('Drop Table apilimit')
        c.execute('Create Table IF NOT EXISTS apilimit(apiID int,apicode varchar(20),apiname varchar(100),dailylimit int)')
        command = 'Insert Into apilimit(apiID, apicode, apiname, dailylimit) Values({},\'{}\',\'{}\',{})'.format('1','QPX','Google QPX Express API',50)
        c.execute(command)
        command = 'Insert Into apilimit(apiID, apicode, apiname, dailylimit) Values({},\'{}\',\'{}\',{})'.format('2','GEO','Google Maps Geocode API',2500)
        c.execute(command)
        conn.commit()

    if table_exists('apihistory') == False:
        #c.execute('Drop Table apihistory') 
        c.execute('Create Table IF NOT EXISTS apihistory(apiID int, date date, numcalls int)')
        c.execute('CREATE UNIQUE INDEX {ix} on {tn}({cn},{cn2})'.format(ix='IDX_apihistory', tn='apihistory', cn='apiID', cn2='date'))
        conn.commit()

    if table_exists('qbxresponse') == False:
        #c.execute('Drop Table qbxresponse')
        command = 'Create Table IF NOT EXISTS qbxresponse(queryid INTEGER PRIMARY KEY, rawresponse BLOB, created DATETIME DEFAULT (DATETIME(\'now\')))'
        c.execute(command)
        conn.commit()

    if table_exists('useraccount') == False:
        #c.execute('Drop Table useraccount')
        command = 'Create Table IF NOT EXISTS useraccount(useraccountid VARCHAR(36) PRIMARY KEY, emailaddress varchar(250), created DATETIME DEFAULT (DATETIME(\'now\')))'
        c.execute(command)
        conn.commit()
        create_user_account('pogster@gmail.com')

    if table_exists('userorigin') == False:
        command = 'Create Table IF NOT EXISTS userorigin(origininstanceID INTEGER PRIMARY KEY, useraccountid VARCHAR(36), origincityID, airportID, created DATETIME DEFAULT (DATETIME(\'now\')))'
        c.execute(command)
        command = 'CREATE UNIQUE INDEX {ix} on {tn}({cn},{cn2},{cn3})'.format(ix='IDX_userorigin', tn='userorigin', cn='useraccountid', cn2='origincityID', cn3='airportID')
        c.execute(command)
        conn.commit()

    if table_exists('userdestination') == False:
        command = 'Create Table IF NOT EXISTS userdestination(destinationinstanceID INTEGER PRIMARY KEY, useraccountid VARCHAR(36), destinationcityID, airportID, created DATETIME DEFAULT (DATETIME(\'now\')))'
        c.execute(command)
        command = 'CREATE UNIQUE INDEX {ix} on {tn}({cn},{cn2},{cn3})'.format(ix='IDX_userdestination', tn='userdestination', cn='useraccountid', cn2='destinationcityID', cn3='airportID')
        c.execute(command)
        conn.commit()

    ############################################################################
    #User Settings

    #User must input emailaddress and up to 1 Origin + 10 Destination cities to track
    #We will use Google Maps Geocode API to geocode a city to get Lat/Long
    #We will use IATACodes Naerby API to lookup the airports in those cities + nearby airports with 150 miles. 

    #create origins (must somehow enforce 1 per user at the beginning)
    cityid, airports = nearby_airports(citycode='PHL')
    for a in airports:
        create_user_route(useraccountid='9e6b6207-31a3-481e-b5e3-5754fdcd222a',o_or_d='o',cityID=cityid,airportID=a)
    
    #create destinations
    cityid, airports = nearby_airports(citycode='REK')
    for a in airports:
        create_user_route(useraccountid='9e6b6207-31a3-481e-b5e3-5754fdcd222a',o_or_d='d',cityID=cityid,airportID=a)
    

    ############################################################################


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
    print json.dumps(jsonbody, indent=4)
    parsedjson = json.loads(json.dumps(jsonbody))
    #print parsedjson
    #print parsedjson['request']['slice'][0]['origin']

    ############################################################################
    #QPX Calls
    #response = qpx_search(json.dumps(jsonbody))

    #Next we must construct a request based on stored user input, instead of hardcoding it

    print '\n'
    c.execute('Select * from apihistory where date = date(\'now\')')
    rows = c.fetchall()
    print rows
    print '\n'

    c.execute('Select * from useraccount')
    rows = c.fetchall()
    print rows
    print '\n'


    #c.execute('Select * from qbxresponse where substr(created,0,11) = date(\'now\')')
    #rows = c.fetchall()
    #for row in rows:
    #   print '\n'+ str(row) +'\n'

    #Parse QBX Response
        #Grab the actual response in the future rather than querying DB
    c.execute('Select rawresponse from qbxresponse where substr(created,0,11) = date(\'now\') order by created desc')
    row = c.fetchone()
    print json.dumps(json.loads(row[0]),indent=4)
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

    exit()



