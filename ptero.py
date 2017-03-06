
import requests, sqlite3, json, ConfigParser, uuid, time, pyodbc
import datetime, dateutil.parser

#Keys
config = ConfigParser.RawConfigParser(allow_no_value=True)
config.read('keys.cfg')
qpxkey = config.get("API", "qpxkey")
skyscannerkey = config.get("API", "skyscannerkey")
iatakey = config.get("API", "iatakey")
geocodekey = config.get("API", "geocodekey")
connstring = "Driver={ODBC Driver 13 for SQL Server};Server=tcp:"+config.get("DB","server")+";DATABASE="+config.get("DB","database")+";UID="+config.get("DB","uname")+";PWD="+ config.get("DB","pwd")

#conn = sqlite3.connect('pterodb')
conn = pyodbc.connect(connstring)
data_resetflag = False

def table_exists(tablename):
    c = conn.cursor()
    #command = 'SELECT count(*) FROM sqlite_master WHERE type=\'table\' AND name=\'{}\';'.format(tablename)
    command = "SELECT count(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '{}'".format(tablename)
    c.execute(command)
    row = c.fetchone()
    if row[0] != 0:
        return True
    else:
        return False

def data_reset(reset=False):
    if reset == True:
        c = conn.cursor()
        table = ['apilimit','apihistory','qpxresponse','useraccount','userorigin',
                 'userdestination','qpxresponse','qpxtrip','qpxsegment','qpxleg',
                 'sksresponse','sksquotes','seasons']
        for t in table:
            if table_exists(t) == True:
                print 'Dropping Table {}'.format(t)
                c.execute('Drop Table {}'.format(t))
        #c.execute('VACUUM;')
        conn.commit()
        print 'Data Reset Success'

def update_api_history(apiID,numcalls,reset=False):
    c = conn.cursor()   
    if reset == True:
        c.execute('Delete from apihistory;')
        #c.execute('VACUUM;')
        conn.commit()
        return False

    command = 'Select count(*) from apihistory where apiID = {} and date = cast(CURRENT_TIMESTAMP as date)'.format(apiID)
    c.execute(command)
    row = c.fetchone()
    if row[0] == 0:
        command = 'Insert Into apihistory(apiID,date,numcalls) Values({},cast(CURRENT_TIMESTAMP as date),{})'.format(apiID,numcalls)
        c.execute(command)
        conn.commit()
        return True
    else:
        command = 'Select numcalls from apihistory where apiID = {} and date = cast(CURRENT_TIMESTAMP as date)'.format(apiID)
        c.execute(command)
        row = c.fetchone()
        command = 'Update apihistory Set numcalls = {} where apiID = {} and date = cast(CURRENT_TIMESTAMP as date)'.format(row[0]+numcalls,apiID)
        c.execute(command)
        conn.commit()
        return True

def api_limit_reached(apiID):
    c = conn.cursor()
    c.execute('Select numcalls from apihistory where apiID={} and date = cast(CURRENT_TIMESTAMP as date)'.format(apiID))
    numcalls = c.fetchone()
    c.execute('Select dailylimit from apilimit where apiID={}'.format(apiID))
    dailylimit = c.fetchone()
    if numcalls >= dailylimit:
        return True
    else: 
        return False

def update_qpx_response(useraccountID, rawresponse, requestID):
    c = conn.cursor()   
    #command = "Insert Into qpxresponse(rawresponse) values(\'{}\');".format(rawresponse)
    c.execute("Insert Into qpxresponse(useraccountID, rawresponse,requestID) values(?,?,?)",(useraccountID,rawresponse,requestID))
    conn.commit()
    command = 'Select TOp 1 * from qpxresponse order by queryid desc'
    c.execute(command)
    row = c.fetchone()
    return row[0]

def create_user_account(emailaddress):
    c = conn.cursor()   
    try:
        newuuid=uuid.uuid4()
        c.execute('Insert Into useraccount(useraccountid, emailaddress) Values(\'{}\',\'{}\')'.format(str(newuuid),emailaddress))
        c.execute('Select TOP 1 * from useraccount Order by created desc')
        row = c.fetchone()
        conn.commit()
        return row[0]
    except:
        return None

def create_user_route(useraccountid,o_or_d,cityID,airportID,seasons=[]):
    if o_or_d == 'o':
        table = 'userorigin'
        col = 'origincityid'
        col2 =  'origininstanceID'
    elif o_or_d == 'd':
        table = 'userdestination'
        col = 'destinationcityid'
        col2 = 'destinationinstanceID'
    else: 
        return None

    try: 
        season1 = seasons[0]
    except IndexError:
        season1 = 'NULL'
    try: 
        season2 = seasons[1]
    except IndexError:
        season2 = 'NULL'
    try: 
        season3 = seasons[2]
    except IndexError:
        season3 = 'NULL'

    try:
        c = conn.cursor()   
        c.execute('Insert Into {}(useraccountid, {}, airportID, season1ID, season2ID, season3ID) Values(\'{}\',{},{},{s1},{s2},{s3})'.format(table,col,useraccountid,cityID,airportID,s1=season1,s2=season2,s3=season3))
        conn.commit()
        c.execute('Select TOP 1 {} from {} where useraccountid = \'{}\' Order by created desc'.format(col2,table, useraccountid))
        row = c.fetchone()
        return row[0]

    except pyodbc.Error as ex:
        print ex.args[1]
        return ex.args[1]

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

def qpx_parse(response, verbose=False):
    #Parse qpx Response

    #https://qpx-express-demo.itasoftware.com/
    #c.execute('Select rawresponse from qpxresponse where substr(created,0,11) = date(\'now\') order by created desc')
    #row = c.fetchone()
    #print json.dumps(json.loads(row[0]),indent=4)
    #r = json.loads(row[0])
    
    requestID = response['trips']['requestId']

    x=0
    for to in response['trips']['tripOption']:
        tripoption = str(x)
        price = to['saleTotal'][3:]
        currency = to['saleTotal'][:3]
        tripoptionID = to['id']
        total_flight_duration = str(to['slice'][0]['duration'])
        fareID = to['pricing'][0]['fare'][0]['id']
        farebasiscode = to['pricing'][0]['fare'][0]['basisCode']

        segments = []
        legs = []
        for s in to['slice'][0]['segment']:
            #We do legs first because we need leg duration to calc connectionduration in certain annoying instances
            legdurationtotal = 0
            for l in s['leg']:
                d1 = dateutil.parser.parse(l['arrivalTime'])
                d2 = dateutil.parser.parse(l['departureTime'])
                if 'meal' in l:
                    meal = l['meal']
                else:
                    meal = 'Unknown'
                if 'changePlane' in l:
                    changeplane = l['changePlane']
                    if changeplane == True:
                        changeplane = 1
                    elif changeplane == False:
                        changeplane = 0
                else: 
                    changeplane = None
                if 'secure' in l:
                    secure = l['secure']
                    if secure == True:
                        secure = 1
                    elif secure == False:
                        secure = 0
                else: 
                    secure = None
                if 'originTerminal' in l:
                    originterminal = l['originTerminal']
                else:
                    originterminal = None
                if 'destinationTerminal' in l:
                    destinationterminal = l['destinationTerminal']
                else:
                    destinationterminal = None
                if 'onTimePerformance' in l:
                    onTimePerformance = str(l['onTimePerformance'])
                else:
                    onTimePerformance = None
                legs.append({'requestID': requestID,
                             'tripoptionID' : tripoptionID,
                             'farebasiscode' : farebasiscode,
                             'segmentID': s['id'],
                             'legID' : l['id'],
                             'aircraft' : l['aircraft'],
                             'arrivaltime' : d1.strftime('%Y-%m-%d %X'),
                             'arrivaltimeutcoffset':d1.strftime('%z'),
                             'departuretime':d2.strftime('%Y-%m-%d %X'),
                             'departuretimeutcoffset':d2.strftime('%z'),
                             'origin' : l['origin'],
                             'originterminal' : originterminal,
                             'destination' : l['destination'],
                             'destinationterminal' : destinationterminal,
                             'duration' : str(l['duration']),
                             'ontimeperformance' : onTimePerformance,
                             'mileage' : str(l['mileage']),
                             'meal' : meal,
                             'secure' : secure,
                             'changeplane' : changeplane})
                legdurationtotal = legdurationtotal + l['duration']

            connectionduration = None
            try:
                connectionduration = s['connectionDuration']
            except:
                connectionduration = to['slice'][0]['duration'] - legdurationtotal
                pass
            segments.append({'requestID': requestID,
                             'tripoptionID' : tripoptionID,
                             'farebasiscode' : farebasiscode,
                             'segmentID' : s['id'],
                             'segmentcarrier' : str(s['flight']['carrier']),
                             'segmentflightnumber' : str(s['flight']['number']),
                             'cabin' : s['cabin'],
                             'bookingcode' : s['bookingCode'],
                             'bookingcodecount' : str(s['bookingCodeCount']),
                             'marriedsegmentgroup' : s['marriedSegmentGroup'],
                             'connectionduration' : connectionduration})
            
        for p in to['pricing']:
            try:
                adultcount = str(p['passengers']['adultCount'])
            except:
                pass
            try:
                seniorcount = str(p['passengers']['seniorCount'])
            except:
                pass
            try:
                childcount = str(p['passengers']['childCount'])
            except:
                pass
            try:
                infantinseatcount = str(p['passengers']['infantInSeatCount'])
            except:
                pass
            try:
                infantinlapcount = str(p['passengers']['infantInLapCount'])
            except:
                pass

        latestticketingtime = to['pricing'][0]['latestTicketingTime']

        d3 = dateutil.parser.parse(latestticketingtime)

        trip = {'requestID':requestID,
                'fareID':fareID,
                'farebasiscode':farebasiscode,
                'tripoptionID':tripoptionID,
                'tripoption':tripoption,
                'latestticketingtime':d3.strftime('%Y-%m-%d %X'),
                'origin': requestorigin,
                'destination': requestdestination,
                'price':price,
                'currency':currency,
                'totalflightduration':total_flight_duration,
                'connections' : len(legs)-1,
                'adultcount':adultcount,
                'seniorcount':seniorcount,
                'childcount':childcount,
                'infantinseatcount':infantinseatcount,
                'infantinlapcount':infantinlapcount}                      

        x+=1

        try:
            c.execute('Insert into qpxtrip(requestID,fareID,farebasiscode,tripoptionID,tripoption,latestticketingtime,origin,destination,'
                          'price,currency,totalflightduration,connections,adultcount,seniorcount,childcount,infantinseatcount,infantinlapcount) '
                          'VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',(trip['requestID'],trip['fareID'],trip['farebasiscode'],trip['tripoptionID'],
                            trip['tripoption'],trip['latestticketingtime'],trip['origin'],trip['destination'],trip['price'],
                            trip['currency'],trip['totalflightduration'],trip['connections'],trip['adultcount'],trip['seniorcount'],
                            trip['childcount'],trip['infantinseatcount'],trip['infantinlapcount']))
            conn.commit()
        except pyodbc.Error as ex:
                print ex.args[1]

        for si in segments:
            try:
                c.execute('Insert into qpxsegment(requestID,tripoptionID,farebasiscode,segmentID,segmentcarrier,segmentflightnumber,cabin,'
                          'bookingcode,bookingcodecount,marriedsegmentcount,connectionduration) ' 
                          'VALUES(?,?,?,?,?,?,?,?,?,?,?)',(si['requestID'],si['tripoptionID'],si['farebasiscode'],si['segmentID'],
                           si['segmentcarrier'],si['segmentflightnumber'],si['cabin'],si['bookingcode'],si['bookingcodecount'],
                           si['marriedsegmentgroup'],si['connectionduration']))
                conn.commit()
            except pyodbc.Error as ex:
                print ex.args[1]
                pass

        for l in legs:
            try:
                c.execute('Insert into qpxleg(requestID,tripoptionID,farebasiscode,segmentID,legID,aircraft,arrivaltime,'
                          'arrivaltimeutcoffset,departuretime,departuretimeutcoffset,origin,originterminal,destination,'
                          'destinationterminal,duration,ontimeperformance,mileage,meal,secure,changeplane) ' 
                          'VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',(l['requestID'],l['tripoptionID'],l['farebasiscode'],l['segmentID'],l['legID'],
                           l['aircraft'],l['arrivaltime'],l['arrivaltimeutcoffset'],l['departuretime'],l['departuretimeutcoffset'],
                           l['origin'],l['originterminal'],l['destination'],l['destinationterminal'],l['duration'],l['ontimeperformance'],
                           l['mileage'],l['meal'],l['secure'],l['changeplane']))
                conn.commit()
            except pyodbc.Error as ex:
                print ex.args[1]
                pass
                #return 'er:', er.message       

        if verbose == True:
            print '\n'
            for i in trip:
                print str(i) + ': ' + str(trip[i])

            for si in segments:
                print '\n'
                print 'SEGMENTS: '
                for sii in si:
                    print str(sii) + ': ' + str(si[sii])

            for l in legs:
                print '\n'
                print 'LEGS: '
                for li in l:
                    print str(li) + ': ' + str(l[li])

def qpx_search(useraccountID,jsonquery,apikey=qpxkey,verbose=False):
    if api_limit_reached(apiID=1) == False:
        headers = {'content-type': 'application/json'}
        url = 'https://www.googleapis.com/qpxExpress/v1/trips/search?key={}'.format(apikey)
        r = requests.post(url, headers=headers, data=jsonquery)
        if r.status_code == 200:
            print str(r.status_code) +' - Success!'
            update_api_history(apiID=1,numcalls=1)
            response = r.json()
            requestID = response['trips']['requestId']
            update_qpx_response(useraccountID,r.text,requestID)
            qpx_parse(response=response,verbose=verbose)
            return r.json()
        else: 
            print str(r.status_code) + ' - Failure!'
            return None
    else:
        print 'API Limit Reached!'
        return None

def get_user_preferences(useraccountid):
    #Query to get unique combinations of Origin/Destinations for a user
    #Add in the future to return preferred airlines, travel seasons, length of trip, etc...
    c = conn.cursor()
    command = """--See users preferences
                Select uo.useraccountID, a1.code as origincode, a2.code as destinationcode,
                s.monthnum1,s.monthnum2,s.monthnum3,
                s2.monthnum1 as monthnum4,s2.monthnum2 as monthnum5,s2.monthnum3 as monthnum6,
                s3.monthnum1 as monthnum7,s3.monthnum2 as monthnum8,s3.monthnum3 as monthnum9
                from userorigin uo 
                join userdestination ud on ud.useraccountid = uo.useraccountid 
                left join seasons s on s.seasonID = ud.season1ID
                left join seasons s2 on s2.seasonID = ud.season2ID
                left join seasons s3 on s3.seasonID = ud.season3ID
                left join airports a1 on a1.airportID = uo.airportID 
                left join airports a2 on a2.airportID = ud.airportID """
    c.execute(command)
    rows = c.fetchall()
    routes = []
    for r in rows:
        routes.append([r[1],r[2], [r[3],r[4],r[5],r[6],r[7],r[8],r[9],r[10],r[11]] ])
    return routes

def update_sks_response(useraccountID,rawresponse):
        c = conn.cursor()   
        #command = "Insert Into qpxresponse(rawresponse) values(\'{}\');".format(rawresponse)
        c.execute("Insert Into sksresponse(useraccountID,rawresponse) values(?,?)",(useraccountID,rawresponse))
        conn.commit()
        command = 'Select TOp 1 * from sksresponse order by queryid desc'
        c.execute(command)
        row = c.fetchone()
        return row[0]

def sks_search(useraccountID, userip, origin, destination, month=None, apikey=skyscannerkey):
    headers = {'Accept': 'application/json',
               'X-Forwarded-For': userip}
    if month != None:
        year = time.strftime("%Y")
        current_month = time.strftime("%m")
        if int(current_month) > int(month):
            year = year+1
        period = str(year)+'-'+str(month).zfill(2)
        url = 'http://partners.api.skyscanner.net/apiservices/browsedates/v1.0/US/USD/en-US/{}-Iata/{}-Iata/{}/{}/?apiKey={}'.format(origin,destination,period,period,apikey)
    else:
        url = 'http://partners.api.skyscanner.net/apiservices/browsedates/v1.0/US/USD/en-US/{}-Iata/{}-Iata/anytime/anytime/?apiKey={}'.format(origin,destination,apikey)

    while True:
        r = requests.get(url, headers=headers)
        update_api_history(apiID=6,numcalls=1)
        queryID = update_sks_response(useraccountID,r.text)
        if r.status_code == 200:
            #print json.dumps(r.json(), indent=4)
            response = r.json()
            
            for x in response['Carriers']:
                command = "Update airlines Set skscarrierID = {} where skscarrierID IS NULL and name = \'{}\'".format(x['CarrierId'],x['Name'])
                c.execute(command)
                conn.commit()

            for x in response['Places']:
                command = "Update airports Set sksplaceID = {} where sksplaceID IS NULL and code = \'{}\'".format(x['PlaceId'],x['IataCode'])
                c.execute(command)
                conn.commit()

            for q in response['Quotes']:
                d1 = dateutil.parser.parse(q['QuoteDateTime']) 
                d2 = dateutil.parser.parse(q['OutboundLeg']['DepartureDate']) 
                d3 = dateutil.parser.parse(q['InboundLeg']['DepartureDate']) 

                if q['Direct'] == True:
                    direct = 1
                else: 
                    direct = 0

                if len(q['OutboundLeg']['CarrierIds'])>0:
                    out_carrierID = q['OutboundLeg']['CarrierIds'][0]
                else: 
                    out_carrierID = 'NULL'
                
                if len(q['InboundLeg']['CarrierIds'])>0:
                    in_carrierID = q['InboundLeg']['CarrierIds'][0]
                else:
                    in_carrierID = 'NULL'
                
                command = """Insert Into sksquotes(queryID,quoteID,quotedatetime,minprice,direct,out_carrierID,out_originID,out_destinationID,out_departuredate,in_carrierID,in_originID,in_destinationID,in_departuredate) 
                        VALUES({queryID},{quoteID},\'{quotedatetime}\',{minprice},{direct},{out_carrierID},{out_originID},{out_destinationID},\'{out_departuredate}\',{in_carrierID},{in_originID},{in_destinationID},\'{in_departuredate}\')""".format(queryID=queryID,
                        quoteID=q['QuoteId'],
                        quotedatetime=d1.strftime('%Y-%m-%d %X'),
                        minprice=q['MinPrice'],
                        direct=direct,
                        out_carrierID=out_carrierID,
                        out_originID=q['OutboundLeg']['OriginId'],
                        out_destinationID=q['OutboundLeg']['DestinationId'],
                        out_departuredate=d2.strftime('%Y-%m-%d %X'),
                        in_carrierID=in_carrierID,
                        in_originID=q['InboundLeg']['OriginId'],
                        in_destinationID=q['InboundLeg']['DestinationId'],
                        in_departuredate=d3.strftime('%Y-%m-%d %X'))
                try:
                    c.execute(command)
                    conn.commit()
                except pyodbc.Error as ex:
                    print ex.args[1]
                    conn.rollback()
                    conn.close
                    exit()
            break
        elif r.status_code == 429:
            print '429 - Too Many Requests'
            time.sleep(1)
            #No Break = Retry
        else:
            print r.status_code
            print json.dumps(r.json(), indent=4)
            break

def get_users():
    c = conn.cursor()
    c.execute('select * from useraccount')
    rows=c.fetchall()
    users = []
    for r in rows:
        users.append({'useraccountid':r[0],'emailaddress':r[1],'created':r[2]})
    return users


if __name__ == '__main__':

    ############################################################################
    #DB Setup
    data_reset(reset=data_resetflag)

    c = conn.cursor()
    if table_exists('apilimit') == False:
        #c.execute('Drop Table apilimit')
        c.execute('CREATE TABLE IF NOT EXISTS apilimit(apiID int,apicode varchar(20),apiname varchar(100),dailylimit int)')
        command = 'INSERT INTO apilimit(apiID, apicode, apiname, dailylimit) Values({},\'{}\',\'{}\',{})'.format('1','QPX','Google QPX Express API',50)
        c.execute(command)
        command = 'INSERT INTO apilimit(apiID, apicode, apiname, dailylimit) Values({},\'{}\',\'{}\',{})'.format('2','GEO','Google Maps Geocode API',2500)
        c.execute(command)
        conn.commit()

    if table_exists('apihistory') == False:
        #c.execute('Drop Table apihistory') 
        c.execute('CREATE TABLE IF NOT EXISTS apihistory(apiID int, date date, numcalls int)')
        c.execute('CREATE UNIQUE INDEX {ix} on {tn}({cn},{cn2})'.format(ix='IDX_apihistory', tn='apihistory', cn='apiID', cn2='date'))
        conn.commit()

    if table_exists('qpxresponse') == False:
        #c.execute('Drop Table qpxresponse')
        #command = 'CREATE TABLE IF NOT EXISTS qpxresponse(queryid INTEGER PRIMARY KEY, rawresponse Nvarchar(MAX), created DATETIME DEFAULT (DATETIME(\'now\')), requestID TEXT)'
        command = "CREATE TABLE dbo.qpxresponse (queryid INTEGER PRIMARY KEY IDENTITY(1,1), rawresponse NVARCHAR(MAX), created DATETIME DEFAULT CURRENT_TIMESTAMP, requestID TEXT, useraccountID VARCHAR(36))"
        c.execute(command)
        conn.commit()

    if table_exists('useraccount') == False:
        #c.execute('Drop Table useraccount')
        command = 'CREATE TABLE IF NOT EXISTS useraccount(useraccountid VARCHAR(36) PRIMARY KEY, emailaddress varchar(250), created DATETIME DEFAULT (DATETIME(\'now\')))'
        c.execute(command)
        conn.commit()
        create_user_account('pogster@gmail.com')

    if table_exists('userorigin') == False:
        command = 'CREATE TABLE IF NOT EXISTS userorigin(origininstanceID INTEGER PRIMARY KEY, useraccountid VARCHAR(36), origincityID, airportID, created DATETIME DEFAULT (DATETIME(\'now\')))'
        c.execute(command)
        command = 'CREATE UNIQUE INDEX {ix} on {tn}({cn},{cn2},{cn3})'.format(ix='IDX_userorigin', tn='userorigin', cn='useraccountid', cn2='origincityID', cn3='airportID')
        c.execute(command)
        conn.commit()
        #create origins (must somehow enforce 1 per user at the beginning)
        cityid, airports = nearby_airports(citycode='PHL')
        for a in airports:
            create_user_route(useraccountid='9e6b6207-31a3-481e-b5e3-5754fdcd222a',o_or_d='o',cityID=cityid,airportID=a)

    if table_exists('userdestination') == False:
        command = "CREATE TABLE IF NOT EXISTS userdestination(destinationinstanceID INTEGER PRIMARY KEY, useraccountid VARCHAR(36), destinationcityID, airportID, created DATETIME DEFAULT (DATETIME(\'now\')))"
        c.execute(command)
        command = 'CREATE UNIQUE INDEX {ix} on {tn}({cn},{cn2},{cn3})'.format(ix='IDX_userdestination', tn='userdestination', cn='useraccountid', cn2='destinationcityID', cn3='airportID')
        c.execute(command)
        conn.commit()
        #create destinations
        cityid, airports = nearby_airports(citycode='REK')
        for a in airports:
            create_user_route(useraccountid='9e6b6207-31a3-481e-b5e3-5754fdcd222a',o_or_d='d',cityID=cityid,airportID=a)

    if table_exists('qpxtrip') == False:
        #command = "CREATE TABLE IF NOT EXISTS qpxtrip('tripinstanceID` INTEGER PRIMARY KEY, `requestID` VARCHAR(12), `fareID` VARCHAR(500), `farebasiscode` VARCHAR(8), `tripoptionID` VARCHAR(25), `tripoption` INTEGER, `latestticketingtime` DATETIME, `origin` VARCHAR(3), `destination` VARCHAR(3), `price` DECIMAL(10,2), `currency` VARCHAR(3), `totalflightduration` INTEGER, 'connections' INTEGER, `adultcount` INTEGER, `seniorcount` INTEGER, `childcount` INTEGER, `infantinseatcount` INTEGER, `infantinlapcount` INTEGER, `created` INTEGER DEFAULT (DATETIME(\'now\')))"
        command = "CREATE TABLE dbo.qpxtrip ( tripinstanceID INTEGER PRIMARY KEY IDENTITY(1,1), requestID VARCHAR(22) COLLATE SQL_Latin1_General_Cp1_CS_AS, fareID VARCHAR(500) COLLATE SQL_Latin1_General_Cp1_CS_AS, farebasiscode VARCHAR(8), tripoptionID VARCHAR(25) COLLATE SQL_Latin1_General_Cp1_CS_AS, tripoption INTEGER, latestticketingtime DATETIME, origin VARCHAR(3), destination VARCHAR(3), price DECIMAL(10,2), currency VARCHAR(3), totalflightduration INTEGER, connections INTEGER, adultcount INTEGER, seniorcount INTEGER, childcount INTEGER, infantinseatcount INTEGER, infantinlapcount INTEGER, created DATETIME DEFAULT CURRENT_TIMESTAMP)"
        c.execute(command)
        command = 'CREATE UNIQUE INDEX {ix} on {tn}({cn} DESC, {cn2} ASC)'.format(ix='IDX_qpxtrip', tn='qpxtrip', cn='requestID', cn2='tripoptionID')
        c.execute(command)
        conn.commit()   

    if table_exists('qpxsegment') == False:
        #command = "CREATE TABLE IF NOT EXISTS qpxsegment(`segmentinstanceID` INTEGER PRIMARY KEY, `requestID` VARCHAR(22), `tripoptionID` VARCHAR(25), `farebasiscode` VARCHAR(8), `segmentID` VARCHAR(16), `segmentcarrier` VARCHAR(2), `segmentflightnumber` VARCHAR(10), `cabin` VARCHAR(50), `bookingcode` VARCHAR(10), `bookingcodecount` INTEGER, `marriedsegmentcount` INTEGER, `connectionduration` INTEGER, `created` DATETIME DEFAULT DATETIME(\'now\'))"
        command = "CREATE TABLE dbo.qpxsegment (segmentinstanceID INTEGER PRIMARY KEY IDENTITY(1,1), requestID VARCHAR(22) COLLATE SQL_Latin1_General_Cp1_CS_AS, tripoptionID VARCHAR(25) COLLATE SQL_Latin1_General_Cp1_CS_AS, farebasiscode VARCHAR(8), segmentID VARCHAR(16) COLLATE SQL_Latin1_General_Cp1_CS_AS, segmentcarrier VARCHAR(2), segmentflightnumber VARCHAR(10), cabin VARCHAR(50), bookingcode VARCHAR(10), bookingcodecount INTEGER, marriedsegmentcount INTEGER, connectionduration INTEGER, created DATETIME DEFAULT CURRENT_TIMESTAMP)"
        c.execute(command)
        command = 'CREATE UNIQUE INDEX {ix} on {tn}({cn} DESC,{cn2} ASC,{cn3} ASC)'.format(ix='IDX_qpxsegment', tn='qpxsegment', cn='requestID', cn2='tripoptionID', cn3='segmentID')
        c.execute(command)
        conn.commit()   

    if table_exists('qpxleg') == False:
        #command = "CREATE TABLE IF NOT EXISTS qpxleg(`leginstanceID` INTEGER UNIQUE, `requestID` VARCHAR(22), `tripoptionID` VARCHAR(25), `farebasiscode` VARCHAR(8), `segmentID` VARCHAR(16), `legID` VARCHAR(16), `aircraft` VARCHAR(3), `arrivaltime` DATETIME, `arrivaltimeutcoffset` INTEGER, `departuretime` DATETIME, `departuretimeutcoffset` INTEGER, `origin` VARCHAR(3), `originterminal` VARCHAR(5), `destination` VARCHAR(3), `destinationterminal` VARCHAR(5), `duration` INTEGER, `ontimeperformance` INTEGER, `mileage` INTEGER, `meal` VARCHAR(100), `secure` INTEGER, `changeplane` INTEGER, `created` DATETIME DEFAULT DATETIME(\'now\')"
        command = "CREATE TABLE dbo.qpxleg (leginstanceID INTEGER PRIMARY KEY IDENTITY(1,1), requestID VARCHAR(22) COLLATE SQL_Latin1_General_Cp1_CS_AS, tripoptionID varchar(25) COLLATE SQL_Latin1_General_Cp1_CS_AS, farebasiscode VARCHAR(8), segmentID VARCHAR(16) COLLATE SQL_Latin1_General_Cp1_CS_AS, legID VARCHAR(16), aircraft VARCHAR(3), arrivaltime DATETIME, arrivaltimeutcoffset INTEGER, departuretime DATETIME, departuretimeutcoffset INTEGER, origin VARCHAR(3), originterminal VARCHAR(5), destination VARCHAR(3), destinationterminal VARCHAR(5), duration INTEGER, ontimeperformance INTEGER, mileage INTEGER, meal VARCHAR(100), secure INTEGER, changeplane INTEGER, created DATETIME DEFAULT CURRENT_TIMESTAMP)"
        c.execute(command)
        command = 'CREATE UNIQUE INDEX {ix} on {tn}({cn} DESC,{cn2} ASC,{cn3} ASC)'.format(ix='IDX_qpxleg', tn='qpxleg', cn='requestID', cn2='tripoptionID',cn3='legID')
        c.execute(command)
        conn.commit()   

    if table_exists('sksresponse') == False:
        #command = 'CREATE TABLE IF NOT EXISTS sksresponse(queryid INTEGER PRIMARY KEY, rawresponse Nvarchar(MAX), created DATETIME DEFAULT (DATETIME(\'now\')))'
        command = "CREATE TABLE dbo.sksresponse ( queryid INTEGER PRIMARY KEY IDENTITY(1,1), rawresponse NVARCHAR(MAX), created DATETIME DEFAULT CURRENT_TIMESTAMP, useraccountID VARCHAR(36))"
        c.execute(command)
        conn.commit()

    if table_exists('sksquotes') == False:
        #command = "CREATE TABLE IF NOT EXISTS sksquotes(queryID INTEGER NOT NULL, quoteID INTEGER, quotedatetime datetime, minprice decimal(10,2), direct tinyint, out_carrierID INTEGER, out_originID INTEGER, out_destinationID INTEGER, out_departuredate datetime, in_carrierID INTEGER, in_originID INTEGER, in_destinationID INTEGER, in_departuredate datetime, created DATETIME DEFAULT(DATETIME(\'now\')))"
        command = "CREATE TABLE sksquotes(queryID INTEGER, quoteID INTEGER, quotedatetime datetime, minprice decimal(10,2), direct tinyint, out_carrierID INTEGER, out_originID INTEGER, out_destinationID INTEGER, out_departuredate datetime, in_carrierID INTEGER, in_originID INTEGER, in_destinationID INTEGER, in_departuredate datetime, created DATETIME DEFAULT CURRENT_TIMESTAMP)"
        c.execute(command)
        command = "CREATE UNIQUE INDEX IDX_sksquotes ON sksquotes(queryID ,quoteID)"
        c.execute(command)
        conn.commit()
    
    if table_exists('seasons') == False:
        command = "CREATE TABLE IF NOT EXISTS seasons(seasonID INTEGER PRIMARY KEY, seasonname INTEGER, monthname1 TEXT, monthname2 TEXT, monthname3 TEXT, monthnum1 INTEGER, monthnum2 INTEGER, monthnum3 INTEGER, created DATETIME DEFAULT (DATETIME(\'now\')))"
        c.execute(command)
        command = """Insert into seasons (seasonname,monthname1,monthname2,monthname3,monthnum1,monthnum2,monthnum3) values ('Spring','March','April','May',3,4,5);
                    Insert into seasons (seasonname,monthname1,monthname2,monthname3,monthnum1,monthnum2,monthnum3) values ('Summer','June','July','August',6,7,8);
                    Insert into seasons (seasonname,monthname1,monthname2,monthname3,monthnum1,monthnum2,monthnum3) values ('Fall','September','October','November',9,10,11);
                    Insert into seasons (seasonname,monthname1,monthname2,monthname3,monthnum1,monthnum2,monthnum3) values ('Winter','December','January','February',12,1,2);
                    """
        c.execute(command)
        conn.commit()

    ############################################################################
    #User Settings

    #User must input emailaddress and up to 1 Origin + 10 Destination cities to track
    #We will use IATACodes Naerby API to lookup the airports in those cities + nearby airports with 150 miles. 

    #create origins (must somehow enforce 1 per user at the beginning)
    cityid, airports = nearby_airports(citycode='PHL')
    for a in airports:
        create_user_route(useraccountid='9e6b6207-31a3-481e-b5e3-5754fdcd222a',o_or_d='o',cityID=cityid,airportID=a)
    
    #create destinations
    cityid, airports = nearby_airports(citycode='REK')
    for a in airports:
        create_user_route(useraccountid='9e6b6207-31a3-481e-b5e3-5754fdcd222a',o_or_d='d',cityID=cityid,airportID=a,seasons=[1,2])

    ############################################################################
    #SKS Search
    users = get_users()
    for u in users:
        routes = get_user_preferences(u['useraccountid'])
        for r in routes:
            print r
            try: #loop through specified months
                for m in r[2]:
                    #print m, u['useraccountid']
                    if m!=None:
                        sks_search(useraccountID=u['useraccountid'], userip='100.34.202.47',origin=r[0],destination=r[1],month=m)
            except Exception as err: #no months specified
                print err
                sks_search(useraccountID=u['useraccountid'], userip='100.34.202.47',origin=r[0],destination=r[1])




    ############################################################################
    #QPX Search

    requestorigin = 'PHL'
    requestdestination = 'LAX'

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
        "solutions": 50,
        "slice": [
          {
            "origin": requestorigin,
            "destination": requestdestination,
            "date": "2017-06-01",
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

    #print json.dumps(jsonbody, indent=4)
    #parsedjson = json.loads(json.dumps(jsonbody))

    
    #r = qpx_search(useraccountID='9e6b6207-31a3-481e-b5e3-5754fdcd222a',jsonquery=json.dumps(jsonbody),verbose=False)
    ## Next we must construct a request based on stored user input, instead of hardcoding it


    print '\n'
    conn.close

    exit()



