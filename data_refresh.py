
import requests, sqlite3, json, ConfigParser, time, datetime
from dateutil.parser import parse
from dateutil.relativedelta import *
from dateutil import tz
from ptero import update_api_history 
from ptero import api_limit_reached

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
    #IATA Cities DB
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
                response = r.json()
                c = conn.cursor()
                count = 0
                for i in response['response']:
                    c.execute("Select * from cities where code = '{}'".format(i['code']))
                    row = c.fetchone() 
                    if row[0] == None: #Only insert new rows
                        #print 'inserting {}, {}, {}'.format(i['code'],i['name'].encode('utf-8'),i['country_code'])
                        c.execute('Insert Into cities(code, name, country_code) values(?,?,?)',(i['code'],i['name'],i['country_code']))
                        count = count+1
                conn.commit()
                print 'iata_cities_refresh - '+ str(r.status_code) +' - Success - '+str(count)+' updated!'
            else: 
                print str(r.status_code) + ' - ERROR!'
        else:
            print 'IATA cities -> its not time for an update yet'
    else: 
        r = requests.post(url, headers=headers) #API Call and refill table data
        if r.status_code == 200:
            response = r.json()
            c = conn.cursor()
            count = 0
            for i in response['response']:
                c.execute('Insert Into cities(code, name, country_code) values(?,?,?)',(i['code'],i['name'],i['country_code']))
                count = count+1

            conn.commit()   
            print 'iata_cities_refresh - '+ str(r.status_code) +' - Success - '+str(count)+' updated!'
        else: 
            print str(r.status_code) + ' - ERROR!'
    return None

def geocode_cities():
    #Google API for Lat/Long of each City
    #9368 cities but 2500 per day limit and 50 per second limit. Will need to save existing lat/long so as not to requery each week. 
    c = conn.cursor()
    c.execute("Select * from cities where lat IS NULL or long IS NULL or state IS NULL")
    #c.execute("Select * from cities where code > 'ATC'")
    rows = c.fetchall()
    headers = {'content-type': 'application/json'}
    count = 0
    for r in rows:
        if api_limit_reached(apiID=2) == False:
        #if i<=0: #2450 limit per day
            code = r[0]
            cityorig = r[1].encode("utf8")
            cityorig = cityorig.replace("'", "''") #double up on quotes to escape them in the WHERE clause
            city = cityorig.translate(None, "'") #strip ' from city names
            country = r[2]
            state = r[3]
            if state != None:
                url = 'https://maps.googleapis.com/maps/api/geocode/json?address={}&components=country:{}|administrative_area:{}&key={}'.format(city,country,state,geocodekey)
            else: 
                url = 'https://maps.googleapis.com/maps/api/geocode/json?address={}&components=country:{}&key={}'.format(city,country,geocodekey)
            attempt = 1
            for attempt in range(3):
                r = requests.post(url, headers=headers)
                update_api_history(apiID=2,numcalls=1)
                if r.status_code == 200:
                    response = r.json()
                    # print r.url                
                    # print response
                    # Get Lat/Long of the City/Country
                    if response['status'] == 'OK':
                        lat = response['results'][0]['geometry']['location']['lat']
                        lng = response['results'][0]['geometry']['location']['lng']
                        #Get State of Lat/Long
                        statesearch = response['results'][0]['address_components']
                        state = None
                        for s in statesearch:
                            if s['types']:
                                d = {s['types'][0]:s['short_name']}
                                if d.get('administrative_area_level_1'):
                                    state = d.get('administrative_area_level_1').encode("utf8")
                                    state = state.replace("'", "''")
                        print "Update cities Set state='{}',lat={},long={} Where name = '{}' AND country_code = '{}'".format(state,lat,lng,cityorig,country)
                        c.execute("Update cities Set state='{}',lat={},long={} Where name = '{}' AND country_code = '{}'".format(state,lat,lng,cityorig,country))
                        conn.commit()
                        count+=1
                        break
                    elif response['status'] == 'ZERO_RESULTS':
                        #Retry with country in the address, this fixes some edge cases like 'St Martin, MF'
                        url = 'https://maps.googleapis.com/maps/api/geocode/json?address={}&key={}'.format(city+','+country,geocodekey)
                        attempt+=1
                    elif response['status'] == 'OVER_QUERY_LIMIT':
                        break
                    else:
                        print 'Error: ', response['status']
                        attempt+=1
                else: 
                    print str(r.status_code) + ' - API ERROR!'
                    attempt+=1

            time.sleep(0.04) #no more than 25 requests per second
        else:
            print 'Google Geocode -> Exceeded Daily API Limit'
            break 
    print '{} rows geocoded!'.format(count)


############################################################################
#DB Setup
data_reset(reset=data_resetflag)

c = conn.cursor()
if table_exists('cities') == False:
    command = 'Create Table IF NOT EXISTS cities(code varchar(3) PRIMARY KEY, name varchar(100), country_code varchar(2), state varchar(100), lat Decimal(9,6), long Decimal(9,6), created DATETIME DEFAULT (DATETIME(\'now\')))'
    c.execute(command)
    conn.commit()
    print 'Table cities Created!'

############################################################################

#update_api_history(apiID=2,numcalls=2517)
iata_city_refresh(force=False)
geocode_cities()


#Should use update_api_history function from ptero.py to track this max calls constraint

conn.close
