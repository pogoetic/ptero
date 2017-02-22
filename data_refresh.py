
import requests, sqlite3, json, ConfigParser, time, datetime, pyodbc
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
conn2 = pyodbc.connect('DRIVER='{ODBC Driver 13 for SQL Server}';PORT=1433;SERVER='+config.get("DB","server")+';PORT=1443;DATABASE='+config.get("DB","database")+';UID='+config.get("DB","uname")+';PWD='+ config.get("DB","pwd"))
cursor = conn2.cursor()
cursor.execute("select @@VERSION")
row = cursor.fetchone()
if row:
    print row

exit()

data_resetflag = False

def data_reset(reset=False):
    if reset == True:
        c = conn.cursor()
        table = ['cities','airports']
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


def iata_refresh(table, apikey=iatakey, force=False):
    #IATA Airline DB
    if table == 'airports':
        id_col = 'airportID'
        apiID = 4
    elif table == 'airlines':
        id_col = 'airlineID'
        apiID = 3
    elif table == 'cities':
        id_col = 'cityID'
        apiID = 5

    headers = {'content-type': 'application/json'}
    url = ' https://iatacodes.org/api/v6/{}?api_key={}'.format(table,apikey)
    c = conn.cursor()
    c.execute('Select count(*) from {}'.format(table))
    numrows = c.fetchone()
    if numrows[0] != 0:
        c.execute('Select max(created) from {}'.format(table))
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
                    c.execute("Select code from {} where code = \'{}\'".format(table,i['code']))
                    if c.fetchone() == None: #Only insert new rows
                        if table == 'cities':
                            c.execute('Insert Into {}(code, name, country_code) values(?,?,?)'.format(table),(i['code'],i['name'],i['country_code']))
                        else: 
                            c.execute('Insert Into {}(code, name) values(?,?)'.format(table),(i['code'],i['name']))
                            count = count+1
                conn.commit()
                update_api_history(apiID=apiID,numcalls=count)
                print 'iata_{}_refresh - '.format(table)+ str(r.status_code) +' - Success - '+str(count)+' updated!'
            else: 
                print str(r.status_code) + ' - ERROR!'
            if count == 0:
                #we update one date so our check run logic holds for next time
                c.execute('Update {} set created = DATETIME(\'now\') where {} = 1'.format(table,id_col))
                conn.commit()
        else: 
            print 'IATA {} -> its not time for an update yet'.format(table)
    else: 
        r = requests.post(url, headers=headers) #API Call and refill table data
        if r.status_code == 200:
            response = r.json()
            c = conn.cursor()
            count = 0
            for i in response['response']:
                #need to remove dupes before insert:
                c.execute("Select code from {} where code = \'{}\'".format(table,i['code']))
                if c.fetchone() == None:
                    if table == 'cities':
                        c.execute('Insert Into {}(code, name, country_code) values(?,?,?)'.format(table),(i['code'],i['name'],i['country_code']))
                    else: 
                        c.execute('Insert Into {}(code, name) values(?,?)'.format(table),(i['code'],i['name']))
                        count = count+1
            conn.commit()   
            update_api_history(apiID=apiID,numcalls=count)
            print 'iata_{}_refresh - '.format(table)+ str(r.status_code) +' - Success - '+str(count)+' updated!'
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
    overlimit = 0
    for r in rows:
        if api_limit_reached(apiID=2) == False and overlimit == 0:
            code = r[1]
            cityorig = r[2].encode("utf8")
            cityorig = cityorig.replace("'", "''") #double up on quotes to escape them in the WHERE clause
            city = cityorig.translate(None, "'") #strip ' from city names
            country = r[3]
            state = r[4]
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
                        overlimit = 1
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
    print 'Google Geocode -> {} rows geocoded!'.format(count)

if __name__ == '__main__':
 ############################################################################
#DB Setup
    data_reset(reset=data_resetflag)

    c = conn.cursor()
    if table_exists('cities') == False:
        command = 'Create Table IF NOT EXISTS cities(cityID INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE, code varchar(3), name varchar(100), country_code varchar(2), state varchar(100), lat Decimal(9,6), long Decimal(9,6), created DATETIME DEFAULT (DATETIME(\'now\')))'
        c.execute(command)
        command = 'CREATE UNIQUE INDEX IDX_cities ON cities (code ASC)'
        c.execute(command)
        conn.commit()
        print 'Table cities Created!'

        command = 'Create Table IF NOT EXISTS airports(airportID INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE, code varchar(3), name varchar(100), lat Decimal(9,6), long Decimal(9,6), created DATETIME DEFAULT (DATETIME(\'now\')))'
        c.execute(command)
        command = 'CREATE UNIQUE INDEX IDX_airports ON airports (code ASC)'
        c.execute(command)
        conn.commit()
        print 'Table ariports Created!'

        command = 'Create Table IF NOT EXISTS airlines(airlineID INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE, code varchar(2), name varchar(100), created DATETIME DEFAULT (DATETIME(\'now\')), skscarrierID INTEGER NULL)'
        c.execute(command)
        command = 'CREATE UNIQUE INDEX IDX_airlines ON airlines (code ASC)'
        c.execute(command)
        conn.commit()
        print 'Table ariports Created!'

    ############################################################################

    #1 - Get City Name from User (will want to pre-populate city names in the future from full list of IATA cities)
    #2 - Hit IATA Cities Endpoint, collect 'country_code' as well
    #3 - Hit Google Geocode API with CityName, CountryName, collect Lat/Long
    #4 - Hit IATA airport API

    iata_refresh(table='cities', force=False)
    geocode_cities()
    iata_refresh(table='airports', force=False)
    iata_refresh(table='airlines', force=False)







conn.close
