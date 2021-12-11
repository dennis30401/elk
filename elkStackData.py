import sqlite3, time
from datetime import datetime, timezone, timedelta
import schedule
from sqlite3.dbapi2 import Cursor
from elasticsearch import Elasticsearch

'''
 Sensor logs in logs.sqlite3 data DDL is: 
 
 CREATE TABLE metricsNumber(
     id INTEGER,
     date DATE,
     value REAL
 )
 
 There are also other table, but don;t know what they mean.

 Regardless of whether they are the number of people, humidity, temperature,
 return data will look like [(4.0,), (0.0,), (4.0,)], 
 list contain tuple, and the data type in tuple is float.
 But limit 1 will contain only one tuple [(4.0,)]
'''

'''database, ip, port, etc., configure field'''
data_peopleNumber_id = '1'
data_humidity_id     = '3'
data_temperature_id  = '2'
logs_file_path       = "/home/pi/.webthings/log/logs.sqlite3"
teacher_host         = '140.134.25.64'
elasticsearh_port    = 19200

'''global value, if it is called in function, it needs to be declared as a global variable'''
peopleNumber_old = 100
humidity_old     = 100
temperature_old  = 100

def get_humidity( cursorObj : Cursor ) -> int:
    '''get latest humidity in logs.sqlite3'''
    id = data_humidity_id
    sql_string = "select value  from metricsNumber where  id = "+ id +"  ORDER BY date desc LIMIT 1 ;"
    cursorObj.execute(sql_string)
    dataList = cursorObj.fetchall()
    global humidity_old # handle "local variable referenced before assignment" error
    if dataList:
        # if dataList no empty, update new value
        humidityFloat = dataList[0][0]
        humidityInt = int(humidityFloat)
        # update old value
        humidity_old = humidityInt
    else:
        humidityInt = humidity_old
    return humidityInt 

def get_people( cursorObj : Cursor ) -> int:
    '''get latest people number in logs.sqlite3'''
    id =data_peopleNumber_id
    sql_string = "select value  from metricsNumber where  id = "+ id +"  ORDER BY date desc LIMIT 1 ;"
    cursorObj.execute(sql_string)
    dataList = cursorObj.fetchall()
    global peopleNumber_old
    if dataList:
        peopleFloat = dataList[0][0]
        peopleInt = int(peopleFloat)
        peopleNumber_old = peopleInt
    else:
        peopleInt = peopleNumber_old
    return peopleInt

def get_temp( cursorObj : Cursor ) -> int:
    '''get latest temperature in logs.sqlite3'''
    id = data_temperature_id
    sql_string = "select value  from metricsNumber where  id = "+ id +"  ORDER BY date desc LIMIT 1 ;"
    cursorObj.execute(sql_string)
    dataList = cursorObj.fetchall()
    global temperature_old
    if dataList:
        tempFloat = dataList[0][0]
        tempInt = int(tempFloat)
        temperature_old = tempInt
    else:
        tempInt = temperature_old
    return tempInt

def get_datetime() -> str:   # get UTC+8 time format
    """ return formatted time (YYYY-MM-DD HH:MM UTC+8) """
    # set time zone(TW->UTC+8)
    tz = timezone(timedelta(hours=+8))
    # get the current time、 time zone, and convert to ISO format(to second) e.g. "2021-09-19T16:50:00+08:00"
    formatTime = datetime.now(tz).isoformat(timespec="seconds")
    return formatTime

def send_data_elk():
    '''get data from sqlite, then send data to elasticsearch'''
    # open connection to database
    con = sqlite3.connect(logs_file_path)
    cursorObj = con.cursor()
    # Get the required data
    peopleNumber = get_people(cursorObj)
    temperature = get_temp(cursorObj)
    humidity = get_humidity(cursorObj)    
    # close connection
    cursorObj.close()
    con.close()

    # send to elasticsearch
    es = Elasticsearch(hosts=teacher_host, port=elasticsearh_port)
    fmDate = get_datetime()
    datas = {
        "time" : fmDate,
        "PeopleNum" : peopleNumber
    }
    es.index(index='opentest',body=datas)

    datas = {
        "time" : fmDate,
        "humidity" : humidity
    }
    es.index(index='humtest',body=datas)

    datas = {
        "time" : fmDate,
        "temp" : temperature
    }
    es.index(index='temptest',body=datas)
    es.close()
    # print(peopleNumber,end='\n')
    # print(humidity,end='\n')
    # print(temperature)

def multipleSchedulers():
    scheduler1 = schedule.Scheduler()
    scheduler1.every().hour.at(":00").do(send_data_elk)
    scheduler1.every().hour.at(":05").do(send_data_elk)
    scheduler1.every().hour.at(":10").do(send_data_elk)
    scheduler1.every().hour.at(":15").do(send_data_elk)
    scheduler1.every().hour.at(":20").do(send_data_elk)
    scheduler1.every().hour.at(":25").do(send_data_elk)
    scheduler1.every().hour.at(":30").do(send_data_elk)
    scheduler1.every().hour.at(":35").do(send_data_elk)
    scheduler1.every().hour.at(":40").do(send_data_elk)
    scheduler1.every().hour.at(":45").do(send_data_elk)
    scheduler1.every().hour.at(":50").do(send_data_elk)
    scheduler1.every().hour.at(":55").do(send_data_elk)

    while True:
        scheduler1.run_pending()
        time.sleep(1)

if __name__ == '__main__':
    # multipleSchedulers()
    # no scheduler test
    send_data_elk()
